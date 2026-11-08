package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"sync"
)

// parallelReader implements Reader by prefetching fixed-size Range GETs
// into an ordered reorder buffer.
type parallelReader struct {
	cfg    Config
	url    string
	size   int64
	etag   string
	ranges []byteRange

	ctx    context.Context
	cancel context.CancelFunc

	mu            sync.Mutex
	cond          *sync.Cond
	nextToFetch   int
	nextToDeliver int
	inflight      int
	slots         [][]byte // indexed by chunk; nil = not ready
	buf           []byte   // remainder of the chunk currently being read
	err           error
	closed        bool
	wg            sync.WaitGroup
}

func openParallelReader(ctx context.Context, cfg Config, rawURL string, info objectInfo) (*parallelReader, error) {
	ranges := planChunks(info.Size, cfg.ChunkSize)
	if len(ranges) == 0 {
		return nil, fmt.Errorf("downloader: empty plan for size %d", info.Size)
	}

	ctx, cancel := context.WithCancel(ctx)
	r := &parallelReader{
		cfg:    cfg,
		url:    rawURL,
		size:   info.Size,
		etag:   info.ETag,
		ranges: ranges,
		ctx:    ctx,
		cancel: cancel,
		slots:  make([][]byte, len(ranges)),
	}
	r.cond = sync.NewCond(&r.mu)

	r.mu.Lock()
	r.scheduleLocked()
	r.mu.Unlock()
	return r, nil
}

func (r *parallelReader) Size() int64 { return r.size }

func (r *parallelReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for {
		if r.closed {
			return 0, fs.ErrClosed
		}
		if len(r.buf) > 0 {
			n := copy(p, r.buf)
			r.buf = r.buf[n:]
			return n, nil
		}
		if r.nextToDeliver >= len(r.ranges) {
			if r.err != nil {
				return 0, r.err
			}
			return 0, io.EOF
		}
		if data := r.slots[r.nextToDeliver]; data != nil {
			r.slots[r.nextToDeliver] = nil
			r.nextToDeliver++
			r.buf = data
			r.scheduleLocked()
			r.cond.Broadcast()
			n := copy(p, r.buf)
			r.buf = r.buf[n:]
			return n, nil
		}
		if r.err != nil {
			return 0, r.err
		}
		r.cond.Wait()
	}
}

func (r *parallelReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.cancel()
	r.buf = nil
	for i := range r.slots {
		r.slots[i] = nil
	}
	r.cond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
	return nil
}

// scheduleLocked launches fetch goroutines while under the prefetch / concurrency caps.
// Caller must hold r.mu.
func (r *parallelReader) scheduleLocked() {
	if r.closed || r.err != nil {
		return
	}
	for r.inflight < r.cfg.Concurrency &&
		r.nextToFetch < len(r.ranges) &&
		r.nextToFetch < r.nextToDeliver+r.cfg.PrefetchWindow {
		idx := r.nextToFetch
		r.nextToFetch++
		r.inflight++
		r.wg.Add(1)
		go r.fetch(idx)
	}
}

func (r *parallelReader) fetch(idx int) {
	defer r.wg.Done()
	br := r.ranges[idx]
	data, err := r.doRangeGet(br)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.inflight--
	if r.closed {
		r.cond.Broadcast()
		return
	}
	if r.err != nil {
		r.scheduleLocked()
		r.cond.Broadcast()
		return
	}
	if err != nil {
		r.err = err
		r.cancel() // Stop sibling requests and retry waits on a terminal error.
		r.cond.Broadcast()
		return
	}
	r.slots[idx] = data
	r.scheduleLocked()
	r.cond.Broadcast()
}

func (r *parallelReader) doRangeGetOnce(ctx context.Context, br byteRange) ([]byte, bool, error) {
	want := br.End - br.Start
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("downloader: range request: %w", err)
	}
	applyHeaders(req, r.cfg.Header)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", br.Start, br.End-1))
	if r.etag != "" {
		req.Header.Set("If-Match", r.etag)
	}

	resp, err := r.cfg.HTTPClient.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
			err = errors.Join(err, ctxErr)
		}
		return nil, retryableNetworkError(err), fmt.Errorf("downloader: range GET [%d-%d): %w", br.Start, br.End, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		data, err := io.ReadAll(io.LimitReader(resp.Body, want+1))
		if int64(len(data)) > want {
			return nil, false, fmt.Errorf("downloader: range [%d-%d): got %d bytes, want %d", br.Start, br.End, len(data), want)
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
				err = errors.Join(err, ctxErr)
			}
			return nil, retryableNetworkError(err), fmt.Errorf("downloader: reading range [%d-%d): %w", br.Start, br.End, err)
		}
		if int64(len(data)) < want {
			return nil, true, fmt.Errorf("downloader: range [%d-%d): got %d bytes, want %d: %w", br.Start, br.End, len(data), want, io.ErrUnexpectedEOF)
		}
		return data, false, nil

	case http.StatusOK:
		// Server ignored Range — refuse to stitch (correctness invariant #4).
		return nil, false, fmt.Errorf("downloader: server returned 200 for Range [%d-%d); refusing to stitch", br.Start, br.End)

	default:
		// Close rather than drain error bodies: they may be large or stalled.
		return nil, retryableStatus(resp.StatusCode), fmt.Errorf("downloader: range GET [%d-%d): status %s", br.Start, br.End, resp.Status)
	}
}

// fallbackReader wraps a single streaming GET body.
type fallbackReader struct {
	body   io.ReadCloser
	size   int64
	cancel context.CancelFunc
	closed bool
	mu     sync.Mutex
}

func openFallback(ctx context.Context, cfg Config, rawURL string, size int64) (*fallbackReader, error) {
	ctx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("downloader: fallback request: %w", err)
	}
	applyHeaders(req, cfg.Header)

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("downloader: fallback GET: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("downloader: fallback GET: status %s", resp.Status)
	}
	if size < 0 {
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if n, err := strconv.ParseInt(cl, 10, 64); err == nil {
				size = n
			}
		}
	}
	return &fallbackReader{body: resp.Body, size: size, cancel: cancel}, nil
}

func (r *fallbackReader) Size() int64 { return r.size }

func (r *fallbackReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return 0, fs.ErrClosed
	}
	return r.body.Read(p)
}

func (r *fallbackReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()
	r.cancel()
	return r.body.Close()
}
