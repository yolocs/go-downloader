package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// parallelReader implements Reader by prefetching fixed-size Range GETs
// into an ordered reorder buffer.
type parallelReader struct {
	cfg          Config
	url          string
	size         int64
	etag         string
	lastModified string
	ranges       []byteRange

	ctx    context.Context
	cancel context.CancelFunc

	mu            sync.Mutex
	cond          *sync.Cond
	nextToFetch   int
	nextToDeliver int
	inflight      int
	reservedBytes int64    // active, queued, and current chunk payloads
	slots         [][]byte // indexed by chunk; nil = not ready
	buf           []byte   // remainder of the chunk currently being read
	bytesRead     int64
	bytesFetched  int64
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
		cfg:          cfg,
		url:          rawURL,
		size:         info.Size,
		etag:         info.ETag,
		lastModified: info.LastModified,
		ranges:       ranges,
		ctx:          ctx,
		cancel:       cancel,
		slots:        make([][]byte, len(ranges)),
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
			r.bytesRead += int64(n)
			if len(r.buf) == 0 {
				// Slicing off a prefix retains the whole backing allocation.
				// Release its reservation only after the chunk is fully read.
				r.buf = nil
				br := r.ranges[r.nextToDeliver]
				r.reservedBytes -= br.End - br.Start
				r.nextToDeliver++
				r.scheduleLocked()
			}
			r.emitProgressLocked()
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
			r.buf = data
			continue
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
	r.reservedBytes = 0
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
		r.nextToFetch-r.nextToDeliver < r.cfg.PrefetchWindow {
		idx := r.nextToFetch
		br := r.ranges[idx]
		want := br.End - br.Start
		if want > r.cfg.BufferMemoryLimit-r.reservedBytes {
			break
		}
		r.reservedBytes += want
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
		r.reservedBytes -= br.End - br.Start
		r.cond.Broadcast()
		return
	}
	if err != nil {
		r.reservedBytes -= br.End - br.Start
		r.err = err
		r.cancel() // Stop sibling requests and retry waits on a terminal error.
		r.cond.Broadcast()
		return
	}
	r.slots[idx] = data
	r.bytesFetched += int64(len(data))
	r.scheduleLocked()
	r.emitProgressLocked()
	r.cond.Broadcast()
}

// emitProgressLocked invokes OnProgress with the current counters.
// Caller must hold r.mu. The callback must not re-enter the Reader.
func (r *parallelReader) emitProgressLocked() {
	fn := r.cfg.OnProgress
	if fn == nil || r.closed {
		return
	}
	fn(Progress{
		BytesRead:    r.bytesRead,
		BytesFetched: r.bytesFetched,
		Total:        r.size,
	})
}

func (r *parallelReader) doRangeGetOnce(ctx context.Context, br byteRange, data []byte) ([]byte, bool, error) {
	want := br.End - br.Start
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("downloader: range request: %w", err)
	}
	applyHeaders(req, r.cfg.Header)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", br.Start, br.End-1))
	applyObjectValidators(req, r.etag, r.lastModified)

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
		contentRange := resp.Header.Get("Content-Range")
		start, end, total, ok := parseContentRange(contentRange)
		if !ok || start != br.Start || end != br.End-1 {
			return nil, false, fmt.Errorf("downloader: range [%d-%d): invalid Content-Range %q; want bytes %d-%d/%d", br.Start, br.End, contentRange, br.Start, br.End-1, r.size)
		}
		if total != r.size {
			// Offsets match the request but the object length changed.
			return nil, false, fmt.Errorf("%w: Content-Range total %d != probed size %d", ErrObjectChanged, total, r.size)
		}
		if err := checkETagConsistency(r.etag, resp.Header.Get("ETag")); err != nil {
			return nil, false, fmt.Errorf("downloader: range [%d-%d): %w", br.Start, br.End, err)
		}
		if err := checkContentLength(resp.Header.Get("Content-Length"), want); err != nil {
			return nil, false, fmt.Errorf("downloader: range [%d-%d): %w", br.Start, br.End, err)
		}
		var n int
		var readErr error
		var tail [2]byte
		for readErr == nil {
			// Read the final payload byte and possible excess into a stack
			// buffer, so even excess returned with an error is detected.
			var dst []byte
			if n < len(data)-1 {
				dst = data[n : len(data)-1]
			} else {
				dst = tail[n-len(data)+1:]
			}
			var m int
			m, readErr = resp.Body.Read(dst)
			if n+m > len(data) {
				return nil, false, fmt.Errorf("downloader: range [%d-%d): got more than %d bytes", br.Start, br.End, want)
			}
			if n == len(data)-1 && m > 0 {
				data[n] = tail[0]
			}
			n += m
		}
		if readErr == io.EOF {
			if n == len(data) {
				readErr = nil
			} else {
				readErr = io.ErrUnexpectedEOF
			}
		}
		err = readErr
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
				err = errors.Join(err, ctxErr)
			}
			return nil, retryableNetworkError(err), fmt.Errorf("downloader: reading range [%d-%d): %w", br.Start, br.End, err)
		}
		return data, false, nil

	case http.StatusOK:
		// Server ignored Range — refuse to stitch (correctness invariant #4).
		return nil, false, fmt.Errorf("downloader: server returned 200 for Range [%d-%d); refusing to stitch", br.Start, br.End)

	case http.StatusRequestedRangeNotSatisfiable:
		// A range planned from the probe must remain satisfiable. Do not
		// reinterpret this as EOF or restart after bytes may be delivered.
		return nil, false, fmt.Errorf("downloader: server returned 416 for Range [%d-%d): Content-Range %q; refusing to stitch", br.Start, br.End, resp.Header.Get("Content-Range"))

	case http.StatusPreconditionFailed:
		// If-Match / If-Unmodified-Since rejected: object changed since probe.
		return nil, false, fmt.Errorf("%w: range [%d-%d): status %s", ErrObjectChanged, br.Start, br.End, resp.Status)

	default:
		// Close rather than drain error bodies: they may be large or stalled.
		return nil, retryableStatus(resp.StatusCode), fmt.Errorf("downloader: range GET [%d-%d): status %s", br.Start, br.End, resp.Status)
	}
}

// fallbackReader wraps a single streaming GET body.
type fallbackReader struct {
	body       io.ReadCloser
	size       int64
	onProgress func(Progress)
	cancel     context.CancelFunc
	mu         sync.Mutex
	bytesRead  int64
	closed     bool
}

func openFallback(ctx context.Context, cfg Config, rawURL string, info objectInfo) (*fallbackReader, error) {
	ctx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("downloader: fallback request: %w", err)
	}
	applyHeaders(req, cfg.Header)
	applyObjectValidators(req, info.ETag, info.LastModified)

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("downloader: fallback GET: %w", err)
	}
	if resp.StatusCode == http.StatusPreconditionFailed {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("%w: fallback GET: status %s", ErrObjectChanged, resp.Status)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("downloader: fallback GET: status %s", resp.Status)
	}

	size := info.Size
	contentLength := resp.Header.Get("Content-Length")
	if size < 0 {
		if contentLength != "" {
			n, err := strconv.ParseInt(contentLength, 10, 64)
			if err != nil {
				resp.Body.Close()
				cancel()
				return nil, fmt.Errorf("downloader: fallback GET: bad Content-Length %q: %w", contentLength, err)
			}
			size = n
		}
	} else if contentLength != "" {
		n, err := strconv.ParseInt(strings.TrimSpace(contentLength), 10, 64)
		if err != nil {
			resp.Body.Close()
			cancel()
			return nil, fmt.Errorf("downloader: fallback GET: bad Content-Length %q: %w", contentLength, err)
		}
		if n != size {
			resp.Body.Close()
			cancel()
			return nil, fmt.Errorf("%w: Content-Length %d != probed size %d", ErrObjectChanged, n, size)
		}
	}
	if err := checkETagConsistency(info.ETag, resp.Header.Get("ETag")); err != nil {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("downloader: fallback GET: %w", err)
	}

	return &fallbackReader{
		body:       resp.Body,
		size:       size,
		onProgress: cfg.OnProgress,
		cancel:     cancel,
	}, nil
}

func (r *fallbackReader) Size() int64 { return r.size }

func (r *fallbackReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, fs.ErrClosed
	}
	r.mu.Unlock()

	n, err := r.body.Read(p)
	if n > 0 {
		r.mu.Lock()
		r.bytesRead += int64(n)
		r.emitProgressLocked()
		r.mu.Unlock()
	}
	return n, err
}

// emitProgressLocked invokes OnProgress. Caller must hold r.mu.
func (r *fallbackReader) emitProgressLocked() {
	fn := r.onProgress
	if fn == nil || r.closed {
		return
	}
	// Single-stream path has no reorder buffer; fetched tracks read.
	fn(Progress{
		BytesRead:    r.bytesRead,
		BytesFetched: r.bytesRead,
		Total:        r.size,
	})
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
