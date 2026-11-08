package downloader

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBufferMemoryLimitValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{"negative", []Option{WithBufferMemoryLimit(-1)}},
		{"below chunk size", []Option{WithChunkSize(1024), WithBufferMemoryLimit(1023)}},
		{"derived limit overflow", []Option{WithChunkSize(math.MaxInt64 / 2), WithPrefetchWindow(3)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A missing validation must fail without contacting an external host.
			requested := false
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				requested = true
				return nil, context.Canceled
			})}
			opts := append(tc.opts, WithHTTPClient(client))
			_, err := Open(context.Background(), "https://example.test/object", opts...)
			if err == nil || !strings.Contains(err.Error(), "BufferMemoryLimit") || requested {
				t.Fatalf("Open: err=%v, requested=%v; want memory validation before any request", err, requested)
			}
		})
	}
}

func waitForPrefetch(t *testing.T, r Reader) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		r.(*parallelReader).wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("prefetch did not settle")
	}
}

func TestPrefetchCapsIncludePartiallyReadChunk(t *testing.T) {
	for _, tc := range []struct {
		name                string
		window              int
		limit               int64
		initial, afterDrain int64
	}{
		{"derived budget", 2, 0, 2, 3},
		{"one chunk window", 1, 16, 1, 2},
		{"one chunk budget", 4, 4, 1, 2},
		{"nonmultiple budget and short tail", 4, 9, 2, 4},
		{"window tighter than budget", 2, 100, 2, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte("0123456789abc") // 4 + 4 + 4 + 1 bytes
			var requests atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				serveRange(w, req, payload, &requests)
			}))
			defer srv.Close()
			r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
				WithChunkSize(4), WithConcurrency(2), WithPrefetchWindow(tc.window),
				WithBufferMemoryLimit(tc.limit), WithParallelThreshold(1))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			waitForPrefetch(t, r)
			if got := requests.Load(); got != tc.initial {
				t.Fatalf("idle requests = %d, want %d", got, tc.initial)
			}
			prefix := make([]byte, 4)
			if _, err := io.ReadFull(r, prefix[:1]); err != nil {
				t.Fatal(err)
			}
			waitForPrefetch(t, r)
			if got := requests.Load(); got != tc.initial {
				t.Fatalf("requests after one byte = %d, want %d (current chunk still allocated)", got, tc.initial)
			}
			if _, err := io.ReadFull(r, prefix[1:]); err != nil {
				t.Fatal(err)
			}
			waitForPrefetch(t, r)
			if got := requests.Load(); got != tc.afterDrain {
				t.Fatalf("requests after draining chunk = %d, want %d", got, tc.afterDrain)
			}
			rest, err := io.ReadAll(r)
			if err != nil || !bytes.Equal(append(prefix, rest...), payload) {
				t.Fatalf("download mismatch: prefix=%q rest=%q err=%v", prefix, rest, err)
			}
		})
	}
}

func TestChunkPayloadAllocationFitsBudget(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 1000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		serveRange(w, req, payload, nil)
	}))
	defer srv.Close()
	r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
		WithChunkSize(1000), WithBufferMemoryLimit(1000), WithParallelThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	waitForPrefetch(t, r)
	pr := r.(*parallelReader)
	pr.mu.Lock()
	allocated := cap(pr.slots[0])
	pr.mu.Unlock()
	if allocated != 1000 {
		t.Fatalf("chunk backing allocation = %d, want exact 1000-byte payload budget", allocated)
	}
}

func TestPrefetchBudgetIncludesInflightAndOutOfOrderChunks(t *testing.T) {
	for _, stop := range []string{"close", "cancel"} {
		t.Run(stop, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("Range") == "bytes=4-7" {
					return &http.Response{StatusCode: http.StatusPartialContent,
						Header: http.Header{"Content-Range": {"bytes 4-7/16"}},
						Body:   io.NopCloser(strings.NewReader("4567"))}, nil
				}
				<-req.Context().Done()
				return nil, req.Context().Err()
			})}
			cfg := defaultConfig()
			cfg.HTTPClient, cfg.ChunkSize, cfg.BufferMemoryLimit = client, 4, 8
			if err := validateConfig(&cfg); err != nil {
				t.Fatal(err)
			}
			r, err := openParallelReader(ctx, cfg, "https://example.test/object", objectInfo{Size: 16})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			// Wait for the later chunk to be published while the first is blocked.
			done := make(chan struct{})
			go func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				for r.slots[1] == nil && r.err == nil && !r.closed {
					r.cond.Wait()
				}
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("later chunk did not complete")
			}
			r.mu.Lock()
			admitted, reserved := r.nextToFetch, r.reservedBytes
			r.mu.Unlock()
			if admitted != 2 || reserved != 8 {
				t.Fatalf("admitted %d chunks, reserved %d bytes; want 2 and 8", admitted, reserved)
			}
			if stop == "close" {
				r.Close()
			} else {
				cancel()
			}
			waitForPrefetch(t, r)
			if _, err := r.Read(make([]byte, 1)); (stop == "close" && !errors.Is(err, fs.ErrClosed)) ||
				(stop == "cancel" && !errors.Is(err, context.Canceled)) {
				t.Fatalf("Read after %s = %v", stop, err)
			}
		})
	}
}

type memoryRetryBody struct {
	io.Reader
	buffers *[]*byte
	seen    bool
}

func (b *memoryRetryBody) Read(p []byte) (int, error) {
	if !b.seen {
		*b.buffers = append(*b.buffers, &p[0])
		b.seen = true
	}
	return b.Reader.Read(p)
}
func (b *memoryRetryBody) Close() error { return nil }

func TestChunkRetriesReuseReservedPayloadBuffer(t *testing.T) {
	var buffers []*byte // One worker; waitForPrefetch synchronizes observation.
	attempts := 0
	cfg := defaultConfig()
	cfg.ChunkSize, cfg.BufferMemoryLimit = 7, 7
	cfg.Retry.InitialBackoff = time.Millisecond
	cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		body := "xx" // First attempt ends early; retry must overwrite these bytes.
		if attempts > 1 {
			body = "payload"
		}
		return &http.Response{StatusCode: http.StatusPartialContent,
			Header: http.Header{"Content-Range": {"bytes 0-6/7"}},
			Body:   &memoryRetryBody{Reader: strings.NewReader(body), buffers: &buffers}}, nil
	})}
	if err := validateConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	r, err := openParallelReader(context.Background(), cfg, "https://example.test/object", objectInfo{Size: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	waitForPrefetch(t, r)
	// Each attempt must fill the same payload allocation.
	if len(buffers) != 2 || buffers[0] != buffers[1] {
		t.Fatal("retry allocated another payload buffer instead of reusing the reservation")
	}
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "payload" || attempts != 2 {
		t.Fatalf("ReadAll = %q, %v, attempts=%d", got, err, attempts)
	}
}

// Some readers report a validation or transport error alongside the final bytes.
// Filling a payload buffer must not silently discard that error.
type finalErrorBody struct {
	body           *strings.Reader
	err            error
	readAfterError bool
	done           bool
}

func (b *finalErrorBody) Read(p []byte) (int, error) {
	if b.done {
		b.readAfterError = true
		return 0, io.EOF
	}
	n, err := b.body.Read(p)
	if b.body.Len() == 0 {
		b.done = true
		err = b.err
	}
	return n, err
}
func (*finalErrorBody) Close() error { return nil }

func TestChunkExactLengthPreservesBodyError(t *testing.T) {
	bodyErr := errors.New("body validation failed")
	body := &finalErrorBody{body: strings.NewReader("payload"), err: bodyErr}
	cfg := defaultConfig()
	cfg.ChunkSize, cfg.BufferMemoryLimit = 7, 7
	cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusPartialContent,
			Header: http.Header{"Content-Range": {"bytes 0-6/7"}},
			Body:   body}, nil
	})}
	if err := validateConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	r, err := openParallelReader(context.Background(), cfg, "https://example.test/object", objectInfo{Size: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if len(got) != 0 || !errors.Is(err, bodyErr) || body.readAfterError {
		t.Fatalf("ReadAll = %q, %v; readAfterError=%v; want original error with no further reads", got, err, body.readAfterError)
	}
}
