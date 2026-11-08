package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenValidation(t *testing.T) {
	t.Parallel()

	t.Run("nil context", func(t *testing.T) {
		t.Parallel()
		_, err := Open(nil, "https://example.com/x")
		if err == nil {
			t.Fatal("expected error for nil context")
		}
	})

	t.Run("empty url", func(t *testing.T) {
		t.Parallel()
		_, err := Open(context.Background(), "")
		if err == nil {
			t.Fatal("expected error for empty url")
		}
	})

	t.Run("bad chunk size", func(t *testing.T) {
		t.Parallel()
		_, err := Open(context.Background(), "https://example.com/x", WithChunkSize(0))
		if err == nil {
			t.Fatal("expected error for ChunkSize <= 0")
		}
	})
}

func TestOptionsApply(t *testing.T) {
	t.Parallel()

	client := &http.Client{}
	cfg := defaultConfig()
	for _, opt := range []Option{
		WithChunkSize(1 << 20),
		WithConcurrency(8),
		WithPrefetchWindow(2),
		WithHTTPClient(client),
		WithHeader("Authorization", "Bearer test"),
		WithRetry(RetryPolicy{MaxAttempts: 5}),
		WithBufferMemoryLimit(16 << 20),
		WithProgress(func(Progress) {}),
	} {
		opt(&cfg)
	}

	if cfg.ChunkSize != 1<<20 {
		t.Fatalf("ChunkSize = %d", cfg.ChunkSize)
	}
	if cfg.Concurrency != 8 {
		t.Fatalf("Concurrency = %d", cfg.Concurrency)
	}
	if cfg.PrefetchWindow != 2 {
		t.Fatalf("PrefetchWindow = %d", cfg.PrefetchWindow)
	}
	if cfg.HTTPClient != client {
		t.Fatal("HTTPClient not applied")
	}
	if got := cfg.Header.Get("Authorization"); got != "Bearer test" {
		t.Fatalf("Header Authorization = %q", got)
	}
	if cfg.Retry.MaxAttempts != 5 {
		t.Fatalf("Retry.MaxAttempts = %d", cfg.Retry.MaxAttempts)
	}
	if cfg.BufferMemoryLimit != 16<<20 {
		t.Fatalf("BufferMemoryLimit = %d", cfg.BufferMemoryLimit)
	}
	if cfg.OnProgress == nil {
		t.Fatal("OnProgress not applied")
	}
}

func TestPlanChunks(t *testing.T) {
	t.Parallel()

	got := planChunks(10, 4)
	want := []byteRange{
		{Index: 0, Start: 0, End: 4},
		{Index: 1, Start: 4, End: 8},
		{Index: 2, Start: 8, End: 10},
	}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunk[%d]=%+v want %+v", i, got[i], want[i])
		}
	}
	if planChunks(0, 4) != nil {
		t.Fatal("expected nil for size 0")
	}
}

func TestParseContentRangeTotal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		size int64
		ok   bool
	}{
		{"bytes 0-0/12345", 12345, true},
		{"bytes 0-99/100", 100, true},
		{"bytes 0-0/*", -1, false},
		{"", -1, false},
		{"bytes 0-0/", -1, false},
	}
	for _, tc := range cases {
		size, ok := parseContentRangeTotal(tc.in)
		if ok != tc.ok || size != tc.size {
			t.Fatalf("%q => (%d,%v) want (%d,%v)", tc.in, size, ok, tc.size, tc.ok)
		}
	}
}

func TestParallelDownloadHappyPath(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("abcdefghij"), 500) // 5000 bytes
	var rangeGETs atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, &rangeGETs)
	}))
	defer srv.Close()

	ctx := context.Background()
	r, err := Open(ctx, srv.URL,
		WithHTTPClient(srv.Client()),
		WithChunkSize(1024),
		WithConcurrency(4),
		WithPrefetchWindow(4),
		WithParallelThreshold(1), // force parallel path
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if r.Size() != int64(len(payload)) {
		t.Fatalf("Size=%d want %d", r.Size(), len(payload))
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes", len(got))
	}
	if rangeGETs.Load() < 2 {
		t.Fatalf("expected multiple Range GETs, got %d", rangeGETs.Load())
	}
}

func TestFallbackWhenRangeUnsupported(t *testing.T) {
	t.Parallel()

	payload := []byte("single-stream-body")
	var sawRange atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			sawRange.Store(true)
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %q want %q", got, payload)
	}
	if !sawRange.Load() {
		t.Fatal("expected probe to send Range")
	}
}

func TestCloseCancelsInflight(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("x"), 4096)
	started := make(chan struct{}, 8)
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		if rng == "bytes=0-0" {
			serveRange(w, r, payload, nil)
			return
		}
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-block:
		case <-r.Context().Done():
			return
		}
		serveRange(w, r, payload, nil)
	}))
	defer srv.Close()

	r, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithChunkSize(1024),
		WithConcurrency(2),
		WithPrefetchWindow(2),
		WithParallelThreshold(1),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for range GET to start")
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(block)

	_, err = r.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestParentContextCancel(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("y"), 8192)
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			serveRange(w, r, payload, nil)
			return
		}
		select {
		case <-block:
		case <-r.Context().Done():
			return
		}
		serveRange(w, r, payload, nil)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	r, err := Open(ctx, srv.URL,
		WithHTTPClient(srv.Client()),
		WithChunkSize(1024),
		WithConcurrency(2),
		WithPrefetchWindow(2),
		WithParallelThreshold(1),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	cancel()
	close(block)

	deadline := time.After(2 * time.Second)
	for {
		_, err := io.Copy(io.Discard, r)
		if err != nil {
			if !strings.Contains(err.Error(), "context canceled") && err != context.Canceled {
				// Accept wrapped cancel errors from net/http.
				if ctx.Err() == nil {
					t.Fatalf("unexpected err: %v", err)
				}
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("expected cancel error")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestSmallFileUsesFallback(t *testing.T) {
	t.Parallel()

	payload := []byte("tiny")
	var rangeBeyondProbe atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		if rng != "" && rng != "bytes=0-0" {
			rangeBeyondProbe.Add(1)
		}
		serveRange(w, r, payload, nil)
	}))
	defer srv.Close()

	r, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		// Default ParallelThreshold is 8MiB; tiny payload must use fallback.
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %q", got)
	}
	if rangeBeyondProbe.Load() != 0 {
		t.Fatalf("unexpected parallel Range GETs: %d", rangeBeyondProbe.Load())
	}
}

func serveRange(w http.ResponseWriter, r *http.Request, payload []byte, rangeGETs *atomic.Int64) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", `"test-etag"`)

	rng := r.Header.Get("Range")
	if rng == "" {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
		return
	}

	var start, end int64
	if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil {
		http.Error(w, "bad range", http.StatusBadRequest)
		return
	}
	if start < 0 || end >= int64(len(payload)) || start > end {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(payload)))
		http.Error(w, "unsatisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if rangeGETs != nil && rng != "bytes=0-0" {
		rangeGETs.Add(1)
	}
	chunk := payload[start : end+1]
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(chunk)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(chunk)
}
