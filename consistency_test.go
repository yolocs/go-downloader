package downloader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestChunkRejectsETagMismatch(t *testing.T) {
	t.Parallel()

	payload := []byte("abcdefgh")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			serveRange(w, r, payload, nil)
			return
		}
		attempts.Add(1)
		if r.Header.Get("If-Match") != `"test-etag"` {
			t.Errorf("If-Match = %q", r.Header.Get("If-Match"))
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("ETag", `"other-etag"`)
		w.Header().Set("Content-Range", "bytes 0-3/8")
		w.Header().Set("Content-Length", "4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("abcd"))
	}))
	defer srv.Close()

	r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
		WithChunkSize(4), WithConcurrency(1), WithPrefetchWindow(1), WithParallelThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if !errors.Is(err, ErrObjectChanged) || len(got) != 0 || attempts.Load() != 1 {
		t.Fatalf("ReadAll = %q, %v; attempts = %d; want ErrObjectChanged", got, err, attempts.Load())
	}
}

func TestChunkRejectsSizeChangeInContentRange(t *testing.T) {
	t.Parallel()

	payload := []byte("abcdefgh")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			serveRange(w, r, payload, nil)
			return
		}
		attempts.Add(1)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("ETag", `"test-etag"`)
		w.Header().Set("Content-Range", "bytes 0-3/9") // total changed
		w.Header().Set("Content-Length", "4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("abcd"))
	}))
	defer srv.Close()

	r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
		WithChunkSize(4), WithConcurrency(1), WithPrefetchWindow(1), WithParallelThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if !errors.Is(err, ErrObjectChanged) || len(got) != 0 || attempts.Load() != 1 {
		t.Fatalf("ReadAll = %q, %v; attempts = %d; want ErrObjectChanged", got, err, attempts.Load())
	}
}

func TestChunkPreconditionFailedIsObjectChanged(t *testing.T) {
	t.Parallel()

	payload := []byte("abcdefgh")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			serveRange(w, r, payload, nil)
			return
		}
		attempts.Add(1)
		w.WriteHeader(http.StatusPreconditionFailed)
	}))
	defer srv.Close()

	r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
		WithChunkSize(4), WithConcurrency(1), WithPrefetchWindow(1), WithParallelThreshold(1),
		WithRetry(RetryPolicy{MaxAttempts: 3}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if !errors.Is(err, ErrObjectChanged) || len(got) != 0 || attempts.Load() != 1 {
		t.Fatalf("ReadAll = %q, %v; attempts = %d; want permanent ErrObjectChanged", got, err, attempts.Load())
	}
}

func TestChunkUsesIfUnmodifiedSinceWithoutETag(t *testing.T) {
	t.Parallel()

	payload := []byte("abcdefgh")
	const lastMod = "Wed, 21 Oct 2015 07:28:00 GMT"
	var sawValidator atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		if rng == "bytes=0-0" {
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Last-Modified", lastMod)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(payload)))
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[:1])
			return
		}
		if r.Header.Get("If-Match") != "" {
			t.Errorf("unexpected If-Match %q", r.Header.Get("If-Match"))
		}
		if r.Header.Get("If-Unmodified-Since") == lastMod {
			sawValidator.Store(true)
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Last-Modified", lastMod)
		start, end := int64(0), int64(0)
		_, _ = fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
		chunk := payload[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(chunk)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(chunk)
	}))
	defer srv.Close()

	r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
		WithChunkSize(4), WithConcurrency(2), WithPrefetchWindow(2), WithParallelThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("ReadAll = %q, %v", got, err)
	}
	if !sawValidator.Load() {
		t.Fatal("expected If-Unmodified-Since on chunk requests")
	}
}

func TestChunkAllowsMissingResponseETag(t *testing.T) {
	t.Parallel()

	payload := []byte("abcdefgh")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			serveRange(w, r, payload, nil)
			return
		}
		// Honor If-Match but omit ETag on the chunk response.
		if r.Header.Get("If-Match") != `"test-etag"` {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		rng := r.Header.Get("Range")
		var start, end int64
		_, _ = fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
		chunk := payload[start : end+1]
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(chunk)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(chunk)
	}))
	defer srv.Close()

	r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
		WithChunkSize(4), WithConcurrency(2), WithPrefetchWindow(2), WithParallelThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("ReadAll = %q, %v", got, err)
	}
}

func TestFallbackRejectsETagMismatch(t *testing.T) {
	t.Parallel()

	payload := []byte("tiny")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", `"probe-etag"`)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(payload)))
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[:1])
			return
		}
		w.Header().Set("ETag", `"changed-etag"`)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	_, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()))
	if !errors.Is(err, ErrObjectChanged) {
		t.Fatalf("Open err = %v; want ErrObjectChanged", err)
	}
}

func TestFallbackRejectsSizeMismatch(t *testing.T) {
	t.Parallel()

	payload := []byte("tiny")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", `"probe-etag"`)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(payload)))
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[:1])
			return
		}
		w.Header().Set("ETag", `"probe-etag"`)
		w.Header().Set("Content-Length", "99")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	_, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()))
	if !errors.Is(err, ErrObjectChanged) {
		t.Fatalf("Open err = %v; want ErrObjectChanged", err)
	}
}

func TestFallbackPreconditionFailed(t *testing.T) {
	t.Parallel()

	payload := []byte("tiny")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", `"probe-etag"`)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(payload)))
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[:1])
			return
		}
		w.WriteHeader(http.StatusPreconditionFailed)
	}))
	defer srv.Close()

	_, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()))
	if !errors.Is(err, ErrObjectChanged) {
		t.Fatalf("Open err = %v; want ErrObjectChanged", err)
	}
}

func TestCheckETagConsistency(t *testing.T) {
	t.Parallel()
	if err := checkETagConsistency(`"a"`, `"a"`); err != nil {
		t.Fatal(err)
	}
	if err := checkETagConsistency(`"a"`, ""); err != nil {
		t.Fatal(err)
	}
	if err := checkETagConsistency("", `"a"`); err != nil {
		t.Fatal(err)
	}
	if err := checkETagConsistency(`"a"`, `"b"`); !errors.Is(err, ErrObjectChanged) {
		t.Fatalf("err = %v", err)
	}
}
