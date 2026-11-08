package downloader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChunkRetriesRecover(t *testing.T) {
	for _, status := range []int{408, 429, 500, 502, 503, 504} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			t.Parallel()
			payload := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!?")
			var mu sync.Mutex
			attempts := make(map[string]int)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				rng := r.Header.Get("Range")
				if rng != "bytes=0-0" {
					mu.Lock()
					attempts[rng]++
					attempt := attempts[rng]
					mu.Unlock()
					if r.Header.Get("X-Test") != "preserved" || r.Header.Get("If-Match") != `"test-etag"` {
						http.Error(w, "missing headers", http.StatusBadRequest)
						return
					}
					if attempt < 3 {
						http.Error(w, "temporary failure", status)
						return
					}
				}
				serveRange(w, r, payload, nil)
			}))
			defer srv.Close()

			r, err := Open(context.Background(), srv.URL,
				WithHTTPClient(srv.Client()), WithHeader("X-Test", "preserved"),
				WithChunkSize(16), WithConcurrency(2), WithPrefetchWindow(2), WithParallelThreshold(1))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := io.ReadAll(r)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("ReadAll = %q, %v; want %q", got, err, payload)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(attempts) != 4 {
				t.Fatalf("retried %d ranges, want 4", len(attempts))
			}
			for rng, n := range attempts {
				if n != 3 {
					t.Errorf("%s: %d attempts, want 3", rng, n)
				}
			}
		})
	}
}

func TestChunkRetryAttemptLimit(t *testing.T) {
	for _, maxAttempts := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(maxAttempts), func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") == "bytes=0-0" {
					serveRange(w, r, []byte("payload"), nil)
					return
				}
				attempts.Add(1)
				http.Error(w, "temporary failure", http.StatusServiceUnavailable)
			}))
			defer srv.Close()
			r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
				WithChunkSize(16), WithParallelThreshold(1), WithRetry(RetryPolicy{MaxAttempts: maxAttempts}))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := io.ReadAll(r)
			if err == nil || len(got) != 0 {
				t.Fatalf("ReadAll = %q, %v; want no bytes and an error", got, err)
			}
			if got := attempts.Load(); got != int32(maxAttempts) {
				t.Fatalf("attempts = %d, want %d", got, maxAttempts)
			}
		})
	}
}

func TestChunkDoesNotRetryPermanentStatus(t *testing.T) {
	for _, status := range []int{200, 400, 401, 403, 404, 412, 416, 501} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") == "bytes=0-0" {
					serveRange(w, r, []byte("payload"), nil)
					return
				}
				attempts.Add(1)
				w.WriteHeader(status)
			}))
			defer srv.Close()
			r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()), WithParallelThreshold(1))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := io.ReadAll(r)
			if err == nil || len(got) != 0 || attempts.Load() != 1 {
				t.Fatalf("ReadAll = %q, %v; attempts = %d; want error and one attempt", got, err, attempts.Load())
			}
		})
	}
}

func TestChunkRetriesDiscardPartialBody(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	payload := []byte("correct payload")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=0-0" && attempts.Add(1) == 1 {
			w.Header().Set("Content-Length", "15")
			w.Header().Set("Content-Range", "bytes 0-14/15")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("wrong"))
			return
		}
		serveRange(w, r, payload, nil)
	}))
	defer srv.Close()
	r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()), WithParallelThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, payload) || attempts.Load() != 2 {
		t.Fatalf("ReadAll = %q, %v; attempts = %d; want complete payload after 2 attempts", got, err, attempts.Load())
	}
}

func TestChunkRetryAttemptTimeout(t *testing.T) {
	for _, recover := range []bool{true, false} {
		t.Run(fmt.Sprint(recover), func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			payload := []byte("payload")
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") == "bytes=0-0" {
					serveRange(w, r, payload, nil)
					return
				}
				n := attempts.Add(1)
				if n == 1 || !recover {
					w.Header().Set("Content-Length", "7")
					w.WriteHeader(http.StatusPartialContent)
					w.(http.Flusher).Flush()
					<-r.Context().Done() // Headers arrive, but the body stalls.
					return
				}
				serveRange(w, r, payload, nil)
			}))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r, err := Open(ctx, srv.URL, WithHTTPClient(srv.Client()), WithParallelThreshold(1),
				WithRetry(RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond, AttemptTimeout: 50 * time.Millisecond}))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := io.ReadAll(r)
			if recover {
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatalf("ReadAll = %q, %v; want recovered payload", got, err)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) || len(got) != 0 {
				t.Fatalf("ReadAll = %q, %v; want attempt deadline error", got, err)
			}
			if ctx.Err() != nil || attempts.Load() != 2 {
				t.Fatalf("parent err = %v; attempts = %d; want live parent and 2 attempts", ctx.Err(), attempts.Load())
			}
		})
	}
}

func TestChunkRetryCancellationDuringBackoff(t *testing.T) {
	for _, closeReader := range []bool{true, false} {
		t.Run(fmt.Sprint(closeReader), func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			started := make(chan struct{}, 3)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") == "bytes=0-0" {
					serveRange(w, r, []byte("payload"), nil)
					return
				}
				attempts.Add(1)
				http.Error(w, "temporary", http.StatusServiceUnavailable)
				started <- struct{}{}
			}))
			defer srv.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := Open(ctx, srv.URL, WithHTTPClient(srv.Client()), WithParallelThreshold(1),
				WithRetry(RetryPolicy{InitialBackoff: time.Hour, MaxBackoff: time.Hour}))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			done := make(chan error, 1)
			go func() { _, err := io.ReadAll(r); done <- err }()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("no chunk request")
			}
			select {
			case err := <-done:
				t.Fatalf("Read returned before cancellation: %v", err)
			case <-time.After(20 * time.Millisecond): // Let the response enter backoff.
			}
			wantErr := context.Canceled
			if closeReader {
				wantErr = fs.ErrClosed
				closed := make(chan struct{})
				go func() { r.Close(); close(closed) }()
				select {
				case <-closed:
				case <-time.After(time.Second):
					t.Fatal("Close blocked during retry backoff")
				}
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, wantErr) {
					t.Fatalf("Read error = %v, want %v", err, wantErr)
				}
			case <-time.After(time.Second):
				t.Fatal("Read blocked after cancellation")
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if attempts.Load() != 1 {
				t.Fatalf("attempts = %d, want 1", attempts.Load())
			}
		})
	}
}

func TestRetryPolicyValidation(t *testing.T) {
	for _, policy := range []RetryPolicy{
		{MaxAttempts: -1},
		{InitialBackoff: -1},
		{MaxBackoff: -1},
		{AttemptTimeout: -1},
		{InitialBackoff: time.Second, MaxBackoff: time.Millisecond},
	} {
		// Invalid configuration must fail before trying even the probe request.
		var requests atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			serveRange(w, r, []byte("payload"), nil)
		}))
		r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()), WithRetry(policy))
		if r != nil {
			r.Close()
		}
		srv.Close()
		if err == nil || requests.Load() != 0 {
			t.Errorf("policy %+v: err = %v, requests = %d; want validation error before probe", policy, err, requests.Load())
		}
	}
}
