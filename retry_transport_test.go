package downloader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestChunkTransportErrors(t *testing.T) {
	permanent := errors.New("permanent transport error")
	for _, tc := range []struct {
		name string
		err  error
		want int32
	}{
		{"EOF", io.EOF, 2},
		{"reset", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, 2},
		{"timeout", context.DeadlineExceeded, 2},
		{"permanent", permanent, 1},
		{"canceled", context.Canceled, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload := []byte("payload")
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				serveRange(w, r, payload, nil)
			}))
			defer srv.Close()
			client := srv.Client()
			transport := client.Transport
			var attempts atomic.Int32
			client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Range") != "bytes=0-0" && attempts.Add(1) == 1 {
					return nil, tc.err
				}
				return transport.RoundTrip(r)
			})
			r, err := Open(context.Background(), srv.URL, WithHTTPClient(client), WithParallelThreshold(1),
				WithRetry(RetryPolicy{InitialBackoff: time.Millisecond}))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := io.ReadAll(r)
			if tc.want == 2 {
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatalf("ReadAll = %q, %v; want recovered payload", got, err)
				}
			} else if !errors.Is(err, tc.err) || len(got) != 0 {
				t.Fatalf("ReadAll = %q, %v; want wrapped %v", got, err, tc.err)
			}
			if attempts.Load() != tc.want {
				t.Fatalf("attempts = %d, want %d", attempts.Load(), tc.want)
			}
		})
	}
}

type closeObserver struct {
	io.ReadCloser
	closed *atomic.Int32
}

func (b closeObserver) Close() error {
	b.closed.Add(1)
	return b.ReadCloser.Close()
}

func TestChunkRetryClosesErrorBodyWithoutDraining(t *testing.T) {
	t.Parallel()
	var attempts, closed atomic.Int32
	payload := []byte("payload")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=0-0" && attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.(http.Flusher).Flush()
			<-r.Context().Done() // Draining this body would stall until the timeout.
			return
		}
		serveRange(w, r, payload, nil)
	}))
	defer srv.Close()
	client := srv.Client()
	transport := client.Transport
	client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		resp, err := transport.RoundTrip(r)
		if err == nil && r.Header.Get("Range") != "bytes=0-0" {
			if attempts.Load() == 2 && closed.Load() != 1 {
				t.Error("previous response was not closed before retry")
			}
			resp.Body = closeObserver{resp.Body, &closed}
		}
		return resp, err
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := Open(ctx, srv.URL, WithHTTPClient(client), WithParallelThreshold(1),
		WithRetry(RetryPolicy{InitialBackoff: time.Millisecond, AttemptTimeout: time.Hour}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, payload) || closed.Load() != 2 {
		t.Fatalf("ReadAll = %q, %v; closed = %d; want recovered payload and both bodies closed", got, err, closed.Load())
	}
}

func TestChunkFailureCancelsSiblings(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusOK, http.StatusRequestedRangeNotSatisfiable, http.StatusPartialContent} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			t.Parallel()
			started, canceled := make(chan struct{}), make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Header.Get("Range") {
				case "bytes=0-0":
					serveRange(w, r, []byte("01234567"), nil)
				case "bytes=0-3":
					select {
					case <-started:
						// A 206 without Content-Range is also a permanent failure.
						http.Error(w, "permanent", status)
					case <-r.Context().Done():
					}
				case "bytes=4-7":
					close(started)
					<-r.Context().Done()
					close(canceled)
				}
			}))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r, err := Open(ctx, srv.URL, WithHTTPClient(srv.Client()), WithChunkSize(4),
				WithConcurrency(2), WithParallelThreshold(1))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if _, err := io.ReadAll(r); err == nil || errors.Is(err, context.Canceled) {
				t.Fatalf("Read error = %v, want original permanent failure", err)
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("terminal error did not cancel sibling request")
			}
		})
	}
}

func TestChunkOversizedBodyIsNotRetried(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			serveRange(w, r, []byte("payload"), nil)
			return
		}
		attempts.Add(1)
		w.Header().Set("Content-Range", "bytes 0-6/7")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("too many bytes"))
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
}

func TestRetryBackoff(t *testing.T) {
	// The wait must increase exponentially, stop at its cap, and never overflow.
	for _, tc := range []struct {
		delay, cap, want time.Duration
	}{
		{10, 40, 20},
		{20, 40, 40},
		{40, 40, 40},
		{30, 40, 40},
		{math.MaxInt64/2 + 1, math.MaxInt64, math.MaxInt64},
	} {
		if got := nextBackoff(tc.delay, tc.cap); got != tc.want {
			t.Errorf("nextBackoff(%v, %v) = %v, want %v", tc.delay, tc.cap, got, tc.want)
		}
	}
	var attempts atomic.Int32
	var times []time.Time // One worker and ReadAll synchronize access.
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		times = append(times, time.Now())
		return nil, io.EOF
	})}
	cfg := defaultConfig()
	cfg.HTTPClient = client
	cfg.Retry = RetryPolicy{MaxAttempts: 4, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 25 * time.Millisecond}
	if err := validateConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	r, err := openParallelReader(context.Background(), cfg, "http://example.invalid", objectInfo{Size: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.ReadAll(r); !errors.Is(err, io.EOF) {
		t.Fatalf("Read error = %v, want wrapped EOF", err)
	}
	if attempts.Load() != 4 {
		t.Fatalf("attempts = %d, want 4", attempts.Load())
	}
	for i, minDelay := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond} {
		if elapsed := times[i+1].Sub(times[i]); elapsed < minDelay {
			t.Error(fmt.Sprintf("retry %d waited %v, want at least %v", i+1, elapsed, minDelay))
		}
	}
}

type deadlineBody struct {
	ctx context.Context
	err error
}

func (b deadlineBody) Read([]byte) (int, error) { <-b.ctx.Done(); return 0, b.err }
func (b deadlineBody) Close() error             { return nil }

func TestChunkAttemptTimeoutPreservesOriginalError(t *testing.T) {
	for _, bodyTimeout := range []bool{false, true} {
		t.Run(fmt.Sprint(bodyTimeout), func(t *testing.T) {
			t.Parallel()
			bodyErr := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
			cfg := defaultConfig()
			cfg.Retry = RetryPolicy{MaxAttempts: 1, AttemptTimeout: time.Millisecond}
			cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if bodyTimeout {
					return &http.Response{StatusCode: http.StatusPartialContent,
						Body: deadlineBody{req.Context(), bodyErr}, Header: http.Header{"Content-Range": {"bytes 0-6/7"}}}, nil
				}
				<-req.Context().Done()
				return nil, req.Context().Err()
			})}
			if err := validateConfig(&cfg); err != nil {
				t.Fatal(err)
			}
			r, err := openParallelReader(context.Background(), cfg, "http://example.invalid", objectInfo{Size: 7})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			_, err = io.ReadAll(r)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error = %v, want deadline", err)
			}
			if bodyTimeout {
				var opErr *net.OpError
				if !errors.Is(err, bodyErr) || !errors.As(err, &opErr) {
					t.Fatalf("original body error lost: %v", err)
				}
			} else {
				var urlErr *url.Error
				if !errors.As(err, &urlErr) {
					t.Fatalf("original request error lost: %v", err)
				}
			}
		})
	}
}

type oversizedErrorBody struct{}

func (oversizedErrorBody) Read(p []byte) (int, error) {
	return copy(p, "12345678"), io.ErrUnexpectedEOF
}
func (oversizedErrorBody) Close() error { return nil }

func TestChunkOversizedBodyWithReadErrorIsNotRetried(t *testing.T) {
	var attempts atomic.Int32
	cfg := defaultConfig()
	cfg.Retry = RetryPolicy{InitialBackoff: time.Millisecond}
	cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return &http.Response{StatusCode: http.StatusPartialContent,
			Body: oversizedErrorBody{}, Header: http.Header{"Content-Range": {"bytes 0-6/7"}}}, nil
	})}
	if err := validateConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	r, err := openParallelReader(context.Background(), cfg, "http://example.invalid", objectInfo{Size: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if len(got) != 0 || err == nil || attempts.Load() != 1 {
		t.Fatalf("ReadAll = %q, %v; attempts = %d; want permanent failure after one attempt", got, err, attempts.Load())
	}
}
