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
)

func TestProbeRangeResponses(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		contentRange string
		wantEmpty    bool
	}{
		{"empty object", 416, "bytes */0", true},
		{"nonempty unsatisfied", 416, "bytes */8", false},
		{"missing unsatisfied length", 416, "", false},
		{"unknown unsatisfied length", 416, "bytes */*", false},
		{"satisfied on 416", 416, "bytes 0-0/8", false},
		{"wrong probe start", 206, "bytes 1-1/8", false},
		{"wrong probe end", 206, "bytes 0-1/8", false},
		{"empty satisfied range", 206, "bytes 0-0/0", false},
		{"unsatisfied on 206", 206, "bytes */8", false},
		{"missing offsets", 206, "bytes nonsense/8", false},
		{"negative offsets", 206, "bytes -1-0/8", false},
		{"signed offsets", 206, "bytes +0-0/8", false},
		{"signed total", 206, "bytes 0-0/+8", false},
		{"overflow total", 206, "bytes 0-0/9223372036854775808", false},
		{"unknown total", 206, "bytes 0-0/*", false},
		{"missing total", 206, "bytes 0-0/", false},
		{"multiple ranges", 206, "bytes 0-0/8, bytes 1-1/8", false},
		{"wrong unit", 206, "items 0-0/8", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var plainGETs atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Header.Get("Range") == "" {
					plainGETs.Add(1)
					w.WriteHeader(http.StatusOK)
					return
				}
				w.Header().Set("Content-Range", tc.contentRange)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("x"))
			}))
			defer srv.Close()
			r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()), WithParallelThreshold(1))
			if r != nil {
				defer r.Close()
			}
			if !tc.wantEmpty {
				if err == nil || r != nil || plainGETs.Load() != 0 {
					t.Fatalf("Open = %v, %v; plain GETs = %d; want probe error", r, err, plainGETs.Load())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			if err != nil || len(got) != 0 || r.Size() != 0 || plainGETs.Load() != 1 {
				t.Fatalf("ReadAll = %q, %v; size = %d; plain GETs = %d; want empty stream", got, err, r.Size(), plainGETs.Load())
			}
		})
	}
}

func TestChunkRejectsMismatchedContentRange(t *testing.T) {
	for _, contentRange := range []string{
		"", "bytes nonsense/8", "bytes 4-7/8", "bytes 0-2/8",
		"bytes 0-3/9", "bytes 0-3/*", "bytes */8", "bytes 3-0/8",
		"bytes 0-8/8", "bytes 0-3/3",
	} {
		t.Run(contentRange, func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Header.Get("Range") == "bytes=0-0" {
					serveRange(w, req, []byte("abcdefgh"), nil)
					return
				}
				attempts.Add(1)
				w.Header().Set("Content-Range", contentRange)
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("WRNG")) // Right length, wrong representation.
			}))
			defer srv.Close()
			r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
				WithChunkSize(4), WithConcurrency(1), WithPrefetchWindow(1), WithParallelThreshold(1))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := io.ReadAll(r)
			if err == nil || len(got) != 0 || attempts.Load() != 1 {
				t.Fatalf("ReadAll = %q, %v; attempts = %d; want permanent failure without chunk bytes", got, err, attempts.Load())
			}
		})
	}
}

func TestRangeFailureAfterDeliveredPrefix(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusRequestedRangeNotSatisfiable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			t.Parallel()
			payload := []byte("abcdefgh")
			var attempts, plainGETs atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.Header.Get("Range") {
				case "":
					plainGETs.Add(1)
				case "bytes=4-7":
					attempts.Add(1)
					w.Header().Set("Content-Range", "bytes */4")
					w.WriteHeader(status)
					_, _ = w.Write([]byte("WRONG RESPONSE BODY"))
					return
				}
				serveRange(w, req, payload, nil)
			}))
			defer srv.Close()
			r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()),
				WithChunkSize(4), WithConcurrency(1), WithPrefetchWindow(1), WithParallelThreshold(1))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			prefix := make([]byte, 4)
			if _, err := io.ReadFull(r, prefix); err != nil || !bytes.Equal(prefix, []byte("abcd")) {
				t.Fatalf("ReadFull = %q, %v; want valid prefix", prefix, err)
			}
			got, err := io.ReadAll(r)
			if err == nil || !strings.Contains(err.Error(), fmt.Sprint(status)) || len(got) != 0 || attempts.Load() != 1 || plainGETs.Load() != 0 {
				t.Fatalf("ReadAll = %q, %v; attempts = %d; plain GETs = %d; want terminal error without restarting", got, err, attempts.Load(), plainGETs.Load())
			}
		})
	}
}

// A probe must only inspect headers on ignored or rejected Range responses.
// Reading these bodies can block Open indefinitely on a streaming server.
type unreadProbeBody struct {
	reads, closes atomic.Int32
}

func (b *unreadProbeBody) Read([]byte) (int, error) {
	b.reads.Add(1)
	return 0, fmt.Errorf("probe body must not be read")
}

func (b *unreadProbeBody) Close() error { b.closes.Add(1); return nil }

func TestProbeClosesIgnoredOrRejectedBodyWithoutReading(t *testing.T) {
	for _, status := range []int{200, 416, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			body := &unreadProbeBody{}
			var plainGETs int
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("Range") != "" {
					return &http.Response{StatusCode: status, Status: fmt.Sprint(status), Body: body, Header: make(http.Header)}, nil
				}
				plainGETs++
				if body.closes.Load() != 1 {
					t.Error("probe response was not closed before fallback")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("payload")), Header: make(http.Header)}, nil
			})}
			r, err := Open(context.Background(), "http://example.invalid", WithHTTPClient(client))
			if r != nil {
				defer r.Close()
			}
			if status == 200 {
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				if err != nil || string(got) != "payload" || plainGETs != 1 {
					t.Fatalf("fallback = %q, %v; GETs = %d", got, err, plainGETs)
				}
			} else if err == nil || plainGETs != 0 {
				t.Fatalf("Open error = %v; plain GETs = %d; want probe error", err, plainGETs)
			}
			if body.reads.Load() != 0 || body.closes.Load() != 1 {
				t.Fatalf("probe body reads = %d, closes = %d; want 0, 1", body.reads.Load(), body.closes.Load())
			}
		})
	}
}

func TestChunkClosesInvalidResponseWithoutReading(t *testing.T) {
	for _, status := range []int{200, 206, 416} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			body := &unreadProbeBody{}
			var attempts atomic.Int32
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("Range") == "bytes=0-0" {
					return &http.Response{StatusCode: 206, Body: io.NopCloser(strings.NewReader("p")),
						Header: http.Header{"Content-Range": {"bytes 0-0/7"}}}, nil
				}
				attempts.Add(1)
				return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}, nil
			})}
			r, err := Open(context.Background(), "http://example.invalid", WithHTTPClient(client), WithParallelThreshold(1))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := io.ReadAll(r)
			if err == nil || len(got) != 0 || attempts.Load() != 1 || body.reads.Load() != 0 || body.closes.Load() != 1 {
				t.Fatalf("ReadAll = %q, %v; attempts = %d; body reads/closes = %d/%d; want immediate rejection and close", got, err, attempts.Load(), body.reads.Load(), body.closes.Load())
			}
		})
	}
}
