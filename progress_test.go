package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

func TestParallelProgressHook(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("abcdefghij"), 200) // 2000 bytes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	defer srv.Close()

	var mu sync.Mutex
	var snapshots []Progress
	r, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithChunkSize(500),
		WithConcurrency(2),
		WithPrefetchWindow(4),
		WithParallelThreshold(1),
		WithProgress(func(p Progress) {
			mu.Lock()
			snapshots = append(snapshots, p)
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	waitForPrefetch(t, r)

	mu.Lock()
	preRead := append([]Progress(nil), snapshots...)
	mu.Unlock()
	if len(preRead) == 0 {
		t.Fatal("expected fetch progress before Read")
	}
	lastFetch := preRead[len(preRead)-1]
	if lastFetch.Total != int64(len(payload)) {
		t.Fatalf("Total=%d want %d", lastFetch.Total, len(payload))
	}
	if lastFetch.BytesFetched != int64(len(payload)) {
		t.Fatalf("BytesFetched=%d want %d before Read", lastFetch.BytesFetched, len(payload))
	}
	if lastFetch.BytesRead != 0 {
		t.Fatalf("BytesRead=%d want 0 before Read", lastFetch.BytesRead)
	}
	for i := 1; i < len(preRead); i++ {
		if preRead[i].BytesFetched < preRead[i-1].BytesFetched {
			t.Fatalf("BytesFetched went backwards: %+v -> %+v", preRead[i-1], preRead[i])
		}
		if preRead[i].BytesRead != 0 {
			t.Fatalf("BytesRead advanced before Read: %+v", preRead[i])
		}
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes", len(got))
	}

	mu.Lock()
	all := append([]Progress(nil), snapshots...)
	mu.Unlock()
	final := all[len(all)-1]
	if final.BytesRead != int64(len(payload)) || final.BytesFetched != int64(len(payload)) {
		t.Fatalf("final progress = %+v; want read=fetched=%d", final, len(payload))
	}
	var sawFetchedAhead bool
	for _, p := range all {
		if p.BytesFetched > p.BytesRead {
			sawFetchedAhead = true
		}
		if p.BytesRead > p.BytesFetched {
			t.Fatalf("BytesRead ahead of BytesFetched: %+v", p)
		}
		if p.Total != int64(len(payload)) {
			t.Fatalf("Total=%d want %d", p.Total, len(payload))
		}
	}
	if !sawFetchedAhead {
		t.Fatal("expected BytesFetched to lead BytesRead at some point")
	}
}

func TestFallbackProgressHook(t *testing.T) {
	t.Parallel()

	payload := []byte("fallback-progress-body")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	var mu sync.Mutex
	var snapshots []Progress
	r, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithProgress(func(p Progress) {
			mu.Lock()
			snapshots = append(snapshots, p)
			mu.Unlock()
		}),
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
		t.Fatalf("got %q want %q", got, payload)
	}

	mu.Lock()
	all := append([]Progress(nil), snapshots...)
	mu.Unlock()
	if len(all) == 0 {
		t.Fatal("expected progress callbacks")
	}
	for _, p := range all {
		if p.BytesRead != p.BytesFetched {
			t.Fatalf("fallback counters differ: %+v", p)
		}
		if p.Total != int64(len(payload)) {
			t.Fatalf("Total=%d want %d", p.Total, len(payload))
		}
	}
	final := all[len(all)-1]
	if final.BytesRead != int64(len(payload)) {
		t.Fatalf("final BytesRead=%d want %d", final.BytesRead, len(payload))
	}
}

func TestProgressHookNilIsNoOp(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("z"), 64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	defer srv.Close()

	r, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithChunkSize(16),
		WithParallelThreshold(1),
		WithProgress(nil),
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
		t.Fatalf("payload mismatch")
	}
}

func TestProgressHookSerialized(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("s"), 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	defer srv.Close()

	var concurrent atomic.Int32
	var overlapped atomic.Bool
	r, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithChunkSize(256),
		WithConcurrency(4),
		WithPrefetchWindow(4),
		WithParallelThreshold(1),
		WithProgress(func(Progress) {
			if concurrent.Add(1) != 1 {
				overlapped.Store(true)
			}
			concurrent.Add(-1)
		}),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if overlapped.Load() {
		t.Fatal("progress callback overlapped")
	}
}
