package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestResumeSaveFile_happyPathAndInterrupt(t *testing.T) {
	payload := deterministicPayload(48_000, 7)
	sum := sha256.Sum256(payload)

	var gets atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" && r.Header.Get("Range") != "bytes=0-0" {
			gets.Add(1)
		}
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	dest := filepath.Join(dir, "out.bin")
	state := filepath.Join(dir, "out.resume.json")

	ctx := context.Background()
	cancelCtx, cancel := context.WithCancel(ctx)

	opts := []Option{
		WithHTTPClient(srv.Client()),
		WithParallelThreshold(1),
		WithChunkSize(4096),
		WithConcurrency(2),
		WithProgress(func(p Progress) {
			if p.BytesRead >= 20_000 {
				cancel()
			}
		}),
	}

	r, err := Open(cancelCtx, srv.URL, append(opts, WithResume(ResumePaths{
		StatePath:   state,
		PartialPath: dest,
	}))...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, readErr := io.Copy(io.Discard, r)
	_ = r.Close()
	if readErr == nil {
		t.Fatal("expected cancel during read")
	}
	if !errors.Is(readErr, context.Canceled) {
		t.Fatalf("read err = %v", readErr)
	}

	firstGets := gets.Load()
	if firstGets == 0 {
		t.Fatal("expected parallel range GETs")
	}

	st, err := os.Stat(state)
	if err != nil {
		t.Fatalf("resume state missing: %v", err)
	}
	if st.Size() == 0 {
		t.Fatal("expected non-empty resume state")
	}

	gets.Store(0)
	err = ResumeSaveFile(ctx, srv.URL, dest, state, sum[:], opts...)
	if err != nil {
		t.Fatalf("ResumeSaveFile: %v", err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !digestEqual(got, payload) {
		t.Fatalf("dest mismatch len %d", len(got))
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("resume state should be removed, stat err=%v", err)
	}

	secondGets := gets.Load()
	if secondGets >= firstGets {
		t.Fatalf("resume should fetch fewer chunks: first=%d second=%d", firstGets, secondGets)
	}
}

func TestResumeState_invalidObject(t *testing.T) {
	payload := deterministicPayload(8000, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	dest := filepath.Join(dir, "out.bin")
	state := filepath.Join(dir, "bad.json")

	m := persistedChunkMap{
		Version:   chunkMapVersion,
		URL:       srv.URL,
		Size:      int64(len(payload)),
		ETag:      `"wrong"`,
		ChunkSize: 4096,
		Done:      []bool{true, false},
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(state, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(dest, make([]byte, len(payload)), 0o644)

	_, err = Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithParallelThreshold(1),
		WithChunkSize(4096),
		WithResume(ResumePaths{StatePath: state, PartialPath: dest}),
	)
	if !errors.Is(err, ErrResumeStateInvalid) {
		t.Fatalf("Open err = %v want ErrResumeStateInvalid", err)
	}
}

func TestWithResume_rejectsFallback(t *testing.T) {
	payload := []byte("small")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	_, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithResume(ResumePaths{
			StatePath:   filepath.Join(dir, "s.json"),
			PartialPath: filepath.Join(dir, "p.bin"),
		}),
	)
	if err == nil {
		t.Fatal("expected error when resume needs parallel path")
	}
}
