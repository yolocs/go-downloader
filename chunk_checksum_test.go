package downloader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestChunkChecksumsFromPayload(t *testing.T) {
	payload := []byte("abcdefghijklmnop") // 16 bytes
	digests := ChunkChecksumsFromPayload(payload, 6)
	if len(digests) != 3 {
		t.Fatalf("want 3 chunks, got %d", len(digests))
	}
	for i, br := range planChunks(int64(len(payload)), 6) {
		sum := sha256.Sum256(payload[br.Start:br.End])
		if !digestEqual(digests[i], sum[:]) {
			t.Fatalf("chunk %d digest mismatch", i)
		}
	}
}

func TestWithChunkChecksums_happyPath(t *testing.T) {
	payload := deterministicPayload(24_000, 5)
	chunk := int64(4096)
	digests := ChunkChecksumsFromPayload(payload, chunk)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	r, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithChunkSize(chunk),
		WithParallelThreshold(1),
		WithChunkChecksums(digests),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestWithChunkChecksums_mismatch(t *testing.T) {
	payload := deterministicPayload(12_000, 2)
	chunk := int64(4096)
	digests := ChunkChecksumsFromPayload(payload, chunk)
	digests[1][0] ^= 0xff

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	r, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithChunkSize(chunk),
		WithConcurrency(2),
		WithPrefetchWindow(4),
		WithParallelThreshold(1),
		WithChunkChecksums(digests),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = io.ReadAll(r)
	_ = r.Close()
	if !errors.Is(err, ErrChunkDigestMismatch) {
		t.Fatalf("got %v, want ErrChunkDigestMismatch", err)
	}
}

func TestWithChunkChecksums_rejectsFallback(t *testing.T) {
	payload := []byte("small-payload")
	digests := ChunkChecksumsFromPayload(payload, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	_, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithParallelThreshold(1<<20),
		WithChunkChecksums(digests),
	)
	if err == nil {
		t.Fatal("expected error when chunk checksums require parallel path")
	}
}

func TestWithChunkChecksums_rejectsWrongCount(t *testing.T) {
	payload := deterministicPayload(9000, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	digests := ChunkChecksumsFromPayload(payload, 4096)
	digests = digests[:1]

	_, err := Open(context.Background(), srv.URL,
		WithHTTPClient(srv.Client()),
		WithChunkSize(4096),
		WithParallelThreshold(1),
		WithChunkChecksums(digests),
	)
	if err == nil {
		t.Fatal("expected validation error")
	}
}
