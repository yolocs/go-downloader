package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestParseSHA256Hex(t *testing.T) {
	payload := []byte("hello")
	sum := sha256.Sum256(payload)
	got, err := ParseSHA256Hex(hex.EncodeToString(sum[:]))
	if err != nil {
		t.Fatalf("ParseSHA256Hex: %v", err)
	}
	if !digestEqual(got, sum[:]) {
		t.Fatal("decoded digest mismatch")
	}
	if _, err := ParseSHA256Hex("abc"); err == nil {
		t.Fatal("expected error for short hex")
	}
	if _, err := ParseSHA256Hex(hex.EncodeToString(sum[:]) + "zz"); err == nil {
		t.Fatal("expected error for invalid hex")
	}
}

func TestSaveFile_happyPath(t *testing.T) {
	payload := deterministicPayload(50_000, 3)
	sum := sha256.Sum256(payload)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	dest := filepath.Join(dir, "artifact.bin")

	ctx := context.Background()
	err := SaveFile(ctx, srv.URL, dest, sum[:],
		WithHTTPClient(srv.Client()),
		WithParallelThreshold(1),
		WithChunkSize(4096),
		WithConcurrency(3),
	)
	if err != nil {
		t.Fatalf("SaveFile: %v", err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !digestEqual(got, payload) {
		t.Fatalf("dest contents mismatch (len %d)", len(got))
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want one file in dir, got %d", len(entries))
	}
}

func TestSaveFile_digestMismatch(t *testing.T) {
	payload := []byte("payload-bytes")
	wrong := sha256.Sum256([]byte("other"))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	dest := filepath.Join(dir, "out.bin")

	err := SaveFile(context.Background(), srv.URL, dest, wrong[:], WithHTTPClient(srv.Client()))
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("got err=%v, want ErrDigestMismatch", err)
	}
	if _, err := os.Stat(dest); err == nil {
		t.Fatal("dest file should not exist")
	}
	matches, _ := filepath.Glob(filepath.Join(dir, ".downloader-*"))
	if len(matches) != 0 {
		t.Fatalf("temp files left behind: %v", matches)
	}
}

func TestSaveFile_rejectsBadExpectedDigest(t *testing.T) {
	err := SaveFile(context.Background(), "http://example.com/", filepath.Join(t.TempDir(), "x"), []byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error")
	}
}
