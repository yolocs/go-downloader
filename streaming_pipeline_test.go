package downloader

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func buildGzip(t *testing.T, plain []byte) []byte {
	t.Helper()
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(plain); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return gz.Bytes()
}

func buildTarGz(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var plain bytes.Buffer
	tw := tar.NewWriter(&plain)
	for name, content := range files {
		hdr := &tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("tar header: %v", err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatalf("tar write: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	return buildGzip(t, plain.Bytes())
}

func TestStreamingGzipDecompress(t *testing.T) {
	t.Parallel()

	plain := deterministicPayload(1<<20+123, 9)
	payload := buildGzip(t, plain)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	ctx := context.Background()
	client := srv.Client()
	r, err := Open(ctx, srv.URL,
		WithHTTPClient(client),
		WithChunkSize(64<<10),
		WithParallelThreshold(1),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	gr, err := gzip.NewReader(r)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gr.Close()

	got, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("ReadAll decompressed: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("decompressed length=%d want %d", len(got), len(plain))
	}
}

func TestStreamingTarGzExtract(t *testing.T) {
	t.Parallel()

	wantFiles := map[string][]byte{
		"alpha.txt":    []byte("first\n"),
		"nested/beta":  deterministicPayload(4096, 3),
		"nested/gamma": []byte("third"),
	}
	payload := buildTarGz(t, wantFiles)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	ctx := context.Background()
	client := srv.Client()
	r, err := Open(ctx, srv.URL,
		WithHTTPClient(client),
		WithChunkSize(32<<10),
		WithParallelThreshold(1),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	gr, err := gzip.NewReader(r)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gr.Close()

	dir := t.TempDir()
	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next: %v", err)
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			continue
		}
		dest := filepath.Join(dir, filepath.Clean(hdr.Name))
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		f, err := os.Create(dest)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			t.Fatalf("copy: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	for name, want := range wantFiles {
		path := filepath.Join(dir, filepath.Clean(name))
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %q: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("file %q: len(got)=%d want %d", name, len(got), len(want))
		}
	}
}
