package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// deterministicPayload returns repeatable bytes for corpus / fuzz inputs.
func deterministicPayload(size int, seed byte) []byte {
	if size <= 0 {
		return nil
	}
	out := make([]byte, size)
	var x uint64 = uint64(seed)*2654435761 + 17
	for i := range out {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = byte(x >> (i % 8))
	}
	return out
}

func singleStreamGET(t *testing.T, ctx context.Context, client *http.Client, url string) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status=%d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll GET body: %v", err)
	}
	return got
}

func parallelDownloadBytes(t *testing.T, ctx context.Context, url string, client *http.Client, opts ...Option) []byte {
	t.Helper()
	all := append([]Option{WithHTTPClient(client), WithParallelThreshold(1)}, opts...)
	r, err := Open(ctx, url, all...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll parallel: %v", err)
	}
	return got
}

func assertBitIdenticalToSingleStream(
	t *testing.T,
	payload []byte,
	opts ...Option,
) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	ctx := context.Background()
	client := srv.Client()
	want := singleStreamGET(t, ctx, client, srv.URL)
	got := parallelDownloadBytes(t, ctx, srv.URL, client, opts...)
	if !bytes.Equal(got, want) {
		first := firstMismatch(got, want)
		t.Fatalf("payload mismatch at byte %d (got %d bytes, want %d)", first, len(got), len(want))
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("parallel result diverged from server payload")
	}
}

func firstMismatch(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}

func TestBitIdenticalVsSingleStreamCorpus(t *testing.T) {
	sizes := []int{
		0, 1, 2, 3, 7, 8, 9, 15, 16, 17,
		127, 128, 129, 255, 256, 257,
		1023, 1024, 1025,
		4095, 4096, 4097,
		10_000, 65_537,
		256<<10 + 123,
	}
	configs := []struct {
		name string
		opts []Option
	}{
		{"chunk1_serial", []Option{WithChunkSize(1), WithConcurrency(1), WithPrefetchWindow(1)}},
		{"chunk7_parallel", []Option{WithChunkSize(7), WithConcurrency(3), WithPrefetchWindow(2)}},
		{"chunk1kb", []Option{WithChunkSize(1024), WithConcurrency(4), WithPrefetchWindow(4)}},
		{"chunk4kb_wide", []Option{WithChunkSize(4096), WithConcurrency(8), WithPrefetchWindow(8), WithBufferMemoryLimit(32 << 20)}},
	}

	for _, size := range sizes {
		payload := deterministicPayload(size, byte(size%251))
		for _, cfg := range configs {
			if size > 0 && int64(size) < 4096 && cfg.name == "chunk4kb_wide" {
				continue
			}
			t.Run(fmt.Sprintf("size=%d/%s", size, cfg.name), func(t *testing.T) {
				t.Parallel()
				assertBitIdenticalToSingleStream(t, payload, cfg.opts...)
			})
		}
	}
}

func TestBitIdenticalSmallReads(t *testing.T) {
	t.Parallel()

	payload := deterministicPayload(12_345, 42)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	defer srv.Close()

	ctx := context.Background()
	client := srv.Client()
	want := singleStreamGET(t, ctx, client, srv.URL)

	r, err := Open(ctx, srv.URL,
		WithHTTPClient(client),
		WithChunkSize(997),
		WithConcurrency(5),
		WithPrefetchWindow(4),
		WithParallelThreshold(1),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	var got bytes.Buffer
	buf := make([]byte, 13)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}

	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("small-read mismatch at %d", firstMismatch(got.Bytes(), want))
	}
}

func FuzzBitIdenticalParallelDownload(f *testing.F) {
	f.Add(byte(0), int32(1), int32(512), int32(2), int32(2))
	f.Add(byte(7), int32(4096), int32(1024), int32(4), int32(4))
	f.Add(byte(99), int32(8191), int32(37), int32(3), int32(2))

	f.Fuzz(func(t *testing.T, seed byte, size32 int32, chunk32 int32, conc32 int32, window32 int32) {
		size := int(size32)
		if size < 0 || size > 1<<18 {
			return
		}
		chunk := int64(chunk32)
		if chunk <= 0 {
			chunk = 1
		}
		if chunk > int64(size+1) && size > 0 {
			chunk = int64(size)
		}
		conc := int(conc32)
		if conc < 1 {
			conc = 1
		}
		if conc > 16 {
			conc = 16
		}
		window := int(window32)
		if window < 1 {
			window = 1
		}
		if window > 16 {
			window = 16
		}

		payload := deterministicPayload(size, seed)
		memLimit := chunk * int64(window)
		if memLimit < chunk {
			return
		}
		opts := []Option{
			WithChunkSize(chunk),
			WithConcurrency(conc),
			WithPrefetchWindow(window),
			WithBufferMemoryLimit(memLimit),
		}
		assertBitIdenticalToSingleStream(t, payload, opts...)
	})
}
