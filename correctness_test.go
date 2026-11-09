package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
)

// singleStreamReference downloads url with one plain GET, matching the
// correctness invariant that parallel Range assembly must equal [0, Size).
func singleStreamReference(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("reference GET: %s", resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func assertBitIdenticalToSingleStream(t *testing.T, payload []byte, opts ...Option) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)

	base := []Option{
		WithHTTPClient(srv.Client()),
		WithParallelThreshold(1), // exercise parallel path for multi-byte objects
	}
	want, err := singleStreamReference(context.Background(), srv.Client(), srv.URL)
	if err != nil {
		t.Fatalf("reference GET: %v", err)
	}

	allOpts := append(base, opts...)
	r, err := Open(context.Background(), srv.URL, allOpts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	got, err := readViaBufferedReads(r)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, want) {
		reportPayloadMismatch(t, got, want)
	}
	if r.Size() >= 0 && int64(len(want)) != r.Size() {
		t.Fatalf("Size()=%d want %d", r.Size(), len(want))
	}
}

// readViaBufferedReads drains r with varying buffer sizes so chunk stitching
// is exercised across partial Reads, not only ReadAll coalescing.
func readViaBufferedReads(r Reader) ([]byte, error) {
	var out []byte
	buf := make([]byte, 17) // deliberate non-power-of-two
	for {
		n, err := r.Read(buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
	}
}

func reportPayloadMismatch(t *testing.T, got, want []byte) {
	t.Helper()
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			t.Fatalf("payload mismatch at offset %d: got %d bytes, want %d (got[%d]=%#x want[%d]=%#x)",
				i, len(got), len(want), i, got[i], i, want[i])
		}
	}
	t.Fatalf("payload length mismatch: got %d bytes, want %d", len(got), len(want))
}

func pseudoRandomPayload(seed int64, size int) []byte {
	if size == 0 {
		return nil
	}
	src := rand.New(rand.NewSource(seed))
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(src.Intn(256))
	}
	return out
}

type bitIdenticalCase struct {
	name       string
	size       int
	chunkSize  int64
	concurrent int
	prefetch   int
}

// bitIdenticalCorpus covers boundary sizes and planner/worker combinations
// without exploding request count (e.g. no 1 MiB object with 1-byte chunks).
func bitIdenticalCorpus() []bitIdenticalCase {
	sizes := []int{
		0, 1, 2, 7, 8, 9, 15, 16, 17,
		127, 128, 129, 255, 256, 257,
		1023, 1024, 1025, 4095, 4096, 4097,
		10000, 256 << 10,
	}
	chunks := []int64{1, 7, 128, 1024, 4096}
	concurrency := []int{1, 2, 4, 8}
	prefetch := []int{1, 2, 4}

	var cases []bitIdenticalCase
	for _, size := range sizes {
		for _, chunk := range chunks {
			if size > 0 && chunk > int64(size) {
				continue
			}
			if size > 0 {
				nChunks := (int64(size) + chunk - 1) / chunk
				if nChunks > 512 {
					continue
				}
			}
			for _, c := range concurrency {
				for _, w := range prefetch {
					cases = append(cases, bitIdenticalCase{
						name:       fmt.Sprintf("size=%d_chunk=%d_conc=%d_win=%d", size, chunk, c, w),
						size:       size,
						chunkSize:  chunk,
						concurrent: c,
						prefetch:   w,
					})
				}
			}
		}
	}
	return cases
}

func TestBitIdenticalCorpus(t *testing.T) {
	for _, tc := range bitIdenticalCorpus() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload := pseudoRandomPayload(int64(tc.size)*991+int64(tc.size), tc.size)
			assertBitIdenticalToSingleStream(t, payload,
				WithChunkSize(tc.chunkSize),
				WithConcurrency(tc.concurrent),
				WithPrefetchWindow(tc.prefetch),
			)
		})
	}
}

func TestBitIdenticalFallbackPath(t *testing.T) {
	t.Parallel()

	payload := pseudoRandomPayload(42, 50_000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Ignore Range on all requests but still advertise length.
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	want, err := singleStreamReference(context.Background(), srv.Client(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	r, err := Open(context.Background(), srv.URL, WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	got, err := readViaBufferedReads(r)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, want) {
		reportPayloadMismatch(t, got, want)
	}
}

func FuzzBitIdenticalParallelDownload(f *testing.F) {
	f.Add(int(1024), int64(128), 3, 2)
	f.Add(int(0), int64(1), 1, 1)

	f.Fuzz(func(t *testing.T, size int, chunkSize int64, concurrency, prefetch int) {
		if size < 0 || size > 256<<10 {
			return
		}
		if chunkSize < 1 || chunkSize > 1<<20 {
			return
		}
		if concurrency < 1 || concurrency > 16 {
			return
		}
		if prefetch < 1 || prefetch > 16 {
			return
		}
		if size > 0 {
			nChunks := (int64(size) + chunkSize - 1) / chunkSize
			if nChunks > 512 {
				return
			}
		}
		payload := pseudoRandomPayload(0xF00D, size)
		assertBitIdenticalToSingleStream(t, payload,
			WithChunkSize(chunkSize),
			WithConcurrency(concurrency),
			WithPrefetchWindow(prefetch),
		)
	})
}
