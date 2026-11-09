package downloader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
)

// referenceDownload performs a single streaming GET (no Range) for comparison.
func referenceDownload(t *testing.T, client *http.Client, url string) []byte {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("reference GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reference GET status: %s", resp.Status)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reference ReadAll: %v", err)
	}
	return got
}

func newRangeServer(t *testing.T, payload []byte) (*httptest.Server, *http.Client) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(w, r, payload, nil)
	}))
	t.Cleanup(srv.Close)
	return srv, srv.Client()
}

// testPayload builds a deterministic byte slice for table tests (corpus-style variety).
func testPayload(size int) []byte {
	if size == 0 {
		return nil
	}
	out := make([]byte, size)
	for i := range out {
		switch i % 7 {
		case 0:
			out[i] = byte(i)
		case 1:
			out[i] = 0xff
		case 2:
			out[i] = 0x00
		case 3:
			out[i] = byte(i >> 8)
		case 4:
			out[i] = 'A' + byte(i%26)
		case 5:
			out[i] = byte(size - i)
		default:
			out[i] = byte((i*31 + size) % 251)
		}
	}
	return out
}

func openAndReadAll(t *testing.T, client *http.Client, url string, opts ...Option) []byte {
	t.Helper()
	opts = append([]Option{WithHTTPClient(client)}, opts...)
	r, err := Open(context.Background(), url, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return got
}

func openAndReadBuffered(t *testing.T, client *http.Client, url string, bufSize int, opts ...Option) []byte {
	t.Helper()
	opts = append([]Option{WithHTTPClient(client)}, opts...)
	r, err := Open(context.Background(), url, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	var out bytes.Buffer
	buf := make([]byte, bufSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			out.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	return out.Bytes()
}

func assertBitIdentical(t *testing.T, got, want []byte) {
	t.Helper()
	if bytes.Equal(got, want) {
		return
	}
	gotHash := sha256.Sum256(got)
	wantHash := sha256.Sum256(want)
	t.Fatalf("payload mismatch: got %d bytes (sha256 %x), want %d bytes (sha256 %x)",
		len(got), gotHash, len(want), wantHash)
}

func TestBitIdenticalVsSingleStream(t *testing.T) {
	t.Parallel()

	type cfg struct {
		chunkSize         int64
		concurrency       int
		prefetch          int
		bufferMemoryLimit int64
		forceParallel     bool // WithParallelThreshold(1); false uses default threshold
	}

	for _, tc := range []struct {
		name    string
		size    int
		cfg     cfg
		readBuf int // 0 = io.ReadAll; else incremental Read buffer size
	}{
		{"empty fallback", 0, cfg{}, 0},
		{"one byte", 1, cfg{chunkSize: 1024, concurrency: 4, prefetch: 4, forceParallel: true}, 0},
		{"short tail chunk", 17, cfg{chunkSize: 8, concurrency: 3, prefetch: 2, forceParallel: true}, 0},
		{"exact multiple of chunk", 64, cfg{chunkSize: 16, concurrency: 2, prefetch: 4, forceParallel: true}, 0},
		{"one byte past chunk", 65, cfg{chunkSize: 16, concurrency: 8, prefetch: 1, forceParallel: true}, 0},
		{"kilobyte", 1024, cfg{chunkSize: 127, concurrency: 4, prefetch: 4, forceParallel: true}, 0},
		{"incremental read", 999, cfg{chunkSize: 64, concurrency: 2, prefetch: 3, forceParallel: true}, 13},
		{"memory cap", 5000, cfg{chunkSize: 512, concurrency: 4, prefetch: 4, bufferMemoryLimit: 1536, forceParallel: true}, 0},
		{"low concurrency", 8192, cfg{chunkSize: 1024, concurrency: 1, prefetch: 1, forceParallel: true}, 0},
		{"default threshold large", 9 << 20, cfg{chunkSize: 32 << 10, concurrency: 4, prefetch: 4}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload := testPayload(tc.size)
			srv, client := newRangeServer(t, payload)
			want := referenceDownload(t, client, srv.URL)

			var opts []Option
			if tc.cfg.chunkSize > 0 {
				opts = append(opts, WithChunkSize(tc.cfg.chunkSize))
			}
			if tc.cfg.concurrency > 0 {
				opts = append(opts, WithConcurrency(tc.cfg.concurrency))
			}
			if tc.cfg.prefetch > 0 {
				opts = append(opts, WithPrefetchWindow(tc.cfg.prefetch))
			}
			if tc.cfg.bufferMemoryLimit > 0 {
				opts = append(opts, WithBufferMemoryLimit(tc.cfg.bufferMemoryLimit))
			}
			if tc.cfg.forceParallel {
				opts = append(opts, WithParallelThreshold(1))
			}

			var got []byte
			if tc.readBuf > 0 {
				got = openAndReadBuffered(t, client, srv.URL, tc.readBuf, opts...)
			} else {
				got = openAndReadAll(t, client, srv.URL, opts...)
			}
			assertBitIdentical(t, got, want)
		})
	}
}

func TestBitIdenticalFallbackPath(t *testing.T) {
	t.Parallel()

	// Below default ParallelThreshold; parallel Range must not be used for body.
	payload := testPayload(4096)
	srv, client := newRangeServer(t, payload)
	want := referenceDownload(t, client, srv.URL)

	got, err := func() ([]byte, error) {
		r, err := Open(context.Background(), srv.URL, WithHTTPClient(client))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	}()
	if err != nil {
		t.Fatal(err)
	}
	assertBitIdentical(t, got, want)
}

func TestBitIdenticalCorpusSizes(t *testing.T) {
	t.Parallel()

	// Powers-of-two and off-by-one sizes against a fixed chunk layout.
	chunk := int64(997) // prime chunk size
	for _, size := range []int{1, 996, 997, 998, 1994, 4096, 16384} {
		size := size
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			t.Parallel()
			payload := testPayload(size)
			srv, client := newRangeServer(t, payload)
			want := referenceDownload(t, client, srv.URL)
			got := openAndReadAll(t, client, srv.URL,
				WithChunkSize(chunk),
				WithConcurrency(3),
				WithPrefetchWindow(5),
				WithParallelThreshold(1),
			)
			assertBitIdentical(t, got, want)
		})
	}
}

func FuzzBitIdenticalParallelDownload(f *testing.F) {
	// Seed corpus: small payloads and typical knob values.
	f.Add([]byte("abcdef"), int64(4), 2, 2)
	f.Add(testPayload(128), int64(16), 4, 4)

	f.Fuzz(func(t *testing.T, payload []byte, chunkSize int64, concurrency, prefetch int) {
		if len(payload) == 0 || len(payload) > 64<<10 {
			return
		}
		if chunkSize < 1 || chunkSize > 1<<20 {
			return
		}
		if concurrency < 1 || concurrency > 32 {
			return
		}
		if prefetch < 1 || prefetch > 32 {
			return
		}

		// Randomize memory budget sometimes to stress admission without changing bytes out.
		var memOpts []Option
		lim := int64(prefetch) * chunkSize
		if payload[0]%3 == 0 && lim >= chunkSize {
			memOpts = append(memOpts, WithBufferMemoryLimit(lim))
		}

		srv, client := newRangeServer(t, payload)
		want := referenceDownload(t, client, srv.URL)
		got := openAndReadAll(t, client, srv.URL, append(memOpts,
			WithChunkSize(chunkSize),
			WithConcurrency(concurrency),
			WithPrefetchWindow(prefetch),
			WithParallelThreshold(1),
		)...)
		if !bytes.Equal(got, want) {
			t.Fatalf("fuzz mismatch len got=%d want=%d", len(got), len(want))
		}
	})
}

func TestBitIdenticalPseudoRandomPayload(t *testing.T) {
	t.Parallel()

	// One heavier deterministic pseudo-random body (not fuzz) for stable CI signal.
	const size = 256 << 10
	rng := rand.New(rand.NewSource(0x60_d0_77_6e))
	payload := make([]byte, size)
	rng.Read(payload)

	srv, client := newRangeServer(t, payload)
	want := referenceDownload(t, client, srv.URL)
	got := openAndReadAll(t, client, srv.URL,
		WithChunkSize(777),
		WithConcurrency(6),
		WithPrefetchWindow(7),
		WithBufferMemoryLimit(4*777),
		WithParallelThreshold(1),
	)
	assertBitIdentical(t, got, want)
}
