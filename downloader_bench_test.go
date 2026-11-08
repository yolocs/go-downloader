package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// BenchmarkParallelVsNaive compares parallel Range prefetch against a plain GET
// on a server with synthetic RTT and per-connection bandwidth limits.
func BenchmarkParallelVsNaive(b *testing.B) {
	payload := bytes.Repeat([]byte("bench-payload-!!"), 128<<10) // 2 MiB
	const rtt = 20 * time.Millisecond
	const bytesPerSec = 2 << 20 // 2 MiB/s per connection

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(rtt)
		serveRangeThrottled(w, r, payload, bytesPerSec)
	}))
	b.Cleanup(srv.Close)

	b.Run("naive", func(b *testing.B) {
		client := srv.Client()
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.Get(srv.URL)
			if err != nil {
				b.Fatal(err)
			}
			n, err := io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if err != nil {
				b.Fatal(err)
			}
			if n != int64(len(payload)) {
				b.Fatalf("read %d", n)
			}
		}
	})

	b.Run("parallel", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r, err := Open(context.Background(), srv.URL,
				WithHTTPClient(srv.Client()),
				WithChunkSize(256<<10),
				WithConcurrency(8),
				WithPrefetchWindow(8),
				WithParallelThreshold(1),
			)
			if err != nil {
				b.Fatal(err)
			}
			n, err := io.Copy(io.Discard, r)
			r.Close()
			if err != nil {
				b.Fatal(err)
			}
			if n != int64(len(payload)) {
				b.Fatalf("read %d", n)
			}
		}
	})
}

func serveRangeThrottled(w http.ResponseWriter, r *http.Request, payload []byte, bytesPerSec int) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", `"bench-etag"`)

	rng := r.Header.Get("Range")
	if rng == "" {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		writeThrottled(w, payload, bytesPerSec)
		return
	}

	var start, end int64
	if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil {
		http.Error(w, "bad range", http.StatusBadRequest)
		return
	}
	if start < 0 || end >= int64(len(payload)) || start > end {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(payload)))
		http.Error(w, "unsatisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	chunk := payload[start : end+1]
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(chunk)))
	w.WriteHeader(http.StatusPartialContent)
	writeThrottled(w, chunk, bytesPerSec)
}

func writeThrottled(w http.ResponseWriter, data []byte, bytesPerSec int) {
	const piece = 32 << 10
	flusher, _ := w.(http.Flusher)
	for i := 0; i < len(data); i += piece {
		end := i + piece
		if end > len(data) {
			end = len(data)
		}
		_, _ = w.Write(data[i:end])
		if flusher != nil {
			flusher.Flush()
		}
		n := end - i
		time.Sleep(time.Duration(n) * time.Second / time.Duration(bytesPerSec))
	}
}
