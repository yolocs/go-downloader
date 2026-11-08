package downloader

import (
	"context"
	"net/http"
	"testing"
)

func TestOpenValidation(t *testing.T) {
	t.Parallel()

	t.Run("nil context", func(t *testing.T) {
		t.Parallel()
		_, err := Open(nil, "https://example.com/x")
		if err == nil {
			t.Fatal("expected error for nil context")
		}
	})

	t.Run("empty url", func(t *testing.T) {
		t.Parallel()
		_, err := Open(context.Background(), "")
		if err == nil {
			t.Fatal("expected error for empty url")
		}
	})

	t.Run("bad chunk size", func(t *testing.T) {
		t.Parallel()
		_, err := Open(context.Background(), "https://example.com/x", WithChunkSize(0))
		if err == nil {
			t.Fatal("expected error for ChunkSize <= 0")
		}
	})

	t.Run("not implemented yet", func(t *testing.T) {
		t.Parallel()
		_, err := Open(context.Background(), "https://example.com/x")
		if err == nil {
			t.Fatal("expected not-implemented error until Phase 1")
		}
	})
}

func TestOptionsApply(t *testing.T) {
	t.Parallel()

	client := &http.Client{}
	cfg := defaultConfig()
	for _, opt := range []Option{
		WithChunkSize(1 << 20),
		WithConcurrency(8),
		WithPrefetchWindow(2),
		WithHTTPClient(client),
		WithHeader("Authorization", "Bearer test"),
		WithRetry(RetryPolicy{MaxAttempts: 5}),
		WithBufferMemoryLimit(16 << 20),
		WithProgress(func(Progress) {}),
	} {
		opt(&cfg)
	}

	if cfg.ChunkSize != 1<<20 {
		t.Fatalf("ChunkSize = %d", cfg.ChunkSize)
	}
	if cfg.Concurrency != 8 {
		t.Fatalf("Concurrency = %d", cfg.Concurrency)
	}
	if cfg.PrefetchWindow != 2 {
		t.Fatalf("PrefetchWindow = %d", cfg.PrefetchWindow)
	}
	if cfg.HTTPClient != client {
		t.Fatal("HTTPClient not applied")
	}
	if got := cfg.Header.Get("Authorization"); got != "Bearer test" {
		t.Fatalf("Header Authorization = %q", got)
	}
	if cfg.Retry.MaxAttempts != 5 {
		t.Fatalf("Retry.MaxAttempts = %d", cfg.Retry.MaxAttempts)
	}
	if cfg.BufferMemoryLimit != 16<<20 {
		t.Fatalf("BufferMemoryLimit = %d", cfg.BufferMemoryLimit)
	}
	if cfg.OnProgress == nil {
		t.Fatal("OnProgress not applied")
	}
}
