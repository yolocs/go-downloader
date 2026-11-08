// Package downloader provides a high-performance HTTP downloader that
// exposes a sequential io.Reader while prefetching byte ranges in parallel.
//
// See DESIGN.md for architecture, use cases, and the implementation roadmap.
package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// Default tuning knobs. Safe starting points; override via Option.
const (
	DefaultChunkSize         int64 = 2 << 20 // 2 MiB
	DefaultConcurrency             = 4
	DefaultPrefetchWindow          = 4
	DefaultParallelThreshold       = 8 << 20 // below this, use a single GET
)

// Reader is the optional richer handle returned by Open.
// The required contract for callers is io.ReadCloser; Size is advisory.
type Reader interface {
	io.ReadCloser

	// Size returns the total object size in bytes when known, or -1.
	Size() int64
}

// Progress reports download advancement for WithProgress hooks.
type Progress struct {
	// BytesRead is the number of bytes consumed via Read so far.
	BytesRead int64
	// BytesFetched is the number of bytes successfully downloaded into
	// the reorder buffer (may be ahead of BytesRead).
	BytesFetched int64
	// Total is Content-Length when known, or -1.
	Total int64
}

// RetryPolicy controls per-chunk retry behavior.
// Zero value means "use package defaults" once implemented.
type RetryPolicy struct {
	MaxAttempts int
	// Future: backoff strategy, retryable status codes, etc.
}

// Config holds resolved options for a download.
// Constructed via Option values; not typically built by hand.
type Config struct {
	ChunkSize         int64
	Concurrency       int
	PrefetchWindow    int
	ParallelThreshold int64
	HTTPClient        *http.Client
	Header            http.Header
	Retry             RetryPolicy
	BufferMemoryLimit int64
	OnProgress        func(Progress)
}

// Option mutates Config during Open.
type Option func(*Config)

// WithChunkSize sets the Range chunk size in bytes.
func WithChunkSize(n int64) Option {
	return func(c *Config) {
		c.ChunkSize = n
	}
}

// WithConcurrency sets the maximum number of parallel Range requests.
func WithConcurrency(n int) Option {
	return func(c *Config) {
		c.Concurrency = n
	}
}

// WithPrefetchWindow sets how many chunks may be buffered ahead of the
// read cursor.
func WithPrefetchWindow(n int) Option {
	return func(c *Config) {
		c.PrefetchWindow = n
	}
}

// WithHTTPClient supplies a custom HTTP client (auth, proxy, transport).
func WithHTTPClient(client *http.Client) Option {
	return func(c *Config) {
		c.HTTPClient = client
	}
}

// WithHeader adds a request header on probe and Range requests.
// Repeated calls append values for the same key.
func WithHeader(key, value string) Option {
	return func(c *Config) {
		if c.Header == nil {
			c.Header = make(http.Header)
		}
		c.Header.Add(key, value)
	}
}

// WithRetry sets the per-chunk retry policy.
func WithRetry(policy RetryPolicy) Option {
	return func(c *Config) {
		c.Retry = policy
	}
}

// WithBufferMemoryLimit caps approximate in-memory readahead in bytes.
// Zero means "derive from chunk size × prefetch window".
func WithBufferMemoryLimit(n int64) Option {
	return func(c *Config) {
		c.BufferMemoryLimit = n
	}
}

// WithProgress registers a callback invoked as the download advances.
// The callback must be non-blocking; it may run on fetcher goroutines.
func WithProgress(fn func(Progress)) Option {
	return func(c *Config) {
		c.OnProgress = fn
	}
}

func defaultConfig() Config {
	return Config{
		ChunkSize:         DefaultChunkSize,
		Concurrency:       DefaultConcurrency,
		PrefetchWindow:    DefaultPrefetchWindow,
		ParallelThreshold: DefaultParallelThreshold,
		HTTPClient:        http.DefaultClient,
		Retry: RetryPolicy{
			MaxAttempts: 3,
		},
	}
}

// Open begins a download of url and returns a sequential reader.
//
// The implementation will probe Range support, prefetch chunks in
// parallel, and reassemble them in order. Until Phase 1 lands, Open
// returns an error explaining that the downloader is not implemented.
func Open(ctx context.Context, url string, opts ...Option) (Reader, error) {
	if ctx == nil {
		return nil, fmt.Errorf("downloader: nil context")
	}
	if url == "" {
		return nil, fmt.Errorf("downloader: empty url")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	// Phase 1: probe → plan → fetch → ordered Read.
	_ = cfg
	return nil, fmt.Errorf("downloader: Open not implemented yet; see DESIGN.md")
}

func validateConfig(c *Config) error {
	if c.ChunkSize <= 0 {
		return fmt.Errorf("downloader: ChunkSize must be > 0")
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("downloader: Concurrency must be > 0")
	}
	if c.PrefetchWindow <= 0 {
		return fmt.Errorf("downloader: PrefetchWindow must be > 0")
	}
	if c.HTTPClient == nil {
		return fmt.Errorf("downloader: HTTPClient must not be nil")
	}
	return nil
}
