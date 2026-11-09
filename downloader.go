// Package downloader provides a high-performance HTTP downloader that
// exposes a sequential io.Reader while prefetching byte ranges in parallel.
//
// See README.md for architecture and TRACKING.md for the implementation backlog.
package downloader

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"
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
	// the reorder buffer. On the parallel path this may be ahead of
	// BytesRead; on the single-stream fallback it equals BytesRead.
	BytesFetched int64
	// Total is Content-Length when known, or -1.
	Total int64
}

// RetryPolicy controls per-chunk retry behavior.
// Zero-valued fields use defaults: 3 attempts, 100 ms initial backoff,
// 2 s maximum backoff, and a 30 s timeout per attempt.
type RetryPolicy struct {
	// MaxAttempts includes the initial request. One disables retries.
	MaxAttempts int
	// InitialBackoff is the delay before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponentially increasing delay.
	MaxBackoff time.Duration
	// AttemptTimeout bounds each Range request, including reading its body.
	AttemptTimeout time.Duration
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

// WithPrefetchWindow caps admitted chunks, including active requests, queued
// chunks, and the partially consumed current chunk. It must be positive.
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
// It applies only to parallel Range requests, not the probe or fallback GET.
func WithRetry(policy RetryPolicy) Option {
	return func(c *Config) {
		c.Retry = policy
	}
}

// WithBufferMemoryLimit caps chunk payload storage in bytes, including active
// requests, queued chunks, and the partially consumed current chunk. Zero derives
// the limit from ChunkSize * PrefetchWindow. A positive limit must be at least
// ChunkSize. Negative limits and an overflowing derived limit are rejected.
// The limit excludes HTTP/runtime overhead, chunk metadata, and caller buffers;
// it does not apply to the probe or single-stream fallback.
func WithBufferMemoryLimit(n int64) Option {
	return func(c *Config) {
		c.BufferMemoryLimit = n
	}
}

// WithProgress registers a callback invoked as the download advances.
//
// The callback runs while the reader holds its internal lock, so it is
// serialized per Reader and must not call methods on that Reader (deadlock)
// or block for long. BytesFetched may advance ahead of BytesRead on the
// parallel path; on the single-stream fallback the two counters stay equal.
func WithProgress(fn func(Progress)) Option {
	return func(c *Config) {
		c.OnProgress = fn
	}
}

// WithParallelThreshold sets the minimum object size (bytes) required to
// use parallel Range GETs. Smaller objects use a single streaming GET.
func WithParallelThreshold(n int64) Option {
	return func(c *Config) {
		c.ParallelThreshold = n
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
// Open probes Range support, then either prefetches fixed-size byte
// ranges in parallel (reassembled in order) or falls back to a single
// streaming GET when Range is unavailable, size is unknown, or the
// object is below ParallelThreshold.
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

	info, err := probe(ctx, cfg.HTTPClient, url, cfg.Header)
	if err != nil {
		return nil, err
	}

	useParallel := info.AcceptRanges && info.Size > 0 && info.Size >= cfg.ParallelThreshold
	if !useParallel {
		return openFallback(ctx, cfg, url, info.Size)
	}
	return openParallelReader(ctx, cfg, url, info)
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
	if c.BufferMemoryLimit < 0 {
		return fmt.Errorf("downloader: BufferMemoryLimit must not be negative")
	}
	if c.BufferMemoryLimit == 0 {
		if c.ChunkSize > math.MaxInt64/int64(c.PrefetchWindow) {
			return fmt.Errorf("downloader: derived BufferMemoryLimit overflows int64")
		}
		c.BufferMemoryLimit = c.ChunkSize * int64(c.PrefetchWindow)
	}
	if c.BufferMemoryLimit < c.ChunkSize {
		return fmt.Errorf("downloader: BufferMemoryLimit must be >= ChunkSize")
	}
	if c.HTTPClient == nil {
		return fmt.Errorf("downloader: HTTPClient must not be nil")
	}
	return normalizeRetryPolicy(&c.Retry)
}
