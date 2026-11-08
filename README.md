# go-downloader

A high-performance HTTP downloader for Go with a friendly `io.Reader` API.

Single-connection streaming leaves bandwidth on the table when latency is
high or the server rate-limits per connection. `go-downloader` keeps the
caller API boring — just `Read` / `io.Copy` — while prefetching byte
ranges over HTTP in parallel and reassembling them in order.

```go
r, err := downloader.Open(ctx, url,
    downloader.WithConcurrency(8),
    downloader.WithChunkSize(2<<20),
)
if err != nil {
    return err
}
defer r.Close()
_, err = io.Copy(dst, r)
```

Implementation status and the feature backlog live in
[TRACKING.md](TRACKING.md). Cross items out there as they land; do not
delete them.

## Idea

Expose a sequential byte stream (`io.Reader` / `io.ReadCloser`) while,
under the hood:

1. Discover size and Range support (`HEAD` or a probe `GET` with `Range`).
2. Split the object into fixed-size chunks.
3. Prefetch upcoming chunks with bounded parallel `Range` GETs.
4. Reassemble in order so `Read` sees a normal contiguous stream.
5. Fall back to a single streaming GET when Range is unavailable.

Callers should not need to know about chunks, workers, or buffers unless
they opt into tuning.

**Why `io.Reader` first (not `ReaderAt`):** it drops into `io.Copy`,
`tar`/`gzip` pipelines, hashers, and caches without a custom protocol.
`ReaderAt` / `Seeker` change prefetch policy (locality vs sequential
readahead) and are tracked as later adaptations.

## Architecture

```
                 ┌─────────────┐
  Open(url) ───► │ Coordinator │  size, etag, range support
                 └──────┬──────┘
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
     Worker pool   Ordered buffer   Read cursor
     (Range GETs)  (chunk slots)    (blocks Read)
```

| Piece | Responsibility |
| --- | --- |
| **Prober** | `HEAD` / ranged probe; Content-Length, Accept-Ranges, ETag/Last-Modified |
| **Planner** | Chunk boundaries; respect end offset / partial downloads |
| **Fetcher** | Bounded workers; retries; cancel on `Close` / parent ctx |
| **Reorder buffer** | Hold completed chunks until the read cursor needs them; bound memory |
| **Reader** | `Read` drains the current chunk; advances cursor; wakes planner |
| **Fallback** | Plain `GET` body when Range unsupported or size unknown |

### Concurrency and memory

- Cap inflight requests with `Concurrency`.
- Cap buffered unread bytes with `PrefetchWindow * ChunkSize` (and/or
  `BufferMemoryLimit`).
- When the buffer is full, stop issuing new Range requests until `Read`
  consumes data.
- `Close` cancels inflight work and drops buffers promptly.

### Errors

- Transient HTTP/network errors: retry with backoff (per chunk).
- Permanent 4xx (except 416 handled carefully): fail the Reader.
- Surface the first fatal error on the next `Read` (standard Go stream
  pattern); optional `Err()` for early inspection.
- Context cancel / `Close`: stop cleanly; subsequent `Read` returns
  `ctx.Err()` or `fs.ErrClosed`.

### Correctness invariants

1. Bytes delivered by `Read` match a single contiguous GET of `[0, Size)`.
2. No chunk is delivered out of order to the caller.
3. Cancelation always terminates workers (no goroutine leaks).
4. If the server ignores `Range` and returns `200` with full body, detect
   and either consume that response as the sole source or fail fast —
   never stitch mismatched bodies.

## Public API (sketch)

Package: `github.com/yolocs/go-downloader`.

```go
func Open(ctx context.Context, url string, opts ...Option) (Reader, error)

type Reader interface {
    io.ReadCloser
    Size() int64 // Content-Length when known, else -1
}

func WithChunkSize(n int64) Option
func WithConcurrency(n int) Option
func WithPrefetchWindow(n int) Option
func WithParallelThreshold(n int64) Option
func WithHTTPClient(c *http.Client) Option
func WithHeader(key, value string) Option
func WithRetry(policy RetryPolicy) Option
func WithBufferMemoryLimit(n int64) Option
func WithProgress(fn func(Progress)) Option
```

### Suggested defaults

| Knob | Default | Rationale |
| --- | --- | --- |
| Chunk size | 2 MiB | Balance request overhead vs parallelism |
| Concurrency | 4 | Safe for most origins; user can raise |
| Prefetch window | 4 chunks | ~8 MiB readahead |
| Retry | 3× exponential | Typical transient blips |
| Parallel threshold | 8 MiB | Below this, single GET |

Defaults stay boring and safe; throughput tuning is opt-in.

## Use cases

**Direct fits:** large artifact download; ML / model weights; OCI layer
pull; package or module mirrors; object-storage restore into
`tar`/`zstd`; thin CLI wrapper; CI cache hydration.

**Light adaptations:** streaming decompress/extract over the Reader;
integrity check (stream or per-chunk hash); resume; partial
`WithRange(start, end)`; progress/metrics; auth via custom `http.Client`.

**Deeper adaptations:** `io.ReaderAt` / `io.Seeker`; HTTP(S) FUSE;
multi-mirror fan-in; adaptive concurrency; disk-backed readahead;
transport-agnostic `RangeFetcher` (S3/GCS/Azure); proxy/cache filler;
multi-source / P2P-adjacent designs.

## Non-goals (v1)

- Full aria2 feature parity (DHT, metalink, torrent).
- Automatic mirror discovery.
- Guaranteed maximum throughput on every network (knobs yes; magic
  autotune later).
- Replacing cloud SDK multipart download APIs in v1 — stay HTTP-Range
  generic first; optional fetchers later.

## Open questions

| Topic | Notes |
| --- | --- |
| Servers that advertise Range but behave badly | Need detection + fallback |
| Unknown size (`chunked` without length) | Fall back to single stream |
| Small files | Bypass parallel path below a threshold |
| CPU vs network | Large concurrency can hurt on localhost / saturated NICs |
| Memory pressure | Hard limits essential when consumer is slow |
| HEAD support | Some CDNs dislike HEAD; prefer ranged probe |
| ETag changes mid-download | Detect mismatch; fail rather than corrupt |
| Eager vs lazy prefetch start | Eager hides latency; lazy avoids work if unused |
| API name | `Open` vs `NewReader` vs `Get` — lean `Open` / `NewReader` |

## Success metrics

- Correctness: bit-identical to a single-stream download on fuzz/corpus.
- UX: one-liner `Open` + `io.Copy` for the common case.
- Speed: clear win over single connection on high-RTT links in benchmarks.
- Safety: no leaks under cancel; bounded memory under slow consumers.
- API stability: options pattern grows without breaking `io.Reader`.

## Development

Requires **Go 1.27+**.

```bash
go test ./...
go test -bench=. -benchmem ./...
```

Feature tracking: [TRACKING.md](TRACKING.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
