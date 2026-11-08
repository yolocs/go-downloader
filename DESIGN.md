# Design: Parallel Range Prefetch Downloader

## Problem

Single-connection HTTP streaming leaves bandwidth on the table whenever
latency is high or the server rate-limits per connection. Tools like
`aria2` and `axel` solve this for CLI downloads; we want the same idea as
a small, composable Go library whose primary surface is `io.Reader`.

## Core idea

Expose a sequential byte stream (`io.Reader` / `io.ReadCloser`) while,
under the hood:

1. Discover size and Range support (`HEAD` or a probe `GET` with `Range`).
2. Split the object into fixed-size chunks.
3. Prefetch upcoming chunks with bounded parallel `Range` GETs.
4. Reassemble in order so `Read` sees a normal contiguous stream.
5. Fall back to a single streaming GET when Range is unavailable.

Callers should not need to know about chunks, workers, or buffers unless
they opt into tuning.

## Proposed public API (sketch)

Package path: `github.com/yolocs/go-downloader`.

```go
// Open returns an io.ReadCloser that yields the resource body.
// Prefetching starts immediately (or lazily on first Read — TBD).
func Open(ctx context.Context, url string, opts ...Option) (io.ReadCloser, error)

// Optional richer handle for metadata / future capabilities.
type Reader interface {
    io.ReadCloser
    // Size returns Content-Length when known, or -1.
    Size() int64
}

type Option func(*Config)

func WithChunkSize(n int64) Option          // default ~1–4 MiB
func WithConcurrency(n int) Option         // default runtime.GOMAXPROCS or 4–8
func WithPrefetchWindow(n int) Option      // chunks kept ahead of read cursor
func WithHTTPClient(c *http.Client) Option
func WithHeader(key, value string) Option
func WithRetry(policy RetryPolicy) Option
func WithBufferMemoryLimit(n int64) Option // soft cap on in-memory readahead
func WithProgress(fn func(Progress)) Option
```

Usage stays boring on purpose:

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

### Why `io.Reader` first (not `ReaderAt`)

- Drop-in for `io.Copy`, `tar.NewReader`, `gzip.NewReader`, archive
  extractors, hashers, and most pipeline code.
- Forces a clear sequential contract that matches “download this file.”
- `io.ReaderAt` / `io.Seeker` are valuable adaptations, but they change
  prefetch policy (demand-driven vs sequential readahead). Ship Reader
  first; layer random-access later.

## Internal architecture

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

### Components

| Piece | Responsibility |
| --- | --- |
| **Prober** | `HEAD` / ranged probe; Content-Length, Accept-Ranges, ETag/Last-Modified |
| **Planner** | Chunk boundaries; respect end offset / partial downloads |
| **Fetcher** | Bounded workers; retries; cancel on `Close` / parent ctx |
| **Reorder buffer** | Hold completed chunks until the read cursor needs them; bound memory |
| **Reader** | `Read` drains the current chunk; advances cursor; wakes planner |
| **Fallback** | Plain `GET` body when Range unsupported or size unknown |

### Concurrency & memory model

- Cap inflight requests with `Concurrency`.
- Cap buffered unread bytes with `PrefetchWindow * ChunkSize` (and/or
  `BufferMemoryLimit`).
- When the buffer is full, stop issuing new Range requests until `Read`
  consumes data.
- `Close` cancels inflight work and drops buffers promptly.

### Error handling

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

## Use cases

### Direct fits (little or no adaptation)

1. **Large artifact download** — release binaries, disk images, datasets;
   replace `http.Get` + `io.Copy` for high-RTT or multi-Gbps paths.
2. **ML / model weight fetch** — Hugging Face, GCS/S3 signed URLs; stream
   into local cache while hashing.
3. **Container / OCI layer pull** — registry blob download; compose under
   existing layer unpack pipelines.
4. **Package / module mirrors** — accelerate fat tarballs in internal
   caches or CI.
5. **Backup restore from object storage** — streaming restore into
   `tar`/`zstd` without waiting for the full object.
6. **CLI download tool** — thin wrapper with progress (aria2-like UX,
   library-first design).
7. **CI cache hydration** — pull large caches in parallel at job start.

### Fits with light adaptation

8. **Streaming decompress / extract** — `gzip`/`zstd`/`tar` over the
   Reader; needs steady sequential supply (default mode).
9. **Integrity-checked download** — optional per-chunk checksums or a
   final SHA-256 of the stream; fail before rename-into-place.
10. **Resumable downloads** — persist chunk completion map; reopen with
    `WithResume(state)` after interrupt.
11. **Partial object read** — `WithRange(start, end)` for “bytes 100–200MB
    only” (log segments, truncated previews).
12. **Progress / metrics exporters** — bytes/sec, chunk latency histograms
    for ops dashboards.
13. **Auth / custom transport** — inject `http.Client` for OAuth, mTLS,
    corporate proxies, retry transports.

### Fits with deeper adaptation

14. **`io.ReaderAt` for random access** — ZIP central directories, Parquet
    footers, PDF; prefetch becomes locality-based (window around reads)
    instead of pure sequential readahead.
15. **`io.Seeker` for format parsers** — media remuxers, some archive
    tools; seeking backward may require cache or re-fetch.
16. **HTTP(S) FUSE / virtual file** — expose remote object as a file;
    combine ReaderAt + page cache.
17. **Multi-mirror fan-in** — same byte ranges from multiple URLs/CDNs;
    first valid response wins per chunk (needs checksums).
18. **Adaptive concurrency** — AIMD or bandwidth/RTT controller like
    BBR-inspired tuning; raise/lower workers at runtime.
19. **Disk-backed readahead** — spill completed chunks to temp files when
    memory limit is hit (huge objects, slow consumers).
20. **Non-HTTP range sources** — S3 GetObject Range, GCS, Azure Blob,
    or custom `RangeFetcher` interface so the reorder/prefetch engine is
    transport-agnostic.
21. **Proxy / cache filler** — edge node pulls from origin with parallel
    ranges, serves sequential clients.
22. **P2P / swarm adjacent** — chunk planner + integrity could back a
    simple multi-source downloader (out of initial scope).

## Non-goals (v1)

- Full aria2 feature parity (DHT, metalink, torrent).
- Automatic mirror discovery.
- Guaranteed maximum throughput on every network (tuning knobs yes;
  magic autotune later).
- Replacing specialized cloud SDKs’ multipart download APIs in v1
  (instead: stay HTTP-Range generic; optional fetchers later).

## Risks & open questions

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

## Implementation plan

### Phase 0 — Bootstrap (this PR)

- Go module on **Go 1.27**.
- Design doc (this file) + README orientation.
- Package skeleton: `Open`, options, stub types, doc comments.
- Example placeholder and test harness shape.
- CI: `go test`, `go vet`, golangci-lite or just `gofmt` check.

### Phase 1 — MVP sequential Reader

- Probe + chunk plan + worker pool + ordered buffer.
- Happy path against a Range-capable test server (`net/http/httptest`).
- Fallback to single GET.
- `Close` / context cancelation.
- Benchmarks vs naive `http.Get` (synthetic latency).

### Phase 2 — Production hardening

- Retries, timeouts, 416/200-on-Range handling.
- Memory / prefetch caps.
- Progress hook.
- Small-file fast path.
- ETag / size consistency checks.

### Phase 3 — Richer I/O contracts

- Optional `ReaderAt` (and maybe `Seeker`).
- `WithRange(start, end)` partial downloads.
- Resume support.

### Phase 4 — Performance & ops

- Adaptive concurrency experiments.
- Detailed benchmarks (latency × BW matrix).
- Metrics hooks / `expvar` or OpenTelemetry optional.
- Disk-spill buffer if needed.

### Phase 5 — Ecosystem

- `RangeFetcher` interface for S3/GCS.
- Examples: CLI, OCI layer pull sketch, model fetch.
- Godoc polish and compatibility policy (`v0` until stable).

## Success metrics

- Correctness: bit-identical to a single-stream download on fuzz/corpus.
- UX: one-liner `Open` + `io.Copy` for the common case.
- Speed: clear win over single connection on high-RTT links in benchmarks.
- Safety: no leaks under cancel; bounded memory under slow consumers.
- API stability: options pattern leaves room to grow without breaking
  the `io.Reader` contract.

## Suggested defaults (starting point)

| Knob | Default | Rationale |
| --- | --- | --- |
| Chunk size | 2 MiB | Balance request overhead vs parallelism |
| Concurrency | 4 | Safe for most origins; user can raise |
| Prefetch window | 4 chunks | ~8 MiB readahead |
| Retry | 3× exponential | Typical transient blips |
| Parallel threshold | 8 MiB | Below this, single GET |

Defaults should be boring and safe; throughput tuning is opt-in.
