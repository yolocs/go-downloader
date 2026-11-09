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
- Cap admitted chunks with `PrefetchWindow`, including active requests,
  completed chunks waiting in order, and the partially consumed current chunk.
- Cap chunk payload storage with `BufferMemoryLimit`; zero derives the budget
  from `PrefetchWindow * ChunkSize` (8 MiB with the defaults).
- When the buffer is full, stop issuing new Range requests until `Read`
  fully consumes a chunk. A partial read retains that chunk's full reservation
  because its backing allocation is still live.
- `Close` cancels inflight work and drops buffers promptly.

```go
r, err := downloader.Open(ctx, url,
    downloader.WithChunkSize(2<<20),
    downloader.WithPrefetchWindow(8),
    downloader.WithBufferMemoryLimit(6<<20), // at most three full chunks
)
```

Both caps apply together, so a smaller memory budget can reduce effective
concurrency. The final short chunk reserves only its actual length. Each chunk
uses an exact-size payload buffer, reused across retries. Negative limits,
positive limits below `ChunkSize`, and an overflowing derived budget are rejected
before any request; `PrefetchWindow` must be positive.

The memory budget bounds chunk payload buffers, not total process memory: it
excludes HTTP/transport and Go runtime overhead, caller-owned buffers, and
per-object chunk metadata. Probe and single-stream fallback requests retain
their streaming behavior and do not use the parallel buffer budget.

### Errors

- Transient HTTP/network errors: retry with backoff (per chunk).
- Permanent 4xx (except 416 handled carefully): fail the Reader.
- Object mutation mid-download (ETag/size/`412`): `ErrObjectChanged`.
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

var ErrObjectChanged = errors.New("downloader: remote object changed during download")

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

func SaveFile(ctx context.Context, url, path string, expectedSHA256 []byte, opts ...Option) error
func ParseSHA256Hex(s string) ([]byte, error)

var ErrDigestMismatch = errors.New("downloader: SHA-256 digest mismatch")
```

### Suggested defaults

| Knob | Default | Rationale |
| --- | --- | --- |
| Chunk size | 2 MiB | Balance request overhead vs parallelism |
| Concurrency | 4 | Safe for most origins; user can raise |
| Prefetch window | 4 chunks | ~8 MiB readahead |
| Buffer memory limit | Derived from chunk size × prefetch window | 8 MiB of chunk payload storage |
| Retry | 3 total attempts | Exponential backoff from 100 ms, capped at 2 s |
| Attempt timeout | 30 s | Includes headers and body for each Range request |
| Parallel threshold | 8 MiB | Below this, single GET |

Defaults stay boring and safe; throughput tuning is opt-in.

### Progress

`WithProgress` reports how far the download has advanced:

```go
r, err := downloader.Open(ctx, url,
    downloader.WithProgress(func(p downloader.Progress) {
        fmt.Printf("%d / %d read (%d fetched)\n", p.BytesRead, p.Total, p.BytesFetched)
    }),
)
```

`BytesRead` counts bytes delivered through `Read`. `BytesFetched` counts
bytes that have landed in the reorder buffer and may lead `BytesRead` on
the parallel path; on the single-stream fallback the two stay equal.
`Total` is Content-Length when known, otherwise `-1`.

Callbacks are serialized per reader and run under the reader's lock, so
they must stay non-blocking and must not call methods on that reader.

### Chunk retries and timeouts

Parallel chunks retry HTTP 408, 429, 500, 502, 503, and 504 responses,
transient network failures, truncated bodies, and attempt timeouts. Each
attempt restarts the same range; partial bytes from a failed attempt are
discarded. Other statuses (including `200` on Range, `412`, and `416`)
fail immediately. An exhausted or permanent failure cancels other workers
and is returned by `Read`, with underlying errors preserved for
`errors.Is` and `errors.As`.

```go
r, err := downloader.Open(ctx, url,
    downloader.WithRetry(downloader.RetryPolicy{
        MaxAttempts:    5, // includes the initial attempt; 1 disables retries
        InitialBackoff: 200 * time.Millisecond,
        MaxBackoff:     3 * time.Second,
        AttemptTimeout: 20 * time.Second,
    }),
)
```

Zero-valued policy fields use the defaults above. Negative values and a
maximum backoff below the initial backoff are rejected before any request.
Parent context cancellation and `Close` interrupt requests and retry waits.
Retries keep their worker slot, so concurrency and prefetch limits still
apply. A custom `http.Client.Timeout` may impose a shorter timeout.

This policy applies to parallel Range requests; probe and single-stream
fallback GETs retain their existing behavior. Backoff is exponential without
jitter and does not interpret `Retry-After` headers.

### Range response safety

The `bytes=0-0` probe uses response headers without draining the body, so
an ignored Range or error response cannot stall fallback while streaming
its body. A `200` selects a fresh single-stream GET. A `416` is accepted
only with `Content-Range: bytes */0`, which identifies an empty object and
uses the single-stream path; other probe `416` responses fail `Open`.

Every `206` chunk must declare the exact requested start/end offsets and
the probed total size in `Content-Range`, as well as supply the expected
number of bytes. Missing, malformed, or mismatched range headers fail
without retrying or consuming the response body. Once parallel reading
starts, a `200` or `416` also fails the reader and cancels sibling requests.
The stream never restarts or appends those response bodies, even after a
valid prefix has been delivered. Callers must treat a read error as an
incomplete download and discard any partial output.

### Object consistency

The probe captures `ETag` and `Last-Modified` when present. Parallel Range
requests and the single-stream fallback pin that representation with
`If-Match` (preferred) or `If-Unmodified-Since`. A `412` response surfaces
as `ErrObjectChanged`.

When a `206` (or fallback `200`) also returns an ETag, it must match the
probed value; a differing ETag is `ErrObjectChanged`. Origins that omit
ETag on Range responses are accepted if validators still hold. A
`Content-Range` total that disagrees with the probed size, or a fallback
`Content-Length` that disagrees with the probed size, is likewise
`ErrObjectChanged`. Callers should discard partial output and restart.

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

### Examples

Runnable programs under [examples/](examples/) show streaming
decompress and extract on top of the parallel `Reader`:

| Example | Pipeline |
| --- | --- |
| [decompress-gzip](examples/decompress-gzip/) | download → `gzip.Reader` → file or stdout |
| [extract-targz](examples/extract-targz/) | download → gzip → `tar.Reader` → directory |
| [decompress-zstd](examples/decompress-zstd/) | download → `zstd.Reader` → file or stdout |

### Integrity (atomic file save)

`SaveFile` downloads to a sibling temporary file, hashes the stream with
SHA-256 while writing, and renames into place only when the digest matches.
A mismatch or other failure removes the temporary file and leaves the
destination path unchanged.

```go
expected, err := downloader.ParseSHA256Hex("a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3")
if err != nil {
    return err
}
err = downloader.SaveFile(ctx, url, "/var/data/artifact.bin", expected,
    downloader.WithConcurrency(8),
)
if errors.Is(err, downloader.ErrDigestMismatch) {
    // discard and retry or fetch a new digest
}
```

Pass the same `Option` values as `Open` (client, concurrency, retries, and
so on). `expected` must be exactly 32 bytes; use `ParseSHA256Hex` for
hex-encoded release checksums.

Feature tracking: [TRACKING.md](TRACKING.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
