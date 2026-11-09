# Tracking

Implementation backlog for `go-downloader`, ordered from light (near-term)
to deep (larger adaptations).

**How to update:** when a item ships, ~~strike it through~~ (keep the
line; do not delete). Prefer `- [x] ~~…~~` so history stays visible.
Add new items at the end of the matching section as ideas appear.

---

## Bootstrap

- [x] ~~Go module on Go 1.27~~
- [x] ~~README with idea + architecture~~
- [x] ~~Package skeleton: `Open`, options, stub types, doc comments~~
- [x] ~~Example placeholder and validation tests~~
- [x] ~~CI: `go test`, `go vet`, `gofmt`~~
- [x] ~~This tracking file~~

## MVP — sequential parallel Range reader

- [x] ~~Probe size / Range support (`HEAD` or ranged probe)~~
- [x] ~~Chunk planner (fixed-size ranges over `[0, Size)`)~~
- [x] ~~Bounded worker pool for parallel `Range` GETs~~
- [x] ~~Ordered reorder buffer feeding `Read`~~
- [x] ~~`Close` + parent context cancelation (no goroutine leaks)~~
- [x] ~~Fallback to single streaming GET when Range unsupported / size unknown~~
- [x] ~~Happy-path tests against Range-capable `httptest` server~~
- [x] ~~Benchmarks vs naive `http.Get` (synthetic latency)~~

## Production hardening

- [x] ~~Per-chunk retries with backoff / timeouts~~
- [x] ~~Handle `416` and `200`-on-`Range` safely (no mismatched stitch)~~
- [x] ~~Memory / prefetch caps (`PrefetchWindow`, `BufferMemoryLimit`)~~
- [x] ~~Progress hook (`WithProgress`)~~
- [x] ~~Small-file fast path (below parallel threshold → single GET)~~
- [x] ~~ETag / size consistency checks mid-download~~
- [x] ~~Bit-identical correctness tests vs single-stream download~~

## Light adaptations

- [ ] Streaming decompress / extract examples (`gzip` / `zstd` / `tar`)
- [ ] Integrity: final stream hash (e.g. SHA-256) before rename-into-place
- [ ] Integrity: optional per-chunk checksums
- [ ] Resumable downloads (`WithResume` / persisted chunk map)
- [ ] Partial object read (`WithRange(start, end)`)
- [ ] Progress / metrics exporters (bytes/sec, chunk latency)
- [ ] Auth / custom transport docs (`WithHTTPClient`, headers)
- [ ] Thin CLI wrapper with progress

## Richer I/O contracts

- [ ] Optional `io.ReaderAt` (locality-based prefetch around reads)
- [ ] Optional `io.Seeker` (cache or re-fetch on backward seek)
- [ ] Document seek / random-access prefetch policy vs sequential mode

## Performance and ops

- [ ] Adaptive concurrency (AIMD / RTT–bandwidth inspired)
- [ ] Detailed benchmarks (latency × bandwidth matrix)
- [ ] Metrics hooks (`expvar` and/or OpenTelemetry, optional)
- [ ] Disk-backed readahead spill when memory limit is hit
- [ ] Tune / revisit default chunk size, concurrency, prefetch window

## Deep adaptations / ecosystem

- [ ] `RangeFetcher` interface (transport-agnostic engine)
- [ ] S3 / GCS / Azure range fetch adapters
- [ ] Multi-mirror fan-in (first valid chunk wins; needs checksums)
- [ ] HTTP(S) FUSE / virtual file sketch (`ReaderAt` + page cache)
- [ ] Proxy / cache-filler example (parallel origin pull → sequential clients)
- [ ] Examples: OCI layer pull sketch, model/weight fetch
- [ ] Godoc polish and `v0` compatibility policy
- [ ] Multi-source / P2P-adjacent exploration (out of initial scope)

## Explicit non-goals (for now)

Keep visible so we do not accidentally scope-creep; move to an active
section only if we deliberately take them on.

- [ ] Full aria2 feature parity (DHT, metalink, torrent)
- [ ] Automatic mirror discovery
- [ ] Magic autotune that guarantees max throughput everywhere
- [ ] Replace cloud SDK multipart download APIs wholesale
