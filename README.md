# go-downloader

A high-performance HTTP downloader for Go with a friendly `io.Reader` API.

Instead of single-connection streaming, it performs **parallel HTTP Range
reads** with **read-ahead prefetch**, then reassembles bytes in order so
callers just `Read` (or `io.Copy`) as usual.

> Status: early design / bootstrap. See [DESIGN.md](DESIGN.md) for the
> plan, use cases, and phased roadmap.

## Goals

- **Simple surface** — primary interface is `io.ReadCloser` (+ options).
- **Faster large downloads** — bounded parallel Range GETs hide RTT and
  use more of the pipe.
- **Safe defaults** — memory-capped prefetch, cancelation, fallback when
  Range is unsupported.
- **Composable** — drop into existing pipelines (`io.Copy`, archives,
  hashers, caches) without a custom download protocol.

## Quick taste (planned API)

```go
package main

import (
	"context"
	"io"
	"os"

	downloader "github.com/yolocs/go-downloader"
)

func main() {
	ctx := context.Background()
	r, err := downloader.Open(ctx, "https://example.com/large.bin",
		downloader.WithConcurrency(8),
		downloader.WithChunkSize(2<<20),
	)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	f, err := os.Create("large.bin")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		panic(err)
	}
}
```

## Requirements

- **Go 1.27+**

## Layout

| Path | Purpose |
| --- | --- |
| [DESIGN.md](DESIGN.md) | Architecture, use cases, open questions, phases |
| `downloader.go` | Public API sketch (`Open`, options) |
| `go.mod` | Module on Go 1.27 |

## Development

```bash
go test ./...
go test -bench=. -benchmem ./...
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
