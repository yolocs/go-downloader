package downloader_test

import (
	"context"

	downloader "github.com/yolocs/go-downloader"
)

func ExampleOpen() {
	ctx := context.Background()
	r, err := downloader.Open(ctx, "https://example.com/large.bin",
		downloader.WithConcurrency(8),
		downloader.WithChunkSize(2<<20),
	)
	if err != nil {
		// handle error
		return
	}
	defer r.Close()
	_ = r
	// io.Copy(dst, r)
}
