package downloader_test

import (
	"context"
	"fmt"

	downloader "github.com/yolocs/go-downloader"
)

func ExampleOpen() {
	ctx := context.Background()
	r, err := downloader.Open(ctx, "https://example.com/large.bin",
		downloader.WithConcurrency(8),
		downloader.WithChunkSize(2<<20),
	)
	if err != nil {
		// Phase 0: Open is intentionally unimplemented.
		fmt.Println("planned:", err != nil)
		return
	}
	defer r.Close()
	_ = r
	// Output:
	// planned: true
}
