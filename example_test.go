package downloader_test

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"io"

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

func ExampleOpen_gzip() {
	ctx := context.Background()
	r, err := downloader.Open(ctx, "https://example.com/object.gz")
	if err != nil {
		return
	}
	defer r.Close()

	gr, err := gzip.NewReader(r)
	if err != nil {
		return
	}
	defer gr.Close()
	_, _ = io.Copy(io.Discard, gr)
}

func ExampleOpen_tarGz() {
	ctx := context.Background()
	r, err := downloader.Open(ctx, "https://example.com/bundle.tar.gz")
	if err != nil {
		return
	}
	defer r.Close()

	gr, err := gzip.NewReader(r)
	if err != nil {
		return
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
		if hdr.Typeflag == tar.TypeReg {
			_, _ = io.Copy(io.Discard, tr)
		}
	}
}
