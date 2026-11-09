// Command decompress-zstd downloads a zstd-compressed object in parallel and
// streams the decompressed bytes to stdout (or -out).
//
// Usage:
//
//	go run . -url https://example.com/archive.zst
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"

	"github.com/klauspost/compress/zstd"
	downloader "github.com/yolocs/go-downloader"
)

func main() {
	url := flag.String("url", "", "HTTP(S) URL of a zstd-compressed object")
	outPath := flag.String("out", "", "write decompressed data to this file (default stdout)")
	flag.Parse()

	if *url == "" {
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	r, err := downloader.Open(ctx, *url)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer r.Close()

	zr, err := zstd.NewReader(r)
	if err != nil {
		log.Fatalf("zstd: %v", err)
	}
	defer zr.Close()

	var dst io.Writer = os.Stdout
	if *outPath != "" {
		f, err := os.Create(*outPath)
		if err != nil {
			log.Fatalf("create output: %v", err)
		}
		defer f.Close()
		dst = f
	}

	if _, err := io.Copy(dst, zr); err != nil {
		log.Fatalf("copy: %v", err)
	}
}
