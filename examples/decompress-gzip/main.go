// Command decompress-gzip downloads a gzip-compressed object in parallel and
// streams the decompressed bytes to stdout (or -out).
//
// Usage:
//
//	go run . -url https://example.com/archive.gz
//	go run . -url https://example.com/archive.gz -out ./data.bin
package main

import (
	"compress/gzip"
	"context"
	"flag"
	"io"
	"log"
	"os"

	downloader "github.com/yolocs/go-downloader"
)

func main() {
	url := flag.String("url", "", "HTTP(S) URL of a gzip-compressed object")
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

	gr, err := gzip.NewReader(r)
	if err != nil {
		log.Fatalf("gzip: %v", err)
	}
	defer gr.Close()

	var dst io.Writer = os.Stdout
	if *outPath != "" {
		f, err := os.Create(*outPath)
		if err != nil {
			log.Fatalf("create output: %v", err)
		}
		defer f.Close()
		dst = f
	}

	if _, err := io.Copy(dst, gr); err != nil {
		log.Fatalf("copy: %v", err)
	}
}
