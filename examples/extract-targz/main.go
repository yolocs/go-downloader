// Command extract-targz downloads a .tar.gz object in parallel and extracts
// members into a destination directory without buffering the whole archive.
//
// Usage:
//
//	go run . -url https://example.com/bundle.tar.gz -dir ./out
package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	downloader "github.com/yolocs/go-downloader"
)

func main() {
	url := flag.String("url", "", "HTTP(S) URL of a .tar.gz archive")
	dir := flag.String("dir", ".", "directory to extract into")
	flag.Parse()

	if *url == "" {
		flag.Usage()
		os.Exit(2)
	}

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		log.Fatalf("mkdir: %v", err)
	}
	absDir, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatalf("abs dir: %v", err)
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

	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("tar next: %v", err)
		}
		if err := extractOne(absDir, hdr, tr); err != nil {
			log.Fatalf("extract %q: %v", hdr.Name, err)
		}
	}
}

func extractOne(root string, hdr *tar.Header, r io.Reader) error {
	clean := filepath.Clean(hdr.Name)
	if clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
		return fmt.Errorf("rejecting absolute or parent path %q", hdr.Name)
	}
	dest := filepath.Join(root, clean)
	if !withinRoot(root, dest) {
		return fmt.Errorf("path escapes destination: %q", hdr.Name)
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		return os.MkdirAll(dest, os.FileMode(hdr.Mode)|0o700)
	case tar.TypeReg, tar.TypeRegA:
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)|0o600)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(f, r)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	default:
		return nil
	}
}

func withinRoot(root, path string) bool {
	root = filepath.Clean(root) + string(os.PathSeparator)
	path = filepath.Clean(path)
	return strings.HasPrefix(path+string(os.PathSeparator), root) || path == filepath.Clean(root)
}
