package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ErrDigestMismatch is returned by SaveFile when the streamed SHA-256 does
// not match the expected digest. The partial temporary file is removed.
var ErrDigestMismatch = errors.New("downloader: SHA-256 digest mismatch")

// ErrChunkDigestMismatch is returned when a parallel Range chunk's SHA-256
// does not match the digest supplied via WithChunkChecksums.
var ErrChunkDigestMismatch = errors.New("downloader: chunk SHA-256 digest mismatch")

// ParseSHA256Hex decodes a 64-character hexadecimal SHA-256 digest.
func ParseSHA256Hex(s string) ([]byte, error) {
	if len(s) != sha256.Size*2 {
		return nil, fmt.Errorf("downloader: SHA-256 hex must be %d characters", sha256.Size*2)
	}
	out, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("downloader: invalid SHA-256 hex: %w", err)
	}
	return out, nil
}

// SaveFile downloads url to path atomically. Bytes stream through SHA-256
// while writing a temporary file alongside path; the file is renamed into
// place only when the digest matches expectedSHA256 (exactly 32 bytes).
//
// On any error — including digest mismatch, context cancelation, or a
// truncated stream — the temporary file is removed and path is left
// unchanged. Open options (concurrency, retries, and so on) apply as usual.
func SaveFile(ctx context.Context, url, path string, expectedSHA256 []byte, opts ...Option) error {
	if ctx == nil {
		return fmt.Errorf("downloader: nil context")
	}
	if url == "" {
		return fmt.Errorf("downloader: empty url")
	}
	if path == "" {
		return fmt.Errorf("downloader: empty path")
	}
	if len(expectedSHA256) != sha256.Size {
		return fmt.Errorf("downloader: expected SHA-256 must be %d bytes", sha256.Size)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("downloader: mkdir %q: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, ".downloader-*")
	if err != nil {
		return fmt.Errorf("downloader: create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	r, err := Open(ctx, url, opts...)
	if err != nil {
		_ = tmp.Close()
		return err
	}

	h := sha256.New()
	written, err := copyWithContext(ctx, io.MultiWriter(tmp, h), r)
	closeErr := r.Close()
	if err != nil {
		_ = tmp.Close()
		return err
	}
	if closeErr != nil {
		_ = tmp.Close()
		return closeErr
	}

	got := h.Sum(nil)
	if !digestEqual(got, expectedSHA256) {
		_ = tmp.Close()
		return fmt.Errorf("%w: got %s want %s", ErrDigestMismatch, hex.EncodeToString(got), hex.EncodeToString(expectedSHA256))
	}

	if size := r.Size(); size >= 0 && written != size {
		_ = tmp.Close()
		return fmt.Errorf("downloader: wrote %d bytes, expected %d", written, size)
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("downloader: sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("downloader: close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("downloader: rename into place: %w", err)
	}
	cleanup = false
	return nil
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			n += int64(nw)
			if ew != nil {
				return n, ew
			}
			if nw != nr {
				return n, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				break
			}
			return n, er
		}
	}
	return n, nil
}

// ChunkChecksumsFromPayload returns SHA-256 digests for each fixed-size
// chunk of payload, indexed the same way planChunks numbers Range slices.
func ChunkChecksumsFromPayload(payload []byte, chunkSize int64) [][]byte {
	ranges := planChunks(int64(len(payload)), chunkSize)
	out := make([][]byte, len(ranges))
	for _, br := range ranges {
		sum := sha256.Sum256(payload[br.Start:br.End])
		d := make([]byte, sha256.Size)
		copy(d, sum[:])
		out[br.Index] = d
	}
	return out
}

func verifyChunkDigest(index int, data, expected []byte) error {
	if len(expected) != sha256.Size {
		return fmt.Errorf("downloader: chunk %d expected digest is %d bytes, want %d", index, len(expected), sha256.Size)
	}
	got := sha256.Sum256(data)
	if !digestEqual(got[:], expected) {
		return fmt.Errorf("%w: chunk %d got %s want %s", ErrChunkDigestMismatch, index, hex.EncodeToString(got[:]), hex.EncodeToString(expected))
	}
	return nil
}

func validateChunkChecksums(digests [][]byte, numChunks int) error {
	if len(digests) == 0 {
		return nil
	}
	if len(digests) != numChunks {
		return fmt.Errorf("downloader: chunk checksum count %d != planned chunk count %d", len(digests), numChunks)
	}
	var any bool
	for i, d := range digests {
		if d == nil {
			continue
		}
		any = true
		if len(d) != sha256.Size {
			return fmt.Errorf("downloader: chunk %d checksum must be %d bytes", i, sha256.Size)
		}
	}
	if !any {
		return fmt.Errorf("downloader: chunk checksums has no non-nil digests")
	}
	return nil
}

func digestEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := range a {
		v |= a[i] ^ b[i]
	}
	return v == 0
}
