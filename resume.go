package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ErrResumeStateInvalid is returned when a persisted chunk map does not match
// the current object probe (URL, size, validators, or chunk layout).
var ErrResumeStateInvalid = errors.New("downloader: resume state invalid")

const chunkMapVersion = 1

// ResumePaths configures on-disk resume for a parallel Range download.
// StatePath is a JSON chunk-completion map; PartialPath is the sparse output
// file written at chunk offsets.
type ResumePaths struct {
	StatePath   string
	PartialPath string
}

type persistedChunkMap struct {
	Version      int    `json:"v"`
	URL          string `json:"url"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
	ChunkSize    int64  `json:"chunk_size"`
	Done         []bool `json:"done"`
}

// WithResume enables resumable parallel downloads that persist a chunk map and
// write completed ranges to PartialPath. StatePath and PartialPath must be
// non-empty. The single-stream fallback returns an error when resume is set.
func WithResume(paths ResumePaths) Option {
	return func(c *Config) {
		c.Resume = &ResumePaths{
			StatePath:   paths.StatePath,
			PartialPath: paths.PartialPath,
		}
	}
}

func validateResume(paths *ResumePaths, numChunks int, info objectInfo, url string, chunkSize int64) ([]bool, error) {
	if paths.StatePath == "" || paths.PartialPath == "" {
		return nil, fmt.Errorf("downloader: resume requires StatePath and PartialPath")
	}

	data, err := os.ReadFile(paths.StatePath)
	if err != nil {
		if os.IsNotExist(err) {
			return newChunkDone(numChunks), nil
		}
		return nil, fmt.Errorf("downloader: read resume state: %w", err)
	}

	var saved persistedChunkMap
	if err := json.Unmarshal(data, &saved); err != nil {
		return nil, fmt.Errorf("downloader: decode resume state: %w", err)
	}
	if saved.Version != chunkMapVersion {
		return nil, fmt.Errorf("%w: unsupported resume version %d", ErrResumeStateInvalid, saved.Version)
	}
	if saved.URL != url || saved.Size != info.Size || saved.ChunkSize != chunkSize ||
		saved.ETag != info.ETag || saved.LastModified != info.LastModified {
		return nil, fmt.Errorf("%w: resume metadata does not match probe", ErrResumeStateInvalid)
	}
	if len(saved.Done) != numChunks {
		return nil, fmt.Errorf("%w: chunk count %d != planned %d", ErrResumeStateInvalid, len(saved.Done), numChunks)
	}
	return append([]bool(nil), saved.Done...), nil
}

func newChunkDone(n int) []bool {
	return make([]bool, n)
}

func (r *parallelReader) persistChunkMapLocked() error {
	if r.resumeStatePath == "" {
		return nil
	}
	m := persistedChunkMap{
		Version:      chunkMapVersion,
		URL:          r.url,
		Size:         r.size,
		ETag:         r.etag,
		LastModified: r.lastModified,
		ChunkSize:    r.cfg.ChunkSize,
		Done:         append([]bool(nil), r.chunkDone...),
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("downloader: encode resume state: %w", err)
	}
	dir := filepath.Dir(r.resumeStatePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("downloader: mkdir resume state dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".downloader-resume-*")
	if err != nil {
		return fmt.Errorf("downloader: create resume temp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("downloader: write resume state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("downloader: sync resume state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("downloader: close resume state: %w", err)
	}
	if err := os.Rename(tmpPath, r.resumeStatePath); err != nil {
		return fmt.Errorf("downloader: rename resume state: %w", err)
	}
	cleanup = false
	return nil
}

func (r *parallelReader) writePartialChunk(idx int, data []byte) error {
	if r.partial == nil {
		return nil
	}
	br := r.ranges[idx]
	if _, err := r.partial.WriteAt(data, br.Start); err != nil {
		return fmt.Errorf("downloader: write partial chunk %d: %w", idx, err)
	}
	return nil
}

func (r *parallelReader) loadPartialChunk(br byteRange) ([]byte, error) {
	data := make([]byte, br.End-br.Start)
	if r.partial == nil {
		return nil, fmt.Errorf("downloader: resume partial file not open")
	}
	n, err := r.partial.ReadAt(data, br.Start)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("downloader: read partial chunk [%d-%d): %w", br.Start, br.End, err)
	}
	if int64(n) != br.End-br.Start {
		return nil, fmt.Errorf("downloader: partial chunk [%d-%d): got %d bytes", br.Start, br.End, n)
	}
	return data, nil
}

func openResumePartial(path string, size int64) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("downloader: open partial %q: %w", path, err)
	}
	if size > 0 {
		if err := f.Truncate(size); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("downloader: truncate partial: %w", err)
		}
	}
	return f, nil
}

func removeResumeState(path string) error {
	if path == "" {
		return nil
	}
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ResumeSaveFile downloads url to path, resuming from partial bytes and a
// persisted chunk map when statePath already exists and matches the probe.
// Bytes are hashed with SHA-256 while reading; the destination is left
// unchanged on digest mismatch or other failure. On success the resume state
// file is removed.
func ResumeSaveFile(ctx context.Context, url, path, statePath string, expectedSHA256 []byte, opts ...Option) error {
	if ctx == nil {
		return fmt.Errorf("downloader: nil context")
	}
	if url == "" {
		return fmt.Errorf("downloader: empty url")
	}
	if path == "" {
		return fmt.Errorf("downloader: empty path")
	}
	if statePath == "" {
		return fmt.Errorf("downloader: empty resume state path")
	}
	if len(expectedSHA256) != sha256.Size {
		return fmt.Errorf("downloader: expected SHA-256 must be %d bytes", sha256.Size)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("downloader: mkdir %q: %w", dir, err)
	}

	opts = append(opts, WithResume(ResumePaths{
		StatePath:   statePath,
		PartialPath: path,
	}))

	r, err := Open(ctx, url, opts...)
	if err != nil {
		return err
	}

	h := sha256.New()
	written, err := copyWithContext(ctx, h, r)
	closeErr := r.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}

	got := h.Sum(nil)
	if !digestEqual(got, expectedSHA256) {
		return fmt.Errorf("%w: got %s want %s", ErrDigestMismatch, hex.EncodeToString(got), hex.EncodeToString(expectedSHA256))
	}

	if size := r.Size(); size >= 0 && written != size {
		return fmt.Errorf("downloader: read %d bytes, expected %d", written, size)
	}

	if err := removeResumeState(statePath); err != nil {
		return fmt.Errorf("downloader: remove resume state: %w", err)
	}
	return nil
}
