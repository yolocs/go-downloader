package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// objectInfo is the result of probing a remote URL for size and Range support.
type objectInfo struct {
	Size         int64 // total size in bytes, or -1 if unknown
	AcceptRanges bool
	ETag         string
	LastModified string
}

// probe discovers object size and whether the server honors byte ranges.
// Prefer a ranged GET probe over HEAD — some CDNs mishandle HEAD.
func probe(ctx context.Context, client *http.Client, rawURL string, header http.Header) (objectInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return objectInfo{}, fmt.Errorf("downloader: probe request: %w", err)
	}
	applyHeaders(req, header)
	req.Header.Set("Range", "bytes=0-0")

	resp, err := client.Do(req)
	if err != nil {
		return objectInfo{}, fmt.Errorf("downloader: probe: %w", err)
	}
	defer resp.Body.Close()
	// Drain a little so keep-alive connections can be reused; probe body is tiny.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	info := objectInfo{
		Size:         -1,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		info.AcceptRanges = true
		size, ok := parseContentRangeTotal(resp.Header.Get("Content-Range"))
		if !ok {
			return objectInfo{}, fmt.Errorf("downloader: probe: missing or invalid Content-Range on 206")
		}
		info.Size = size
		return info, nil

	case http.StatusOK:
		// Server ignored Range. Do not trust parallel Range GETs even if
		// Accept-Ranges is advertised — a 200 on bytes=0-0 is decisive.
		info.AcceptRanges = false
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			n, err := strconv.ParseInt(cl, 10, 64)
			if err != nil {
				return objectInfo{}, fmt.Errorf("downloader: probe: bad Content-Length %q: %w", cl, err)
			}
			info.Size = n
		}
		return info, nil

	case http.StatusRequestedRangeNotSatisfiable:
		return objectInfo{}, fmt.Errorf("downloader: probe: unexpected 416 on bytes=0-0")

	default:
		return objectInfo{}, fmt.Errorf("downloader: probe: unexpected status %s", resp.Status)
	}
}

// parseContentRangeTotal parses the total size from a Content-Range header.
// Examples: "bytes 0-0/12345", "bytes 0-0/*". Returns ok=false for "*".
func parseContentRangeTotal(h string) (int64, bool) {
	// RFC 9110: Content-Range = "bytes" SP range-resp
	// range-resp = incl-range "/" (complete-length / "*")
	h = strings.TrimSpace(h)
	if h == "" {
		return -1, false
	}
	if !strings.HasPrefix(strings.ToLower(h), "bytes ") {
		return -1, false
	}
	rest := strings.TrimSpace(h[len("bytes "):])
	slash := strings.LastIndex(rest, "/")
	if slash < 0 || slash+1 >= len(rest) {
		return -1, false
	}
	total := rest[slash+1:]
	if total == "*" {
		return -1, false
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil || n < 0 {
		return -1, false
	}
	return n, true
}

func applyHeaders(req *http.Request, header http.Header) {
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
}
