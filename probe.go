package downloader

import (
	"context"
	"fmt"
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
	// Inspect only headers. A server ignoring Range may return a large or
	// never-ending body; draining it would delay fallback (or an error).

	info := objectInfo{
		Size:         -1,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		start, end, size, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok || start != 0 || end != 0 {
			return objectInfo{}, fmt.Errorf("downloader: probe: missing or invalid Content-Range on 206")
		}
		info.AcceptRanges = true
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
		// bytes=0-0 is unsatisfiable for an empty object. Only an explicit
		// unsatisfied range with total zero establishes that case safely.
		start, end, size, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if ok && start == -1 && end == -1 && size == 0 {
			info.Size = 0
			return info, nil
		}
		return objectInfo{}, fmt.Errorf("downloader: probe: unexpected 416 on bytes=0-0: Content-Range %q", resp.Header.Get("Content-Range"))

	default:
		return objectInfo{}, fmt.Errorf("downloader: probe: unexpected status %s", resp.Status)
	}
}

// parseContentRange parses a single byte range with a known total length.
// Offsets are inclusive; an unsatisfied range ("bytes */N") has -1 offsets.
// Unknown totals cannot be used to plan or validate a parallel download.
func parseContentRange(h string) (start, end, total int64, ok bool) {
	unit, rest, found := strings.Cut(strings.TrimSpace(h), " ")
	if !found || !strings.EqualFold(unit, "bytes") {
		return -1, -1, -1, false
	}
	span, length, found := strings.Cut(rest, "/")
	total, valid := parseRangeNumber(length)
	if !found || !valid {
		return -1, -1, -1, false
	}
	if span == "*" {
		return -1, -1, total, true
	}
	first, last, found := strings.Cut(span, "-")
	start, startOK := parseRangeNumber(first)
	end, endOK := parseRangeNumber(last)
	if !found || !startOK || !endOK || start > end || end >= total {
		return -1, -1, -1, false
	}
	return start, end, total, true
}

func parseRangeNumber(s string) (int64, bool) {
	// The wire grammar is 1*DIGIT; ParseInt alone also accepts a sign.
	if s == "" {
		return -1, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return -1, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
}

func applyHeaders(req *http.Request, header http.Header) {
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
}

// applyObjectValidators pins the download to the probed representation.
// Prefer ETag (If-Match); fall back to Last-Modified when no ETag is present.
func applyObjectValidators(req *http.Request, etag, lastModified string) {
	if etag != "" {
		req.Header.Set("If-Match", etag)
		return
	}
	if lastModified != "" {
		req.Header.Set("If-Unmodified-Since", lastModified)
	}
}

// checkETagConsistency fails when both the probed and response ETags are
// present and disagree. A missing response ETag is tolerated — some origins
// omit it on Range responses while still honoring If-Match.
func checkETagConsistency(probed, got string) error {
	probed = strings.TrimSpace(probed)
	got = strings.TrimSpace(got)
	if probed == "" || got == "" || probed == got {
		return nil
	}
	return fmt.Errorf("%w: ETag %q != probed %q", ErrObjectChanged, got, probed)
}

// checkContentLength fails when a Content-Length header is present and does
// not match the expected byte count. An omitted header is allowed.
func checkContentLength(header string, want int64) error {
	header = strings.TrimSpace(header)
	if header == "" {
		return nil
	}
	n, err := strconv.ParseInt(header, 10, 64)
	if err != nil {
		return fmt.Errorf("bad Content-Length %q: %w", header, err)
	}
	if n != want {
		return fmt.Errorf("Content-Length %d != expected %d", n, want)
	}
	return nil
}
