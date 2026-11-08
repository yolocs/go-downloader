package downloader

// byteRange is a half-open byte interval [Start, End) within the object.
type byteRange struct {
	Index int
	Start int64
	End   int64 // exclusive
}

// planChunks splits [0, size) into fixed-size ranges of at most chunkSize.
func planChunks(size, chunkSize int64) []byteRange {
	if size <= 0 || chunkSize <= 0 {
		return nil
	}
	n := (size + chunkSize - 1) / chunkSize
	out := make([]byteRange, 0, n)
	for start := int64(0); start < size; start += chunkSize {
		end := start + chunkSize
		if end > size {
			end = size
		}
		out = append(out, byteRange{
			Index: len(out),
			Start: start,
			End:   end,
		})
	}
	return out
}
