package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"
)

func normalizeRetryPolicy(p *RetryPolicy) error {
	if p.MaxAttempts < 0 || p.InitialBackoff < 0 || p.MaxBackoff < 0 || p.AttemptTimeout < 0 {
		return fmt.Errorf("downloader: retry attempts and durations must not be negative")
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 3
	}
	if p.InitialBackoff == 0 {
		p.InitialBackoff = 100 * time.Millisecond
	}
	if p.MaxBackoff == 0 {
		p.MaxBackoff = 2 * time.Second
	}
	if p.AttemptTimeout == 0 {
		p.AttemptTimeout = 30 * time.Second
	}
	if p.MaxBackoff < p.InitialBackoff {
		return fmt.Errorf("downloader: retry MaxBackoff must be >= InitialBackoff")
	}
	return nil
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func retryableNetworkError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && (ne.Timeout() || ne.Temporary())
}

func nextBackoff(delay, limit time.Duration) time.Duration {
	// Check before multiplying so even a near-MaxInt64 duration cannot overflow.
	if delay >= limit-delay {
		return limit
	}
	return delay * 2
}

// doRangeGet retries a chunk without relinquishing its worker slot. Each
// attempt owns a new buffer; only a complete successful result is published.
func (r *parallelReader) doRangeGet(br byteRange) ([]byte, error) {
	policy := r.cfg.Retry
	delay := policy.InitialBackoff
	for attempt := 1; ; attempt++ {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(r.ctx, policy.AttemptTimeout)
		data, retryable, err := r.doRangeGetOnce(ctx, br)
		cancel()
		if parentErr := r.ctx.Err(); parentErr != nil {
			return nil, parentErr
		}
		if err == nil {
			return data, nil
		}
		if !retryable || attempt >= policy.MaxAttempts {
			return nil, fmt.Errorf("downloader: chunk failed after %d attempt(s): %w", attempt, err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return nil, r.ctx.Err()
		case <-timer.C:
		}
		delay = nextBackoff(delay, policy.MaxBackoff)
	}
}
