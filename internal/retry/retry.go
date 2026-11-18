package retry

import (
	"context"
	"math/rand"
	"time"
)

type Options struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func DefaultOptions() Options {
	return Options{
		MaxRetries: 5,
		BaseDelay:  1 * time.Second,
		MaxDelay:   30 * time.Second,
	}
}

// Do retries fn with exponential backoff and jitter until success, exhaustion, or context cancellation.
func Do(ctx context.Context, opts Options, fn func() error) error {
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 5
	}
	if opts.BaseDelay <= 0 {
		opts.BaseDelay = 1 * time.Second
	}
	if opts.MaxDelay <= 0 {
		opts.MaxDelay = 30 * time.Second
	}

	delay := opts.BaseDelay
	for attempt := 1; attempt <= opts.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn()
		if err == nil {
			return nil
		}
		if attempt == opts.MaxRetries {
			return err
		}

		var jitter time.Duration
		if jitterRange := int64(delay / 5); jitterRange > 0 {
			jitter = time.Duration(rand.Int63n(jitterRange))
		}
		actualDelay := delay + jitter
		if actualDelay > opts.MaxDelay {
			actualDelay = opts.MaxDelay
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(actualDelay):
		}
		delay *= 2
	}
	return nil
}
