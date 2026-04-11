package cp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestRunPoolExecutesAllJobs(t *testing.T) {
	t.Parallel()
	jobs := []string{"a", "b", "c", "d", "e"}
	var count atomic.Int64
	err := runPool(context.Background(), 3, jobs, func(_ context.Context, _ string) error {
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != 5 {
		t.Errorf("expected 5 executions, got %d", count.Load())
	}
}

func TestRunPoolCancelsOnError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("boom")
	jobs := make([]string, 20)
	for i := range jobs {
		jobs[i] = "x"
	}
	var count atomic.Int64
	err := runPool(context.Background(), 2, jobs, func(ctx context.Context, _ string) error {
		count.Add(1)
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	// With 2 workers all returning errors immediately, cancellation should kick in quickly.
	// Allow at most parallelism*2 to handle timing, but certainly not all 20.
	const maxExpected = 4 // 2 workers, at most 2 rounds before cancel propagates
	if count.Load() > maxExpected {
		t.Errorf("expected cancellation after ~%d jobs, got %d", maxExpected, count.Load())
	}
}

func TestRunPoolRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	jobs := []string{"a", "b", "c"}
	var count atomic.Int64
	_ = runPool(ctx, 2, jobs, func(_ context.Context, _ string) error {
		count.Add(1)
		return nil
	})
	// Pre-cancelled context: due to Go scheduler non-determinism, up to
	// parallelism jobs may start before cancellation is observed.
	const maxAllowed = 2 // number of workers; cancellation should prevent more
	if count.Load() > maxAllowed {
		t.Errorf("expected at most %d jobs with pre-cancelled context, got %d", maxAllowed, count.Load())
	}
}
