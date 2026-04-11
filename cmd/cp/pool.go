package cp

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// runPool runs fn on each job in jobs using at most parallelism concurrent workers.
// The first error returned by fn cancels the context passed to all other workers.
func runPool[T any](ctx context.Context, parallelism int, jobs []T, fn func(context.Context, T) error) error {
	g, gctx := errgroup.WithContext(ctx)
	ch := make(chan T)

	// Feed jobs into channel; stop feeding if context is cancelled.
	g.Go(func() error {
		defer close(ch)
		for _, job := range jobs {
			select {
			case ch <- job:
			case <-gctx.Done():
				return nil
			}
		}
		return nil
	})

	for range parallelism {
		g.Go(func() error {
			for job := range ch {
				if err := fn(gctx, job); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return g.Wait()
}
