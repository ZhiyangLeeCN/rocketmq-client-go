package atomic

import (
	"context"
	"golang.org/x/sync/semaphore"
	"time"
)

type Semaphore struct {
	*semaphore.Weighted
}

func NewSemaphore(permits int64) *Semaphore {
	return &Semaphore{
		semaphore.NewWeighted(permits),
	}
}

func (s *Semaphore) TryAcquire(n int64, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	// Even though ctx will be expired, it is good practice to call its
	// cancelation function in any case. Failure to do so may keep the
	// context and its parent alive longer than necessary.
	defer cancel()

	err := s.Weighted.Acquire(ctx, n)
	if err != nil {
		return false
	} else {
		return true
	}
}
