package source

import (
	"context"
	"fmt"
	"sync"
)

// Lease is a fencing token for a single power-quota grant. Every AcquireLease
// returns a unique ID; only that exact lease can later release the weight.
type Lease struct {
	ID     uint64 `json:"id"`
	Weight int    `json:"weight"`
}

type Admission struct {
	mu       sync.Mutex
	capacity int
	used     int
	nextID   uint64
	live     map[uint64]int // lease ID -> weight; used always equals the sum of live
}

func NewAdmission(capacity int) *Admission {
	return &Admission{capacity: capacity, live: map[uint64]int{}}
}

// AcquireLease waits until weight fits the capacity and grants a uniquely
// identified lease. Callers must release with the returned Lease; a stale or
// duplicated release (including a late Stop from a failed run that arrives
// after another run acquired power) is rejected by Release and never deducts
// another run's quota.
func (a *Admission) AcquireLease(ctx context.Context, weight int) (Lease, error) {
	if weight <= 0 {
		return Lease{}, fmt.Errorf("invalid power weight")
	}
	for {
		a.mu.Lock()
		if a.used+weight <= a.capacity {
			a.nextID++
			id := a.nextID
			a.live[id] = weight
			a.used += weight
			a.mu.Unlock()
			return Lease{ID: id, Weight: weight}, nil
		}
		a.mu.Unlock()
		select {
		case <-ctx.Done():
			return Lease{}, ctx.Err()
		default:
		}
	}
}

// Release revokes exactly the given lease. It returns false and changes
// nothing when the lease is unknown or was already released, so late or
// duplicated releases are harmless even when interleaved with newer leases.
func (a *Admission) Release(lease Lease) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	weight, ok := a.live[lease.ID]
	if !ok {
		return false
	}
	delete(a.live, lease.ID)
	a.used -= weight
	if a.used < 0 {
		a.used = 0
	}
	return true
}

func (a *Admission) Used() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.used
}

// Acquire grants weight without returning a lease and Release(weight) releases
// it blindly. These are retained only for self-contained audits that create a
// throwaway Admission and pair acquire/release immediately on the same
// goroutine. Production paths must use AcquireLease/Release(Lease).
func (a *Admission) Acquire(ctx context.Context, weight int) error {
	if weight <= 0 {
		return fmt.Errorf("invalid power weight")
	}
	for {
		a.mu.Lock()
		if a.used+weight <= a.capacity {
			a.used += weight
			a.mu.Unlock()
			return nil
		}
		a.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (a *Admission) ReleaseWeight(weight int) {
	a.mu.Lock()
	a.used -= weight
	if a.used < 0 {
		a.used = 0
	}
	a.mu.Unlock()
}
