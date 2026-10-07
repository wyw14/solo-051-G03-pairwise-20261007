package chamber

import (
	"sync"

	"github.com/wyw14/cry-161/internal/model"
)

// Occupancy tracks the single run that currently owns a chamber. Each grant
// gets an increasing epoch that acts as a fencing token; releasing requires
// presenting the matching holder and epoch, so a stale release from a previous
// run cannot free a later run's occupancy.
type Occupancy struct {
	mu     sync.Mutex
	holder model.ID
	epoch  uint64
}

// Acquire grants the chamber to holder when it is free, returning the fencing
// token. An occupied chamber yields ok=false.
func (o *Occupancy) Acquire(holder model.ID) (token uint64, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.holder != "" {
		return o.epoch, false
	}
	o.epoch++
	o.holder = holder
	return o.epoch, true
}

// Release frees the chamber only when holder and token both match the current
// grant. A stale token from an earlier run returns false and leaves the
// current occupant untouched.
func (o *Occupancy) Release(holder model.ID, token uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.holder == "" || o.holder != holder || o.epoch != token {
		return false
	}
	o.holder = ""
	return true
}

func (o *Occupancy) Current() (holder model.ID, token uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.holder, o.epoch
}

func (c *Chamber) Acquire(holder model.ID) (uint64, bool) {
	return c.occ.Acquire(holder)
}

func (c *Chamber) Release(holder model.ID, token uint64) bool {
	return c.occ.Release(holder, token)
}

// Occupant returns the run that currently holds the chamber, or "" when free.
func (c *Chamber) Occupant() model.ID {
	holder, _ := c.occ.Current()
	return holder
}
