package batch

import "sort"

// Dispatcher is a ready queue ordered exactly like run.Scheduler: higher
// priority first, ties broken by earlier enqueue time (FIFO). It is rebuilt
// from the store on every scheduling pass and holds no scheduling state of
// its own.
type Dispatcher struct {
	items []*Item
}

func NewDispatcher(capacity int) *Dispatcher {
	return &Dispatcher{items: make([]*Item, 0, capacity)}
}

func (d *Dispatcher) Add(item *Item) {
	d.items = append(d.items, item)
}

// Sort arranges the queued items by priority desc, then EnqueuedAt asc.
func (d *Dispatcher) Sort() {
	sort.SliceStable(d.items, func(i, j int) bool {
		if d.items[i].Priority != d.items[j].Priority {
			return d.items[i].Priority > d.items[j].Priority
		}
		return d.items[i].EnqueuedAt.Before(d.items[j].EnqueuedAt)
	})
}

// Pop removes and returns the highest-priority ready item, or nil.
func (d *Dispatcher) Pop() *Item {
	if len(d.items) == 0 {
		return nil
	}
	item := d.items[0]
	d.items = d.items[1:]
	return item
}

func (d *Dispatcher) Len() int { return len(d.items) }
