package batch

import (
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

// Store is an in-memory batch repository. Stored batches are never handed out
// directly: reads return value copies (mirroring run.Registry), and all
// mutation happens under the store lock via Apply.
type Store struct {
	mu      sync.RWMutex
	batches map[model.ID]*Batch
}

func NewStore() *Store {
	return &Store{batches: map[model.ID]*Batch{}}
}

func (s *Store) Add(batch *Batch) {
	s.mu.Lock()
	s.batches[batch.ID] = cloneBatch(batch)
	s.mu.Unlock()
}

func (s *Store) Get(id model.ID) (*Batch, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	batch, ok := s.batches[id]
	if !ok {
		return nil, false
	}
	return cloneBatch(batch), true
}

func (s *Store) List() []*Batch {
	s.mu.RLock()
	batches := make([]*Batch, 0, len(s.batches))
	for _, batch := range s.batches {
		batches = append(batches, cloneBatch(batch))
	}
	s.mu.RUnlock()
	sort.Slice(batches, func(i, j int) bool { return batches[i].CreatedAt.Before(batches[j].CreatedAt) })
	return batches
}

// Apply runs fn against the live batch under the write lock and returns a
// post-update copy. Scheduling decisions and item transitions must go through
// Apply so they are serialized.
func (s *Store) Apply(id model.ID, fn func(*Batch) error) (*Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch, ok := s.batches[id]
	if !ok {
		return nil, ErrBatchNotFound
	}
	if err := fn(batch); err != nil {
		return cloneBatch(batch), err
	}
	batch.UpdatedAt = time.Now().UTC()
	return cloneBatch(batch), nil
}

// allowedBatchTransitions defines the batch state machine.
var allowedBatchTransitions = map[BatchState]map[BatchState]bool{
	Queued:    {Active: true, Cancelled: true},
	Active:    {Paused: true, Completed: true, Cancelled: true},
	Paused:    {Active: true, Cancelled: true},
	Completed: {},
	Cancelled: {},
}

// TransitionBatch validates and applies a batch state change.
func (s *Store) TransitionBatch(id model.ID, next BatchState) (*Batch, error) {
	return s.Apply(id, func(b *Batch) error {
		if !allowedBatchTransitions[b.State][next] {
			return ErrInvalidTransition
		}
		b.State = next
		return nil
	})
}

func canTransitionItem(from, to ItemState) bool {
	switch from {
	case ItemPending:
		return to == ItemReady || to == ItemRunning || to == ItemBlocked || to == ItemCancelled
	case ItemReady:
		return to == ItemRunning || to == ItemBlocked || to == ItemCancelled
	case ItemRunning:
		return to == ItemSucceeded || to == ItemFailed
	default:
		return false
	}
}

func (b *Batch) item(id model.ID) *Item {
	for _, item := range b.Items {
		if item.ID == id {
			return item
		}
	}
	return nil
}
