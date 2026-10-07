package run

import (
	"github.com/wyw14/cry-161/internal/model"
	"sync"
	"time"
)

type Registry struct {
	mu   sync.RWMutex
	runs map[model.ID]*model.Run
}

func NewRegistry() *Registry { return &Registry{runs: map[model.ID]*model.Run{}} }
func (r *Registry) Add(value *model.Run) {
	r.mu.Lock()
	copy := *value
	r.runs[value.ID] = &copy
	r.mu.Unlock()
}
func (r *Registry) Get(id model.ID) (*model.Run, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.runs[id]
	if !ok {
		return nil, false
	}
	copy := *value
	return &copy, true
}
func (r *Registry) Transition(id model.ID, next model.RunState) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.runs[id]
	if !ok || !model.CanAdvance(value.State, next) {
		return false
	}
	value.State = next
	value.UpdatedAt = time.Now().UTC()
	return true
}
func (r *Registry) SetLayer(id model.ID, number int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.runs[id]
	if !ok {
		return false
	}
	value.Layer = number
	value.UpdatedAt = time.Now().UTC()
	return true
}
func (r *Registry) UpdateError(id model.ID, err error) {
	r.mu.Lock()
	if value, ok := r.runs[id]; ok {
		value.State = model.Failed
		if err != nil {
			value.Error = err.Error()
		}
		value.UpdatedAt = time.Now().UTC()
	}
	r.mu.Unlock()
}
func (r *Registry) List() []*model.Run {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*model.Run, 0, len(r.runs))
	for _, value := range r.runs {
		copy := *value
		out = append(out, &copy)
	}
	return out
}
