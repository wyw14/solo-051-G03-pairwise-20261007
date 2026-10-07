package batch

import (
	"context"
	"fmt"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"sort"
	"sync"
	"time"
)

// RunExecutor is implemented by the service layer; the orchestrator drives
// runs through it without knowing hardware details.
type RunExecutor interface {
	BeginRun(ctx context.Context, b *Batch, spec RunSpec) (model.ID, error)
	ExecuteLayer(ctx context.Context, runID model.ID, layer model.Layer) error
	CompleteRun(ctx context.Context, runID model.ID) error
	FailRun(ctx context.Context, runID model.ID, cause error)
	RecipeLayers(recipeID model.ID) ([]model.Layer, error)
}

type Orchestrator struct {
	mu      sync.Mutex
	batches map[model.ID]*Batch
	pool    *LeasePool
	journal *journal.Store
	chamber string
	wake    chan struct{}
}

func NewOrchestrator(pool *LeasePool, journalStore *journal.Store, chamberName string) *Orchestrator {
	return &Orchestrator{batches: map[model.ID]*Batch{}, pool: pool, journal: journalStore, chamber: chamberName, wake: make(chan struct{}, 1)}
}

func (o *Orchestrator) Submit(input *Batch) (*Batch, error) {
	b := *input
	b.ID = model.NewID()
	b.State = Pending
	b.CreatedAt = time.Now().UTC()
	b.UpdatedAt = b.CreatedAt
	b.Outcomes = nil
	if err := b.Validate(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, dep := range b.DependsOn {
		if _, ok := o.batches[dep]; !ok {
			return nil, fmt.Errorf("dependency %s not found", dep)
		}
	}
	if o.reachesLocked(b.ID, b.DependsOn, map[model.ID]bool{}) {
		return nil, fmt.Errorf("dependency cycle detected")
	}
	stored := b
	o.batches[b.ID] = &stored
	o.emitLocked("batch.submitted", &stored, map[string]any{"name": b.Name, "priority": b.Priority})
	o.notify()
	result := stored
	return &result, nil
}

// reachesLocked reports whether following deps (transitively) returns to id.
func (o *Orchestrator) reachesLocked(id model.ID, deps []model.ID, seen map[model.ID]bool) bool {
	for _, dep := range deps {
		if dep == id {
			return true
		}
		if seen[dep] {
			continue
		}
		seen[dep] = true
		if next, ok := o.batches[dep]; ok && o.reachesLocked(id, next.DependsOn, seen) {
			return true
		}
	}
	return false
}

func (o *Orchestrator) Get(id model.ID) (*Batch, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	value, ok := o.batches[id]
	if !ok {
		return nil, false
	}
	copy := copyBatch(value)
	return &copy, true
}

func (o *Orchestrator) List() []*Batch {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]*Batch, 0, len(o.batches))
	for _, value := range o.batches {
		copy := copyBatch(value)
		out = append(out, &copy)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func copyBatch(b *Batch) Batch {
	out := *b
	out.DependsOn = append([]model.ID(nil), b.DependsOn...)
	out.Specs = append([]RunSpec(nil), b.Specs...)
	out.Outcomes = append([]RunOutcome(nil), b.Outcomes...)
	return out
}

func (o *Orchestrator) Pause(id model.ID) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.batches[id]
	if !ok {
		return fmt.Errorf("batch not found")
	}
	if b.State != Pending && b.State != Running {
		return fmt.Errorf("cannot pause batch in state %s", b.State)
	}
	b.resumeState = b.State
	b.State = Paused
	b.UpdatedAt = time.Now().UTC()
	o.emitLocked("batch.paused", b, nil)
	return nil
}

func (o *Orchestrator) Resume(id model.ID) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.batches[id]
	if !ok {
		return fmt.Errorf("batch not found")
	}
	if b.State != Paused {
		return fmt.Errorf("cannot resume batch in state %s", b.State)
	}
	b.State = b.resumeState
	if b.State != Running {
		b.State = Pending
	}
	b.UpdatedAt = time.Now().UTC()
	o.emitLocked("batch.resumed", b, nil)
	o.notify()
	return nil
}

func (o *Orchestrator) Abort(id model.ID) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.batches[id]
	if !ok {
		return fmt.Errorf("batch not found")
	}
	if b.terminal() {
		return fmt.Errorf("cannot abort batch in state %s", b.State)
	}
	b.State = Aborted
	b.UpdatedAt = time.Now().UTC()
	o.emitLocked("batch.aborted", b, nil)
	o.notify()
	return nil
}

func (o *Orchestrator) notify() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *Orchestrator) emitLocked(kind string, b *Batch, extra map[string]any) {
	if o.journal == nil {
		return
	}
	data := map[string]any{"batch": b.Name, "state": b.State}
	for key, value := range extra {
		data[key] = value
	}
	_ = o.journal.Append(model.NewEvent(kind, b.ID, data))
}

// Run dispatches batches one at a time (single chamber) until ctx ends.
func (o *Orchestrator) Run(ctx context.Context, exec RunExecutor) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		id, ok := o.next()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-o.wake:
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		o.runBatch(ctx, exec, id)
	}
}

// next picks the highest-priority pending batch whose dependencies all
// completed. Batches whose dependency failed, was aborted or was blocked are
// themselves blocked so a failure can never propagate downstream.
func (o *Orchestrator) next() (model.ID, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var pick *Batch
	for _, b := range o.batches {
		if b.State != Pending {
			continue
		}
		ready := true
		for _, dep := range b.DependsOn {
			other, ok := o.batches[dep]
			if !ok {
				ready = false
				break
			}
			switch other.State {
			case Completed:
			case Failed, Aborted, Blocked:
				b.State = Blocked
				b.Error = fmt.Sprintf("dependency %s ended in state %s", dep, other.State)
				b.UpdatedAt = time.Now().UTC()
				o.emitLocked("batch.blocked", b, map[string]any{"dependency": dep})
				ready = false
			default:
				ready = false
			}
			if !ready {
				break
			}
		}
		if !ready || b.State != Pending {
			continue
		}
		if pick == nil || b.Priority > pick.Priority || (b.Priority == pick.Priority && b.CreatedAt.Before(pick.CreatedAt)) {
			pick = b
		}
	}
	if pick == nil {
		return "", false
	}
	pick.State = Running
	pick.UpdatedAt = time.Now().UTC()
	o.emitLocked("batch.started", pick, nil)
	return pick.ID, true
}

func (o *Orchestrator) runBatch(ctx context.Context, exec RunExecutor, id model.ID) {
	for {
		o.mu.Lock()
		b, ok := o.batches[id]
		if !ok || b.State == Aborted {
			o.mu.Unlock()
			return
		}
		if len(b.Outcomes) >= len(b.Specs) {
			b.State = Completed
			b.UpdatedAt = time.Now().UTC()
			o.emitLocked("batch.completed", b, nil)
			o.mu.Unlock()
			return
		}
		spec := b.Specs[len(b.Outcomes)]
		snapshot := copyBatch(b)
		o.mu.Unlock()

		if err := o.waitUntilRunnable(id); err != nil {
			return
		}
		if !o.executeRun(ctx, exec, &snapshot, spec) {
			return
		}
	}
}

// executeRun runs one spec. It returns false when the batch left the
// running state (failed or aborted) and the batch loop must stop.
func (o *Orchestrator) executeRun(ctx context.Context, exec RunExecutor, b *Batch, spec RunSpec) bool {
	layers, err := exec.RecipeLayers(spec.RecipeID)
	if err != nil {
		o.failBatch(b.ID, nil, spec, "", fmt.Errorf("recipe: %w", err))
		return false
	}
	// Dispatch barrier: never start a run on resources a previous run may
	// still hold. Refuse loudly instead of reusing dirty quota.
	if !o.pool.QuotaClear() {
		o.pool.Invalidate()
		o.failBatch(b.ID, nil, spec, "", ErrStaleResources)
		return false
	}
	lease, err := o.pool.Acquire(ctx, b.ID, b.Name, o.chamber, powerWeight(layers))
	if err != nil {
		if err == ErrStaleResources {
			o.pool.Invalidate()
		}
		o.failBatch(b.ID, nil, spec, "", err)
		return false
	}
	runID, err := exec.BeginRun(ctx, b, spec)
	if err != nil {
		o.failBatch(b.ID, lease, spec, runID, err)
		return false
	}
	for _, layer := range layers {
		if err := o.waitUntilRunnable(b.ID); err != nil {
			exec.FailRun(ctx, runID, fmt.Errorf("batch aborted"))
			o.pool.Invalidate()
			o.recordOutcome(b.ID, spec, runID, "failed", fmt.Errorf("batch aborted"))
			return false
		}
		if !o.pool.Valid(lease) {
			exec.FailRun(ctx, runID, fmt.Errorf("resource lease invalidated"))
			o.failBatch(b.ID, lease, spec, runID, fmt.Errorf("resource lease invalidated"))
			return false
		}
		if err := exec.ExecuteLayer(ctx, runID, layer); err != nil {
			exec.FailRun(ctx, runID, err)
			o.failBatch(b.ID, nil, spec, runID, err)
			return false
		}
	}
	if err := exec.CompleteRun(ctx, runID); err != nil {
		exec.FailRun(ctx, runID, err)
		o.failBatch(b.ID, nil, spec, runID, err)
		return false
	}
	o.pool.Release(lease)
	o.recordOutcome(b.ID, spec, runID, "completed", nil)
	return true
}

// powerWeight reserves quota for the most demanding layer of the recipe.
func powerWeight(layers []model.Layer) int {
	weight := 1
	for _, layer := range layers {
		if int(layer.RateNanometers) > weight {
			weight = int(layer.RateNanometers)
		}
	}
	return weight
}

// waitUntilRunnable blocks while the batch is paused and returns an error
// once the batch is aborted.
func (o *Orchestrator) waitUntilRunnable(id model.ID) error {
	for {
		o.mu.Lock()
		state := o.batches[id].State
		o.mu.Unlock()
		switch state {
		case Aborted:
			return fmt.Errorf("batch aborted")
		case Paused:
			select {
			case <-o.wake:
			case <-time.After(10 * time.Millisecond):
			}
		default:
			return nil
		}
	}
}

func (o *Orchestrator) recordOutcome(id model.ID, spec RunSpec, runID model.ID, status string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.batches[id]
	if !ok {
		return
	}
	outcome := RunOutcome{RunID: runID, RecipeID: spec.RecipeID, Status: status}
	if err != nil {
		outcome.Error = err.Error()
	}
	b.Outcomes = append(b.Outcomes, outcome)
	b.UpdatedAt = time.Now().UTC()
}

// failBatch marks the batch failed and invalidates every resource lease, so
// the failure is isolated: dependent batches are blocked and later batches
// re-acquire fresh chamber, carrier and power quota under a new epoch.
func (o *Orchestrator) failBatch(id model.ID, lease *Lease, spec RunSpec, runID model.ID, cause error) {
	if lease != nil {
		o.pool.Release(lease)
	}
	o.pool.Invalidate()
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.batches[id]
	if !ok {
		return
	}
	if runID != "" {
		outcome := RunOutcome{RunID: runID, RecipeID: spec.RecipeID, Status: "failed", Error: cause.Error()}
		b.Outcomes = append(b.Outcomes, outcome)
	}
	b.State = Failed
	b.Error = cause.Error()
	b.UpdatedAt = time.Now().UTC()
	o.emitLocked("batch.failed", b, map[string]any{"error": cause.Error(), "epoch": o.pool.Epoch()})
}
