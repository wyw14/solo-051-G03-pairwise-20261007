package batch

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

// Hardware is the port through which the orchestrator drives a single run.
// The concrete implementation lives in the service package so the batch
// package stays free of a service import (no import cycle).
type Hardware interface {
	// StartRun reserves chamber and carrier leases, creates the run, pumps the
	// chamber down and brings it to conditioning. A non-empty runID means the
	// run (and its leases) exist and the caller MUST invoke Cleanup exactly
	// once. An empty runID means StartRun already released everything it took.
	StartRun(ctx context.Context, batchID model.ID, item Item) (runID model.ID, err error)
	// RunLayers coats every layer of the run's recipe in order.
	RunLayers(ctx context.Context, runID model.ID) error
	// FinishRun cools/vents the run and records the automatic release decision.
	FinishRun(ctx context.Context, runID model.ID) error
	// Cleanup returns chamber, carrier and power quota to a clean state. It is
	// idempotent and fence-checked and must run on every started run, success
	// or failure, using a context that outlives cancellation of the run.
	Cleanup(ctx context.Context, runID model.ID)
}

// Observer receives scheduling events, typically to append journal records.
type Observer interface {
	BatchChanged(batchID model.ID, from, to BatchState)
	ItemBlocked(batchID, itemID model.ID, reason string)
}

type options struct {
	maxInFlight    int
	tick           time.Duration
	cleanupTimeout time.Duration
	observer       Observer
}

// Option configures an Executor.
type Option func(*options)

// WithMaxInFlight caps concurrently running items. Defaults to 1, matching the
// single physical chamber/carrier; the real chamber lease enforces the bound.
func WithMaxInFlight(n int) Option {
	return func(o *options) {
		if n >= 1 {
			o.maxInFlight = n
		}
	}
}

func WithTick(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.tick = d
		}
	}
}

func WithCleanupTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.cleanupTimeout = d
		}
	}
}

func WithObserver(ob Observer) Option {
	return func(o *options) { o.observer = ob }
}

var errNotClaimed = errors.New("item no longer ready")

// Executor runs batch scheduling loops.
type Executor struct {
	store *Store
	hw    Hardware
	opts  options

	ctx context.Context

	mu    sync.Mutex
	loops map[model.ID]*batchLoop
}

type batchLoop struct {
	id       model.ID
	kick     chan struct{}
	inFlight int
	cancel   bool
}

func NewExecutor(store *Store, hw Hardware, opts ...Option) *Executor {
	o := options{maxInFlight: 1, tick: 20 * time.Millisecond, cleanupTimeout: 10 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	return &Executor{store: store, hw: hw, opts: o, ctx: context.Background(), loops: map[model.ID]*batchLoop{}}
}

// Start binds the executor to the process-lifetime context. Pausing never
// cancels in-flight runs; only this context (process shutdown) does, and even
// then resource cleanup runs on an independent, time-bounded context.
func (x *Executor) Start(ctx context.Context) {
	x.ctx = ctx
}

func (x *Executor) loopFor(id model.ID) *batchLoop {
	x.mu.Lock()
	defer x.mu.Unlock()
	l, ok := x.loops[id]
	if !ok {
		l = &batchLoop{id: id, kick: make(chan struct{}, 1)}
		x.loops[id] = l
	}
	return l
}

func (x *Executor) signal(id model.ID) {
	x.mu.Lock()
	l, ok := x.loops[id]
	x.mu.Unlock()
	if !ok {
		return
	}
	select {
	case l.kick <- struct{}{}:
	default:
	}
}

// Spawn activates a queued batch and launches its scheduling loop.
func (x *Executor) Spawn(id model.ID) error {
	batch, ok := x.store.Get(id)
	if !ok {
		return ErrBatchNotFound
	}
	from := batch.State
	updated, err := x.store.TransitionBatch(id, Active)
	if err != nil {
		return err
	}
	_ = x.loopFor(id)
	x.notifyBatch(id, from, updated.State)
	go x.run(id)
	return nil
}

// Pause raises the barrier: no new items start, but in-flight runs finish.
func (x *Executor) Pause(id model.ID) error {
	batch, ok := x.store.Get(id)
	if !ok {
		return ErrBatchNotFound
	}
	from := batch.State
	updated, err := x.store.TransitionBatch(id, Paused)
	if err != nil {
		return err
	}
	x.notifyBatch(id, from, updated.State)
	return nil
}

// Resume lowers the barrier and wakes the loop.
func (x *Executor) Resume(id model.ID) error {
	batch, ok := x.store.Get(id)
	if !ok {
		return ErrBatchNotFound
	}
	from := batch.State
	updated, err := x.store.TransitionBatch(id, Active)
	if err != nil {
		return err
	}
	x.notifyBatch(id, from, updated.State)
	x.signal(id)
	return nil
}

// Cancel requests barrier cancellation: in-flight runs finish safely, then all
// not-yet-started items and the batch are marked cancelled.
func (x *Executor) Cancel(id model.ID) error {
	batch, ok := x.store.Get(id)
	if !ok {
		return ErrBatchNotFound
	}
	// Only a live batch can be cancelled; a terminal batch is a conflict.
	if batch.State != Queued && batch.State != Active && batch.State != Paused {
		return ErrInvalidTransition
	}
	l := x.loopFor(id)
	x.mu.Lock()
	l.cancel = true
	x.mu.Unlock()
	x.signal(id)
	return nil
}

func (x *Executor) notifyBatch(id model.ID, from, to BatchState) {
	if x.opts.observer != nil {
		x.opts.observer.BatchChanged(id, from, to)
	}
}

func (x *Executor) notifyBlocked(batchID model.ID, ids []model.ID) {
	if x.opts.observer == nil {
		return
	}
	for _, id := range ids {
		x.opts.observer.ItemBlocked(batchID, id, "a dependency failed")
	}
}

func (x *Executor) run(id model.ID) {
	batch, ok := x.store.Get(id)
	if !ok {
		return
	}
	graph := GraphFromBatch(batch)
	ticker := time.NewTicker(x.opts.tick)
	defer ticker.Stop()
	l := x.loopFor(id)

	for {
		select {
		case <-x.ctx.Done():
			x.mu.Lock()
			l.cancel = true
			x.mu.Unlock()
		case <-l.kick:
		case <-ticker.C:
		}

		current, ok := x.store.Get(id)
		if !ok {
			return
		}
		if current.State == Completed || current.State == Cancelled {
			return
		}

		x.mu.Lock()
		inFlight := l.inFlight
		cancel := l.cancel
		x.mu.Unlock()

		if cancel {
			if inFlight == 0 {
				if x.finalizeCancelled(id) {
					return
				}
			}
			// Barrier while in-flight runs drain; their completion kicks us.
			continue
		}

		// Recompute blocked cascades and ready items from dependency outcomes.
		refreshed, _ := x.store.Apply(id, func(b *Batch) error {
			refreshStates(b, graph)
			return nil
		})
		counts := refreshed.Counts()

		if counts.Running+counts.Pending+counts.Ready == 0 && inFlight == 0 && refreshed.State == Active {
			if x.finalizeCompleted(id) {
				return
			}
			continue
		}

		if refreshed.State != Active || inFlight >= x.opts.maxInFlight {
			// Paused, or the physical concurrency limit is reached: hold the
			// barrier. In-flight runs are never cancelled here.
			continue
		}

		claimed := x.claim(refreshed)
		if claimed == nil {
			continue
		}

		x.mu.Lock()
		l.inFlight++
		x.mu.Unlock()
		go x.executeItem(id, graph, claimed)
	}
}

// refreshStates blocks items whose dependency reached a terminal non-success
// state and promotes pending items to ready once every dependency succeeded.
func refreshStates(b *Batch, graph *Graph) {
	stateOf := func(nid model.ID) ItemState {
		if it := b.item(nid); it != nil {
			return it.State
		}
		return ""
	}
	for _, it := range b.Items {
		if (it.State == ItemPending || it.State == ItemReady) && graph.anyDepFailed(it.ID, stateOf) {
			it.State = ItemBlocked
			if it.Error == "" {
				it.Error = "a dependency failed"
			}
		}
	}
	for _, it := range b.Items {
		if it.State == ItemPending && graph.allDepsSucceeded(it.ID, stateOf) {
			it.State = ItemReady
		}
	}
}

// claim picks the highest-priority ready item and atomically marks it running.
func (x *Executor) claim(batch *Batch) *Item {
	dispatcher := NewDispatcher(len(batch.Items))
	for _, it := range batch.Items {
		if it.State == ItemReady {
			dispatcher.Add(it)
		}
	}
	dispatcher.Sort()
	chosen := dispatcher.Pop()
	if chosen == nil {
		return nil
	}
	updated, err := x.store.Apply(batch.ID, func(b *Batch) error {
		it := b.item(chosen.ID)
		if it == nil || it.State != ItemReady {
			return errNotClaimed
		}
		it.State = ItemRunning
		it.StartedAt = time.Now().UTC()
		return nil
	})
	if err != nil {
		return nil
	}
	for _, it := range updated.Items {
		if it.ID == chosen.ID {
			copy := *it
			return &copy
		}
	}
	return nil
}

func (x *Executor) executeItem(batchID model.ID, graph *Graph, item *Item) {
	l := x.loopFor(batchID)
	defer func() {
		x.mu.Lock()
		l.inFlight--
		x.mu.Unlock()
		x.signal(batchID)
	}()

	runID, startErr := x.hw.StartRun(x.ctx, batchID, *item)
	if startErr != nil {
		if runID != "" {
			x.cleanup(runID)
		}
		x.settle(batchID, item.ID, "", startErr)
		return
	}
	x.setRunID(batchID, item.ID, runID)

	runErr := x.hw.RunLayers(x.ctx, runID)
	if runErr == nil {
		runErr = x.hw.FinishRun(x.ctx, runID)
	}
	x.cleanup(runID)
	x.settle(batchID, item.ID, runID, runErr)

	if runErr != nil {
		// Failure isolation: every transitive dependent is blocked; unrelated
		// items keep running on the freshly released resources.
		x.notifyBlocked(batchID, graph.TransitiveDependents(item.ID))
	}
}

// cleanup always runs against an independent, time-bounded context so resource
// reclamation completes even when the batch/process context is cancelled.
func (x *Executor) cleanup(runID model.ID) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(x.ctx), x.opts.cleanupTimeout)
	defer cancel()
	x.hw.Cleanup(ctx, runID)
}

func (x *Executor) setRunID(batchID, itemID, runID model.ID) {
	_, _ = x.store.Apply(batchID, func(b *Batch) error {
		if it := b.item(itemID); it != nil {
			it.RunID = runID
		}
		return nil
	})
}

// settle records the terminal item outcome (running -> succeeded/failed).
func (x *Executor) settle(batchID, itemID, runID model.ID, cause error) {
	_, _ = x.store.Apply(batchID, func(b *Batch) error {
		it := b.item(itemID)
		if it == nil {
			return nil
		}
		it.FinishedAt = time.Now().UTC()
		if runID != "" {
			it.RunID = runID
		}
		if cause != nil {
			it.State = ItemFailed
			it.Error = cause.Error()
		} else if canTransitionItem(it.State, ItemSucceeded) {
			it.State = ItemSucceeded
			it.Error = ""
		}
		return nil
	})
}

func (x *Executor) finalizeCompleted(id model.ID) bool {
	batch, ok := x.store.Get(id)
	if !ok {
		return true
	}
	from := batch.State
	updated, err := x.store.TransitionBatch(id, Completed)
	if err != nil {
		return false
	}
	x.notifyBatch(id, from, updated.State)
	return true
}

// finalizeCancelled cancels every not-yet-started item and moves an
// active/paused batch to cancelled. It returns false when the batch cannot be
// finalized in its current state (caller retries on the next tick).
func (x *Executor) finalizeCancelled(id model.ID) bool {
	batch, ok := x.store.Get(id)
	if !ok {
		return true
	}
	from := batch.State
	var blocked []model.ID
	updated, err := x.store.Apply(id, func(b *Batch) error {
		if b.State != Active && b.State != Paused {
			return ErrInvalidTransition
		}
		for _, it := range b.Items {
			if it.State == ItemPending || it.State == ItemReady {
				blocked = append(blocked, it.ID)
				it.State = ItemCancelled
				it.Error = "batch cancelled"
			}
		}
		b.State = Cancelled
		return nil
	})
	if err != nil {
		return false
	}
	x.notifyBlocked(id, blocked)
	x.notifyBatch(id, from, updated.State)
	return true
}
