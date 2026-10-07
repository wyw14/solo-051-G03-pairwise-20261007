package batch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

// fakeHW simulates the run hardware with an internal "active run" set that
// stands in for chamber/carrier/power leases. Cleanup removes a run from that
// set, so tests can assert symmetric release.
type fakeHW struct {
	mu sync.Mutex

	runByID map[model.ID]string
	active  map[model.ID]bool
	order   []string
	cleaned []string
	failAt  map[string]bool
	gates   map[string]chan struct{} // RunLayers blocks until the gate closes
	started chan string

	runCtxErr map[model.ID]error
}

func newFakeHW() *fakeHW {
	return &fakeHW{
		runByID:   map[model.ID]string{},
		active:    map[model.ID]bool{},
		failAt:    map[string]bool{},
		gates:     map[string]chan struct{}{},
		started:   make(chan string, 16),
		runCtxErr: map[model.ID]error{},
	}
}

func (f *fakeHW) gate(key string) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan struct{})
	f.gates[key] = ch
	return ch
}

func (f *fakeHW) fail(key string) {
	f.mu.Lock()
	f.failAt[key] = true
	f.mu.Unlock()
}

func (f *fakeHW) StartRun(_ context.Context, _ model.ID, item Item) (model.ID, error) {
	runID := model.NewID()
	f.mu.Lock()
	f.runByID[runID] = item.Key
	f.active[runID] = true
	f.order = append(f.order, item.Key)
	f.mu.Unlock()
	select {
	case f.started <- item.Key:
	default:
	}
	return runID, nil
}

func (f *fakeHW) RunLayers(ctx context.Context, runID model.ID) error {
	f.mu.Lock()
	key := f.runByID[runID]
	gate := f.gates[key]
	fail := f.failAt[key]
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}
	f.mu.Lock()
	f.runCtxErr[runID] = ctx.Err()
	f.mu.Unlock()
	if fail {
		return contextError("layer failed for " + key)
	}
	return nil
}

func (f *fakeHW) FinishRun(context.Context, model.ID) error { return nil }

func (f *fakeHW) Cleanup(_ context.Context, runID model.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := f.runByID[runID]
	if f.active[runID] {
		f.cleaned = append(f.cleaned, key)
	}
	delete(f.active, runID)
}

func (f *fakeHW) snapshot() (order, cleaned []string, active int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...), append([]string(nil), f.cleaned...), len(f.active)
}

func newTestExecutor(t *testing.T, hw Hardware, tick time.Duration) (*Store, *Executor) {
	t.Helper()
	store := NewStore()
	exec := NewExecutor(store, hw, WithTick(tick), WithMaxInFlight(1))
	exec.Start(context.Background())
	return store, exec
}

func spawnBatch(t *testing.T, store *Store, exec *Executor, input CreateBatchInput) model.ID {
	t.Helper()
	b := NewBuild(model.NewID(), input, time.Now())
	store.Add(b)
	if err := exec.Spawn(b.ID); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	return b.ID
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func itemState(b *Batch, key string) ItemState {
	for _, it := range b.Items {
		if it.Key == key {
			return it.State
		}
	}
	return ""
}

func simpleItem(key string, priority int, deps ...string) ItemInput {
	return ItemInput{Key: key, RecipeID: model.ID("recipe-1"), Priority: priority, DependsOn: deps}
}

func TestExecutor_PriorityOrder_NoDeps(t *testing.T) {
	hw := newFakeHW()
	store, exec := newTestExecutor(t, hw, 2*time.Millisecond)
	id := spawnBatch(t, store, exec, CreateBatchInput{Items: []ItemInput{
		simpleItem("low", 1), simpleItem("high", 9), simpleItem("mid", 5),
	}})
	waitFor(t, 2*time.Second, func() bool {
		b, _ := store.Get(id)
		return b.State == Completed
	})
	order, _, _ := hw.snapshot()
	want := []string{"high", "mid", "low"}
	if len(order) != 3 || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] {
		t.Fatalf("start order=%v want %v", order, want)
	}
}

func TestExecutor_DependencyRequiresSuccess(t *testing.T) {
	hw := newFakeHW()
	store, exec := newTestExecutor(t, hw, 2*time.Millisecond)
	gate := hw.gate("a")
	id := spawnBatch(t, store, exec, CreateBatchInput{Items: []ItemInput{
		simpleItem("a", 1), simpleItem("b", 1, "a"),
	}})

	select {
	case <-hw.started:
	case <-time.After(time.Second):
		t.Fatal("a never started")
	}
	// While a is mid-run, b must stay pending.
	time.Sleep(40 * time.Millisecond)
	b, _ := store.Get(id)
	if got := itemState(b, "b"); got != ItemPending {
		t.Fatalf("b state=%s want pending while a unfinished", got)
	}
	close(gate)
	waitFor(t, 2*time.Second, func() bool {
		b, _ = store.Get(id)
		return b.State == Completed
	})
	b, _ = store.Get(id)
	if got := itemState(b, "b"); got != ItemSucceeded {
		t.Fatalf("b=%s want succeeded", got)
	}
	order, _, _ := hw.snapshot()
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("order=%v want [a b]", order)
	}
}

func TestExecutor_FailurePropagatesAndIsolates(t *testing.T) {
	hw := newFakeHW()
	store, exec := newTestExecutor(t, hw, 2*time.Millisecond)
	hw.fail("a")
	// a -> b -> d ; c is independent.
	id := spawnBatch(t, store, exec, CreateBatchInput{Items: []ItemInput{
		simpleItem("a", 1), simpleItem("b", 1, "a"), simpleItem("c", 1), simpleItem("d", 1, "b"),
	}})
	waitFor(t, 2*time.Second, func() bool {
		b, _ := store.Get(id)
		return b.State == Completed
	})
	b, _ := store.Get(id)
	if got := itemState(b, "a"); got != ItemFailed {
		t.Errorf("a=%s want failed", got)
	}
	if got := itemState(b, "b"); got != ItemBlocked {
		t.Errorf("b=%s want blocked", got)
	}
	if got := itemState(b, "d"); got != ItemBlocked {
		t.Errorf("d=%s want blocked", got)
	}
	if got := itemState(b, "c"); got != ItemSucceeded {
		t.Errorf("c=%s want succeeded (unrelated run must continue)", got)
	}
	// The failed run's resources were released and the independent run ran.
	order, cleaned, active := hw.snapshot()
	if active != 0 {
		t.Errorf("active=%d want 0 after completion", active)
	}
	cleanedA := false
	for _, key := range cleaned {
		if key == "a" {
			cleanedA = true
		}
	}
	if !cleanedA {
		t.Errorf("failed run a was never cleaned: %v", cleaned)
	}
	if len(order) < 2 {
		t.Errorf("independent run c never started after failure: order=%v", order)
	}
}

func TestExecutor_PauseBarrierKeepsInFlightAlive(t *testing.T) {
	hw := newFakeHW()
	store, exec := newTestExecutor(t, hw, 2*time.Millisecond)
	gate := hw.gate("a")
	id := spawnBatch(t, store, exec, CreateBatchInput{Items: []ItemInput{
		simpleItem("a", 1), simpleItem("b", 1), simpleItem("c", 1),
	}})
	<-hw.started // a is running and blocked on the gate

	if err := exec.Pause(id); err != nil {
		t.Fatalf("pause: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	order, _, _ := hw.snapshot()
	if len(order) != 1 || order[0] != "a" {
		t.Fatalf("barrier violated, started=%v want only a", order)
	}

	close(gate) // a finishes naturally despite the pause
	waitFor(t, 2*time.Second, func() bool {
		b, _ := store.Get(id)
		return itemState(b, "a") == ItemSucceeded
	})
	b, _ := store.Get(id)
	if b.State != Paused {
		t.Fatalf("batch=%s want paused", b.State)
	}
	// b/c may be promoted to ready, but while paused they must not be running.
	for _, key := range []string{"b", "c"} {
		if got := itemState(b, key); got == ItemRunning || got == ItemSucceeded {
			t.Fatalf("%s=%s must not start while paused", key, got)
		}
	}
	order, _, _ = hw.snapshot()
	if len(order) != 1 {
		t.Fatalf("nothing may start during pause, order=%v", order)
	}

	if err := exec.Resume(id); err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		b, _ = store.Get(id)
		return b.State == Completed
	})
}

func TestExecutor_CancelDrainsInFlight(t *testing.T) {
	hw := newFakeHW()
	store, exec := newTestExecutor(t, hw, 2*time.Millisecond)
	gate := hw.gate("a")
	id := spawnBatch(t, store, exec, CreateBatchInput{Items: []ItemInput{
		simpleItem("a", 1), simpleItem("b", 1), simpleItem("c", 1),
	}})
	<-hw.started
	if err := exec.Cancel(id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	close(gate) // in-flight run completes safely
	waitFor(t, 2*time.Second, func() bool {
		b, _ := store.Get(id)
		return b.State == Cancelled
	})
	b, _ := store.Get(id)
	if got := itemState(b, "a"); got != ItemSucceeded {
		t.Errorf("a=%s want succeeded (in-flight finishes)", got)
	}
	if got := itemState(b, "b"); got != ItemCancelled {
		t.Errorf("b=%s want cancelled", got)
	}
	if got := itemState(b, "c"); got != ItemCancelled {
		t.Errorf("c=%s want cancelled", got)
	}
	_, _, active := hw.snapshot()
	if active != 0 {
		t.Errorf("active=%d want 0", active)
	}
}

func TestExecutor_RepeatedPauseIsConflict(t *testing.T) {
	hw := newFakeHW()
	store, exec := newTestExecutor(t, hw, 2*time.Millisecond)
	id := spawnBatch(t, store, exec, CreateBatchInput{Items: []ItemInput{simpleItem("a", 1)}})
	waitFor(t, 2*time.Second, func() bool {
		b, _ := store.Get(id)
		return b.State == Completed
	})
	// A completed batch cannot be paused.
	if err := exec.Pause(id); err != ErrInvalidTransition {
		t.Fatalf("pause completed err=%v want ErrInvalidTransition", err)
	}
	// A completed batch cannot be cancelled either.
	if err := exec.Cancel(id); err != ErrInvalidTransition {
		t.Fatalf("cancel completed err=%v want ErrInvalidTransition", err)
	}
}

// contextError is a small error type used by the fake to avoid importing
// fmt solely for failures.
type contextError string

func (e contextError) Error() string { return string(e) }
