package batch

import (
	"context"
	"fmt"
	"github.com/wyw14/cry-161/internal/chamber"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/source"
	"github.com/wyw14/cry-161/internal/substrate"
	"sync"
	"testing"
	"time"
)

// fakeExecutor records the order of run executions, loads the carrier like
// the real executor, and fails runs of configured recipes at layer time so
// the failure happens while resources are held.
type fakeExecutor struct {
	mu        sync.Mutex
	carrier   *substrate.Carrier
	begun     []model.ID
	runBatch  map[model.ID]string
	runRecipe map[model.ID]model.ID
	layerFail map[model.ID]bool
	layers    map[model.ID][]model.Layer
	runSeq    int
}

func newFakeExecutor(carrier *substrate.Carrier) *fakeExecutor {
	return &fakeExecutor{carrier: carrier, runBatch: map[model.ID]string{}, runRecipe: map[model.ID]model.ID{}, layerFail: map[model.ID]bool{}, layers: map[model.ID][]model.Layer{}}
}

func (f *fakeExecutor) addRecipe(id model.ID, failAtLayer bool) {
	f.layers[id] = []model.Layer{{Number: 1, Material: "SiO2", TargetNanometers: 100, RateNanometers: 2}}
	f.layerFail[id] = failAtLayer
}

func (f *fakeExecutor) BeginRun(ctx context.Context, b *Batch, spec RunSpec) (model.ID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runSeq++
	runID := model.ID(fmt.Sprintf("run-%d", f.runSeq))
	if err := f.carrier.Load(b.Name, runID); err != nil {
		return "", err
	}
	f.begun = append(f.begun, runID)
	f.runBatch[runID] = b.Name
	f.runRecipe[runID] = spec.RecipeID
	return runID, nil
}

func (f *fakeExecutor) ExecuteLayer(ctx context.Context, runID model.ID, layer model.Layer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.layerFail[f.runRecipe[runID]] {
		return fmt.Errorf("layer %d deposition failed", layer.Number)
	}
	return nil
}

func (f *fakeExecutor) CompleteRun(ctx context.Context, runID model.ID) error    { return nil }
func (f *fakeExecutor) FailRun(ctx context.Context, runID model.ID, cause error) {}

func (f *fakeExecutor) RecipeLayers(recipeID model.ID) ([]model.Layer, error) {
	layers, ok := f.layers[recipeID]
	if !ok {
		return nil, fmt.Errorf("recipe not found")
	}
	return layers, nil
}

func (f *fakeExecutor) begunCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.begun)
}

func newTestOrchestrator(t *testing.T) (*Orchestrator, *LeasePool, *substrate.Carrier, *source.Admission) {
	t.Helper()
	carrier := substrate.NewCarrier()
	admission := source.NewAdmission(100)
	pool := NewLeasePool(chamber.New("test-chamber"), carrier, admission)
	orch := NewOrchestrator(pool, journal.New(t.TempDir()), "test-chamber")
	return orch, pool, carrier, admission
}

func waitForState(t *testing.T, orch *Orchestrator, id model.ID, want State) *Batch {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b, ok := orch.Get(id)
		if ok && b.State == want {
			return b
		}
		time.Sleep(2 * time.Millisecond)
	}
	b, _ := orch.Get(id)
	t.Fatalf("batch %s did not reach state %s (state=%s error=%s)", id, want, b.State, b.Error)
	return nil
}

func runOrchestrator(orch *Orchestrator, exec RunExecutor) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go orch.Run(ctx, exec)
	return cancel
}

func TestPriorityOrdering(t *testing.T) {
	orch, _, carrier, _ := newTestOrchestrator(t)
	exec := newFakeExecutor(carrier)
	low := model.ID("recipe-low")
	high := model.ID("recipe-high")
	exec.addRecipe(low, false)
	exec.addRecipe(high, false)

	// Submit both before the dispatch loop starts so ordering is decided
	// by priority, not submission timing.
	lowBatch, err := orch.Submit(&Batch{Name: "low", Priority: 1, Specs: []RunSpec{{RecipeID: low}}})
	if err != nil {
		t.Fatal(err)
	}
	highBatch, err := orch.Submit(&Batch{Name: "high", Priority: 9, Specs: []RunSpec{{RecipeID: high}}})
	if err != nil {
		t.Fatal(err)
	}
	cancel := runOrchestrator(orch, exec)
	defer cancel()

	highDone := waitForState(t, orch, highBatch.ID, Completed)
	waitForState(t, orch, lowBatch.ID, Completed)

	exec.mu.Lock()
	first := exec.begun[0]
	exec.mu.Unlock()
	if highDone.Outcomes[0].RunID != first {
		t.Fatalf("high priority batch did not run first")
	}
}

func TestDependencyFailureBlocksDependents(t *testing.T) {
	orch, _, carrier, _ := newTestOrchestrator(t)
	exec := newFakeExecutor(carrier)
	bad := model.ID("recipe-bad")
	good := model.ID("recipe-good")
	exec.addRecipe(bad, true)
	exec.addRecipe(good, false)

	first, err := orch.Submit(&Batch{Name: "first", Specs: []RunSpec{{RecipeID: bad}}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := orch.Submit(&Batch{Name: "second", DependsOn: []model.ID{first.ID}, Specs: []RunSpec{{RecipeID: good}}})
	if err != nil {
		t.Fatal(err)
	}
	cancel := runOrchestrator(orch, exec)
	defer cancel()

	waitForState(t, orch, first.ID, Failed)
	blocked := waitForState(t, orch, second.ID, Blocked)
	if blocked.Error == "" {
		t.Fatalf("blocked batch should carry an explanation")
	}
	// Only the failed batch's run may have begun; the dependent's never.
	if exec.begunCount() != 1 {
		t.Fatalf("dependent batch must never dispatch after a dependency failure")
	}
}

func TestFailureIsolationReleasesResources(t *testing.T) {
	orch, pool, carrier, admission := newTestOrchestrator(t)
	exec := newFakeExecutor(carrier)
	bad := model.ID("recipe-bad")
	good := model.ID("recipe-good")
	exec.addRecipe(bad, true)
	exec.addRecipe(good, false)
	cancel := runOrchestrator(orch, exec)
	defer cancel()

	epochBefore := pool.Epoch()
	failed, err := orch.Submit(&Batch{Name: "failed", Specs: []RunSpec{{RecipeID: bad}}})
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, orch, failed.ID, Failed)

	if pool.Epoch() == epochBefore {
		t.Fatalf("failure must bump the resource epoch")
	}
	if carrier.Loaded() {
		t.Fatalf("carrier must be unloaded after a failure")
	}
	if admission.Used() != 0 {
		t.Fatalf("power quota must be fully released after a failure, used=%d", admission.Used())
	}
	if !pool.QuotaClear() {
		t.Fatalf("pool must report clear quota after invalidation")
	}

	// An independent batch submitted after the failure must re-acquire
	// fresh resources and complete.
	next, err := orch.Submit(&Batch{Name: "next", Specs: []RunSpec{{RecipeID: good}}})
	if err != nil {
		t.Fatal(err)
	}
	done := waitForState(t, orch, next.ID, Completed)
	if len(done.Outcomes) != 1 || done.Outcomes[0].Status != "completed" {
		t.Fatalf("independent batch did not complete after failure: %+v", done.Outcomes)
	}
	if admission.Used() != 0 || carrier.Loaded() {
		t.Fatalf("resources must be released again after the follow-up batch")
	}
}

func TestPauseResume(t *testing.T) {
	orch, _, carrier, _ := newTestOrchestrator(t)
	exec := newFakeExecutor(carrier)
	recipe := model.ID("recipe-a")
	exec.addRecipe(recipe, false)

	paused, err := orch.Submit(&Batch{Name: "pausable", Specs: []RunSpec{{RecipeID: recipe}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := orch.Pause(paused.ID); err != nil {
		t.Fatal(err)
	}
	cancel := runOrchestrator(orch, exec)
	defer cancel()

	time.Sleep(50 * time.Millisecond)
	if exec.begunCount() != 0 {
		t.Fatalf("paused batch must not dispatch")
	}
	if err := orch.Resume(paused.ID); err != nil {
		t.Fatal(err)
	}
	waitForState(t, orch, paused.ID, Completed)
}

func TestAbortSkipsRemainingRuns(t *testing.T) {
	orch, _, carrier, _ := newTestOrchestrator(t)
	exec := newFakeExecutor(carrier)
	recipe := model.ID("recipe-a")
	exec.addRecipe(recipe, false)

	b, err := orch.Submit(&Batch{Name: "abortable", Specs: []RunSpec{{RecipeID: recipe}, {RecipeID: recipe}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := orch.Abort(b.ID); err != nil {
		t.Fatal(err)
	}
	cancel := runOrchestrator(orch, exec)
	defer cancel()

	time.Sleep(50 * time.Millisecond)
	if exec.begunCount() != 0 {
		t.Fatalf("aborted batch must not dispatch")
	}
	current, _ := orch.Get(b.ID)
	if current.State != Aborted {
		t.Fatalf("expected aborted state, got %s", current.State)
	}
}

func TestSubmitValidation(t *testing.T) {
	orch, _, _, _ := newTestOrchestrator(t)
	if _, err := orch.Submit(&Batch{Name: "", Specs: []RunSpec{{RecipeID: "x"}}}); err == nil {
		t.Fatalf("empty name must be rejected")
	}
	if _, err := orch.Submit(&Batch{Name: "empty"}); err == nil {
		t.Fatalf("batch without runs must be rejected")
	}
	if _, err := orch.Submit(&Batch{Name: "dep", DependsOn: []model.ID{"missing"}, Specs: []RunSpec{{RecipeID: "x"}}}); err == nil {
		t.Fatalf("unknown dependency must be rejected")
	}
	if _, err := orch.Submit(&Batch{Name: "ok", Specs: []RunSpec{{RecipeID: "x"}}}); err != nil {
		t.Fatalf("valid batch must be accepted: %v", err)
	}
}
