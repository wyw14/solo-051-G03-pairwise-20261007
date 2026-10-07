package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/batch"
	"github.com/wyw14/cry-161/internal/model"
)

func newBatchService(t *testing.T) *Service {
	t.Helper()
	t.Setenv("OPTICCOAT_DATA", t.TempDir())
	return New()
}

func recipeID(t *testing.T, svc *Service) model.ID {
	t.Helper()
	recipes := svc.RecipesList()
	if len(recipes) == 0 {
		t.Fatal("no published recipes")
	}
	return recipes[0].ID
}

func waitBatch(t *testing.T, svc *Service, id model.ID, timeout time.Duration) *batch.Detail {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		detail, err := svc.BatchDetail(id)
		if err != nil {
			t.Fatalf("batch detail: %v", err)
		}
		if detail.State == batch.Completed || detail.State == batch.Cancelled {
			return detail
		}
		if time.Now().After(deadline) {
			t.Fatalf("batch %s did not settle, state=%s counts=%+v", id, detail.State, detail.Counts)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func batchItem(svc *Service, id model.ID, key string) *batch.Item {
	items, _ := svc.BatchItems(id)
	for _, item := range items {
		if item.Key == key {
			return item
		}
	}
	return nil
}

func runsForBatch(svc *Service, id model.ID) []*model.Run {
	out := make([]*model.Run, 0)
	for _, r := range svc.RunsList() {
		if r.Batch == string(id) {
			out = append(out, r)
		}
	}
	return out
}

func assertResourcesClean(t *testing.T, svc *Service) {
	t.Helper()
	if used := svc.Source.AdmissionUsed(); used != 0 {
		t.Errorf("admission used=%d want 0", used)
	}
	if svc.Carrier.Loaded() {
		t.Errorf("carrier still loaded")
	}
	if holder := svc.Chamber.Occupant(); holder != "" {
		t.Errorf("chamber still occupied by %s", holder)
	}
}

func itemInput(key string, recipe model.ID, priority int, deps ...string) batch.ItemInput {
	return batch.ItemInput{Key: key, RecipeID: recipe, Priority: priority, DependsOn: deps}
}

func TestE2E_BatchRunsAllLayersToReleased(t *testing.T) {
	svc := newBatchService(t)
	recipe := recipeID(t, svc)

	detail, err := svc.CreateBatch(context.Background(), batch.CreateBatchInput{
		Name:  "campaign",
		Items: []batch.ItemInput{itemInput("a", recipe, 10)},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	final := waitBatch(t, svc, detail.ID, 5*time.Second)
	if final.Counts.Succeeded != 1 {
		t.Fatalf("counts=%+v want 1 succeeded", final.Counts)
	}
	item := batchItem(svc, detail.ID, "a")
	if item == nil || item.State != batch.ItemSucceeded || item.RunID == "" {
		t.Fatalf("item=%+v want succeeded with run", item)
	}
	runs := runsForBatch(svc, detail.ID)
	if len(runs) != 1 || runs[0].State != model.Released {
		t.Fatalf("runs=%+v want one released run", runs)
	}
	trace := svc.Runtime.Traces.Trace(item.RunID)
	if len(trace.Thickness) != 2 {
		t.Fatalf("thickness samples=%d want 2 (one per layer)", len(trace.Thickness))
	}
	assertResourcesClean(t, svc)
}

func TestE2E_PreheatFailureIsolatesAndContinues(t *testing.T) {
	svc := newBatchService(t)
	recipe := recipeID(t, svc)
	// The very first preheat fails; every later attempt succeeds.
	svc.Source.ConfigurePreheatFailures(1)

	detail, err := svc.CreateBatch(context.Background(), batch.CreateBatchInput{
		Items: []batch.ItemInput{
			itemInput("a", recipe, 10),
			itemInput("b", recipe, 5),      // independent
			itemInput("c", recipe, 1, "a"), // depends on failed a
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	final := waitBatch(t, svc, detail.ID, 5*time.Second)

	if item := batchItem(svc, detail.ID, "a"); item == nil || item.State != batch.ItemFailed {
		t.Fatalf("a=%+v want failed", item)
	}
	if item := batchItem(svc, detail.ID, "c"); item == nil || item.State != batch.ItemBlocked {
		t.Fatalf("c=%+v want blocked by a", item)
	}
	bItem := batchItem(svc, detail.ID, "b")
	if bItem == nil || bItem.State != batch.ItemSucceeded {
		t.Fatalf("b=%+v want succeeded on released resources", bItem)
	}
	// b must have acquired a clean carrier and reached released.
	bRun, ok := svc.Runs.Get(bItem.RunID)
	if !ok || bRun.State != model.Released {
		t.Fatalf("independent run b=%+v want released", bRun)
	}
	if final.Counts.Failed != 1 || final.Counts.Blocked != 1 || final.Counts.Succeeded != 1 {
		t.Fatalf("counts=%+v want failed=1 blocked=1 succeeded=1", final.Counts)
	}
	assertResourcesClean(t, svc)
}

func TestE2E_BusyFailureReleasesLease(t *testing.T) {
	svc := newBatchService(t)
	recipe := recipeID(t, svc)
	svc.Source.ConfigureBusy(true)

	detail, err := svc.CreateBatch(context.Background(), batch.CreateBatchInput{
		Items: []batch.ItemInput{itemInput("a", recipe, 1)},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	final := waitBatch(t, svc, detail.ID, 5*time.Second)
	if final.Counts.Failed != 1 {
		t.Fatalf("counts=%+v want 1 failed", final.Counts)
	}
	assertResourcesClean(t, svc)

	// After clearing the fault, a new batch runs on fresh leases; stale state
	// from the failed run must not affect it.
	svc.Source.ConfigureBusy(false)
	next, err := svc.CreateBatch(context.Background(), batch.CreateBatchInput{
		Items: []batch.ItemInput{itemInput("z", recipe, 1)},
	})
	if err != nil {
		t.Fatalf("create second: %v", err)
	}
	waitBatch(t, svc, next.ID, 5*time.Second)
	item := batchItem(svc, next.ID, "z")
	if item == nil || item.State != batch.ItemSucceeded {
		t.Fatalf("z=%+v want succeeded after fault cleared", item)
	}
	assertResourcesClean(t, svc)
}

func TestE2E_PauseResumeBarrier(t *testing.T) {
	svc := newBatchService(t)
	recipe := recipeID(t, svc)

	detail, err := svc.CreateBatch(context.Background(), batch.CreateBatchInput{
		Items: []batch.ItemInput{itemInput("a", recipe, 1), itemInput("b", recipe, 1)},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Raise the barrier immediately. One item may already be in flight; that is
	// the documented barrier semantics.
	if _, err := svc.PauseBatch(detail.ID); err != nil {
		t.Fatalf("pause: %v", err)
	}

	// Wait until the batch reports paused, then freeze the set of finished
	// items and confirm nothing new starts while paused.
	deadline := time.Now().Add(2 * time.Second)
	for {
		d, _ := svc.BatchDetail(detail.ID)
		if d.State == batch.Paused {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("batch never paused")
		}
		time.Sleep(2 * time.Millisecond)
	}
	settledWhilePaused := func() int {
		d, _ := svc.BatchDetail(detail.ID)
		return d.Counts.Succeeded + d.Counts.Failed
	}
	before := settledWhilePaused()
	time.Sleep(80 * time.Millisecond)
	if got := settledWhilePaused(); got != before {
		t.Fatalf("items advanced while paused: before=%d after=%d", before, got)
	}
	d, _ := svc.BatchDetail(detail.ID)
	if d.State != batch.Paused {
		t.Fatalf("state=%s want paused", d.State)
	}

	if _, err := svc.ResumeBatch(detail.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	final := waitBatch(t, svc, detail.ID, 5*time.Second)
	if final.Counts.Succeeded != 2 {
		t.Fatalf("counts=%+v want 2 succeeded after resume", final.Counts)
	}
	assertResourcesClean(t, svc)
}

func TestE2E_CreateRejectsBadGraph(t *testing.T) {
	svc := newBatchService(t)
	recipe := recipeID(t, svc)

	if _, err := svc.CreateBatch(context.Background(), batch.CreateBatchInput{
		Items: []batch.ItemInput{itemInput("a", recipe, 1, "missing")},
	}); err == nil {
		t.Fatal("missing dependency must be rejected")
	}
	if _, err := svc.CreateBatch(context.Background(), batch.CreateBatchInput{
		Items: []batch.ItemInput{itemInput("a", recipe, 1, "a")},
	}); err == nil {
		t.Fatal("self cycle must be rejected")
	}
	if _, err := svc.CreateBatch(context.Background(), batch.CreateBatchInput{
		Items: []batch.ItemInput{itemInput("a", "not-a-uuid", 1)},
	}); err == nil {
		t.Fatal("unknown recipe must be rejected")
	}
}
