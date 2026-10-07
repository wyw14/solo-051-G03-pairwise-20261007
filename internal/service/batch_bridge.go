package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-161/internal/batch"
	"github.com/wyw14/cry-161/internal/chamber"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/recipe"
	"github.com/wyw14/cry-161/internal/run"
	"github.com/wyw14/cry-161/internal/substrate"
)

// runLeases records the fencing credentials a started run holds so cleanup can
// release exactly what that run acquired.
type runLeases struct {
	chamberToken uint64
	carrier      substrate.Token
}

// batchBridge adapts the orchestrator's Hardware port onto the real
// controllers. It owns the per-run lease bookkeeping that guarantees symmetric
// release of chamber, carrier and power quota.
type batchBridge struct {
	svc     *Service
	runtime *Runtime

	mu     sync.Mutex
	leases map[model.ID]*runLeases
}

func newBatchBridge(svc *Service, runtime *Runtime) *batchBridge {
	return &batchBridge{svc: svc, runtime: runtime, leases: map[model.ID]*runLeases{}}
}

// StartRun reserves the chamber and carrier, creates the run, pumps down and
// brings the run to conditioning. On failure before the run is fully started
// it releases every resource it took itself and returns an empty runID, so the
// caller skips Cleanup; a non-empty runID always has a matching lease entry.
func (b *batchBridge) StartRun(ctx context.Context, batchID model.ID, item batch.Item) (model.ID, error) {
	s := b.svc
	runID := model.NewID()

	chamberToken, ok := s.Chamber.Acquire(runID)
	if !ok {
		return "", fmt.Errorf("chamber occupied by run %s", s.Chamber.Occupant())
	}
	carrierToken, err := s.Carrier.LoadFor(string(batchID), runID)
	if err != nil {
		s.Chamber.Release(runID, chamberToken)
		return "", fmt.Errorf("load carrier: %w", err)
	}

	b.mu.Lock()
	b.leases[runID] = &runLeases{chamberToken: chamberToken, carrier: carrierToken}
	b.mu.Unlock()
	abort := func(cause error) (model.ID, error) {
		b.releaseLeases(runID)
		s.Runs.UpdateError(runID, cause)
		return "", cause
	}

	created := &model.Run{ID: runID, Batch: string(batchID), RecipeID: item.RecipeID, State: model.Loading, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	s.Runs.Add(created)
	if !s.Runs.Transition(runID, model.Pumping) {
		return abort(fmt.Errorf("cannot enter pumping"))
	}
	if err := s.Vacuum.Start(ctx); err != nil {
		return abort(fmt.Errorf("vacuum start: %w", err))
	}
	s.Chamber.SetVacuum(s.Vacuum.Pressure(), s.Vacuum.Stable())
	s.Chamber.SetInterlock("vacuum", true)

	if !s.Runs.Transition(runID, model.Conditioning) {
		return abort(fmt.Errorf("cannot enter conditioning"))
	}
	started, _ := s.Runs.Get(runID)
	_ = s.Journal.Append(journal.RunStarted(*started))
	return runID, nil
}

// RunLayers compiles the run's recipe and coats every layer in order. Only the
// first layer transitions into coating; later layers run while already in the
// coating state, matching interlock.Permit and the linear CanAdvance rules.
func (b *batchBridge) RunLayers(ctx context.Context, runID model.ID) error {
	s := b.svc
	current, ok := s.Runs.Get(runID)
	if !ok {
		return fmt.Errorf("run not found")
	}
	compiled, err := s.Recipes.Active(current.RecipeID)
	if err != nil {
		return fmt.Errorf("load recipe: %w", err)
	}
	plan, err := recipe.Compile(compiled)
	if err != nil {
		return fmt.Errorf("compile recipe: %w", err)
	}

	starter := &run.Starter{Registry: s.Runs, Vacuum: s.Vacuum}
	for index, layer := range plan.Layers {
		if index == 0 {
			if err := starter.StartCoating(ctx, runID); err != nil {
				s.Runs.UpdateError(runID, err)
				return err
			}
		}
		if err := s.RunLayer(ctx, b.runtime, runID, layer); err != nil {
			return err // RunLayer already marked the run failed and raised an incident
		}
		// Return the layer's power quota immediately so the next layer acquires
		// a fresh, fenced lease and used never accumulates across layers.
		s.Source.Stop()
		s.Runs.SetLayer(runID, layer.Number)
	}
	return nil
}

// FinishRun cools and vents the run and evaluates the automatic release. A run
// that cannot be released (missing evidence or a critical incident) is treated
// as failed, which routes it through the same resource cleanup as any failure.
func (b *batchBridge) FinishRun(ctx context.Context, runID model.ID) error {
	s := b.svc
	if err := run.Complete(s.Runs, runID); err != nil {
		s.Runs.UpdateError(runID, err)
		return fmt.Errorf("cool and vent: %w", err)
	}
	record, err := s.AutoRelease(runID)
	if err != nil {
		s.Runs.UpdateError(runID, err)
		return err
	}
	if !s.Runs.Transition(runID, model.Released) {
		err := fmt.Errorf("cannot release run")
		s.Runs.UpdateError(runID, err)
		return err
	}
	_ = record
	return nil
}

// Cleanup is the single exit path for every started run. Each step is
// idempotent and fence-checked, so a late or duplicated call never touches
// another run's chamber, carrier or power quota.
func (b *batchBridge) Cleanup(ctx context.Context, runID model.ID) {
	s := b.svc
	leases := b.releaseLeases(runID)
	if leases == nil {
		return
	}

	// 1. Make the hardware safe first: shutter closed, source power zero.
	_ = b.runtime.Shutter.Close(ctx)
	s.Source.Stop()
	s.Chamber.SetSourcePower(0)

	// 2. Wait for the chamber to cool at zero power.
	if err := chamber.DefaultCooldownPolicy().Wait(ctx, s.Chamber); err != nil {
		incident := b.runtime.Incidents.Raise(runID, "warning", "cooldown interrupted during cleanup: "+err.Error())
		_ = s.Journal.Append(journal.IncidentRaised(incident))
	}

	// 3. Vent and stop pumping.
	if err := chamber.Vent(s.Chamber, s.Vacuum, ctx); err != nil {
		incident := b.runtime.Incidents.Raise(runID, "warning", "vent interrupted during cleanup: "+err.Error())
		_ = s.Journal.Append(journal.IncidentRaised(incident))
	}
	s.Vacuum.Stop()

	// 4. Release occupancy only with the matching fencing tokens.
	s.Chamber.Release(runID, leases.chamberToken)
	s.Carrier.Unload(leases.carrier)

	incident := b.runtime.Incidents.Raise(runID, "warning", "run resources released to clean state")
	_ = s.Journal.Append(journal.IncidentRaised(incident))
	_ = s.Journal.Append(journal.RunRecovered(runID))
}

// releaseLeases removes and returns the run's lease record, or nil when the
// run never held leases or already released them.
func (b *batchBridge) releaseLeases(runID model.ID) *runLeases {
	b.mu.Lock()
	defer b.mu.Unlock()
	leases := b.leases[runID]
	delete(b.leases, runID)
	return leases
}
