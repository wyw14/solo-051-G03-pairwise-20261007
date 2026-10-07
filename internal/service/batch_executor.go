package service

import (
	"context"
	"github.com/wyw14/cry-161/internal/batch"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/run"
	"sort"
)

// BatchExecutor adapts the batch orchestrator to the existing run, recipe,
// source and chamber services.
type BatchExecutor struct {
	svc     *Service
	runtime *Runtime
}

func (s *Service) BatchExecutor(runtime *Runtime) batch.RunExecutor {
	return &BatchExecutor{svc: s, runtime: runtime}
}

func (e *BatchExecutor) BeginRun(ctx context.Context, b *batch.Batch, spec batch.RunSpec) (model.ID, error) {
	coordinator := &run.Coordinator{Registry: e.svc.Runs, Recipes: e.svc.Recipes, Carrier: e.svc.Carrier, Chamber: e.svc.Chamber, Vacuum: e.svc.Vacuum, Journal: e.svc.Journal}
	started, err := coordinator.Start(ctx, b.Name, spec.RecipeID)
	if err != nil {
		return "", err
	}
	if err := run.Advance(e.svc.Runs, started.ID, model.Conditioning); err != nil {
		return "", err
	}
	if err := run.Advance(e.svc.Runs, started.ID, model.Coating); err != nil {
		return "", err
	}
	return started.ID, nil
}

func (e *BatchExecutor) ExecuteLayer(ctx context.Context, runID model.ID, layer model.Layer) error {
	return e.svc.RunLayer(ctx, e.runtime, runID, layer)
}

func (e *BatchExecutor) CompleteRun(ctx context.Context, runID model.ID) error {
	e.svc.Source.Stop()
	e.svc.Chamber.SetSourcePower(0)
	return run.Complete(e.svc.Runs, runID)
}

func (e *BatchExecutor) FailRun(ctx context.Context, runID model.ID, cause error) {
	e.svc.Source.Stop()
	e.svc.Chamber.SetSourcePower(0)
	e.svc.Runs.UpdateError(runID, cause)
	incident := e.runtime.Incidents.Raise(runID, "critical", cause.Error())
	_ = e.svc.Journal.Append(journal.IncidentRaised(incident))
}

func (e *BatchExecutor) RecipeLayers(recipeID model.ID) ([]model.Layer, error) {
	recipe, err := e.svc.Recipes.Active(recipeID)
	if err != nil {
		return nil, err
	}
	layers := make([]model.Layer, 0, len(recipe.Layers))
	for _, layer := range recipe.Layers {
		layers = append(layers, layer)
	}
	sort.Slice(layers, func(i, j int) bool { return layers[i].Number < layers[j].Number })
	return layers, nil
}
