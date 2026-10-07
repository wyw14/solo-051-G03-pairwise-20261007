package service

import (
	"context"
	"fmt"
	"github.com/wyw14/cry-161/internal/batch"
	"github.com/wyw14/cry-161/internal/chamber"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/recipe"
	"github.com/wyw14/cry-161/internal/rotation"
	"github.com/wyw14/cry-161/internal/run"
	"github.com/wyw14/cry-161/internal/source"
	"github.com/wyw14/cry-161/internal/substrate"
	"github.com/wyw14/cry-161/internal/vacuum"
	"os"
)

type Service struct {
	Runs     *run.Registry
	Recipes  *recipe.Store
	Drafts   *recipe.DraftBook
	Carrier  *substrate.Carrier
	Chamber  *chamber.Chamber
	Vacuum   *vacuum.Controller
	Source   *source.Controller
	Rotation *rotation.Controller
	Journal  *journal.Store
	Runtime  *Runtime
	Batches  *batch.Store
	Exec     *batch.Executor
}

func New() *Service {
	recipes := recipe.NewStore()
	id := model.NewID()
	recipes.Publish(&model.Recipe{ID: id, Name: "AR coating", Revision: 1, Layers: map[int]model.Layer{1: {Number: 1, Material: "SiO2", TargetNanometers: 100, RateNanometers: 2}, 2: {Number: 2, Material: "TiO2", TargetNanometers: 60, RateNanometers: 3}}})
	dir := os.Getenv("OPTICCOAT_DATA")
	if dir == "" {
		dir = "data"
	}
	s := &Service{Runs: run.NewRegistry(), Recipes: recipes, Drafts: recipe.NewDraftBook(), Carrier: substrate.NewCarrier(), Chamber: chamber.New("coat-01"), Vacuum: vacuum.NewController(), Source: source.NewController(100), Rotation: rotation.NewController(), Journal: journal.New(dir), Runtime: NewRuntime(), Batches: batch.NewStore()}
	bridge := newBatchBridge(s, s.Runtime)
	s.Exec = batch.NewExecutor(s.Batches, bridge, batch.WithObserver(&batchJournalObserver{svc: s}))
	return s
}

// StartExecutor binds background batch execution to the process lifetime ctx.
func (s *Service) StartExecutor(ctx context.Context) { s.Exec.Start(ctx) }

func (s *Service) RecipesList() []*model.Recipe { return s.Recipes.List() }
func (s *Service) RunsList() []*model.Run       { return s.Runs.List() }
func (s *Service) Start(ctx context.Context, batch string, recipeID model.ID) (*model.Run, error) {
	coordinator := &run.Coordinator{Registry: s.Runs, Recipes: s.Recipes, Carrier: s.Carrier, Chamber: s.Chamber, Vacuum: s.Vacuum, Journal: s.Journal}
	return coordinator.Start(ctx, batch, recipeID)
}
func (s *Service) UpdateDraft(id model.ID, layer model.Layer) error {
	if !s.Drafts.UpdateLayer(id, layer) {
		return fmt.Errorf("draft not found")
	}
	return nil
}
