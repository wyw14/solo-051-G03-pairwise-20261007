package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/cry-161/internal/batch"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
)

// batchJournalObserver mirrors orchestrator state changes into the event log.
type batchJournalObserver struct {
	svc *Service
}

func (o *batchJournalObserver) BatchChanged(batchID model.ID, from, to batch.BatchState) {
	_ = o.svc.Journal.Append(journal.BatchStateChanged(batchID, string(from), string(to)))
}

func (o *batchJournalObserver) ItemBlocked(batchID, itemID model.ID, reason string) {
	_ = o.svc.Journal.Append(journal.ItemBlocked(batchID, itemID, reason))
}

// CreateBatch validates the request, pre-checks every recipe, persists a
// queued batch and immediately activates it for background execution.
func (s *Service) CreateBatch(ctx context.Context, input batch.CreateBatchInput) (*batch.Detail, error) {
	if err := batch.Validate(input); err != nil {
		return nil, err
	}
	for _, item := range input.Items {
		if _, err := s.Recipes.Active(item.RecipeID); err != nil {
			return nil, fmt.Errorf("item %q: recipe %s: %w", item.Key, item.RecipeID, err)
		}
	}
	created := batch.NewBuild(model.NewID(), input, time.Now().UTC())
	s.Batches.Add(created)
	_ = s.Journal.Append(journal.BatchCreated(created.ID, created.Name, len(created.Items)))
	if err := s.Exec.Spawn(created.ID); err != nil {
		return nil, err
	}
	return s.BatchDetail(created.ID)
}

func (s *Service) PauseBatch(id model.ID) (*batch.Detail, error) {
	if err := s.Exec.Pause(id); err != nil {
		return nil, err
	}
	return s.BatchDetail(id)
}

func (s *Service) ResumeBatch(id model.ID) (*batch.Detail, error) {
	if err := s.Exec.Resume(id); err != nil {
		return nil, err
	}
	return s.BatchDetail(id)
}

func (s *Service) CancelBatch(id model.ID) (*batch.Detail, error) {
	if err := s.Exec.Cancel(id); err != nil {
		return nil, err
	}
	return s.BatchDetail(id)
}

func (s *Service) BatchDetail(id model.ID) (*batch.Detail, error) {
	batchValue, ok := s.Batches.Get(id)
	if !ok {
		return nil, batch.ErrBatchNotFound
	}
	return &batch.Detail{Batch: batchValue, Counts: batchValue.Counts()}, nil
}

func (s *Service) BatchItems(id model.ID) ([]*batch.Item, error) {
	batchValue, ok := s.Batches.Get(id)
	if !ok {
		return nil, batch.ErrBatchNotFound
	}
	return batchValue.Items, nil
}

func (s *Service) BatchesList() []*batch.Detail {
	batches := s.Batches.List()
	details := make([]*batch.Detail, 0, len(batches))
	for _, batchValue := range batches {
		details = append(details, &batch.Detail{Batch: batchValue, Counts: batchValue.Counts()})
	}
	return details
}
