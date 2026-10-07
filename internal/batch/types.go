package batch

import (
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

type BatchState string

const (
	Queued    BatchState = "queued"
	Active    BatchState = "active"
	Paused    BatchState = "paused"
	Completed BatchState = "completed"
	Cancelled BatchState = "cancelled"
)

type ItemState string

const (
	ItemPending   ItemState = "pending"
	ItemReady     ItemState = "ready"
	ItemRunning   ItemState = "running"
	ItemSucceeded ItemState = "succeeded"
	ItemFailed    ItemState = "failed"
	ItemBlocked   ItemState = "blocked"
	ItemCancelled ItemState = "cancelled"
)

// ItemStateOrder is used by CanTransitionItem.
func itemTerminal(state ItemState) bool {
	switch state {
	case ItemSucceeded, ItemFailed, ItemBlocked, ItemCancelled:
		return true
	default:
		return false
	}
}

// ItemInput is one run requested inside a batch. Key is the client-assigned
// identifier inside the batch that DependsOn entries reference.
type ItemInput struct {
	Key       string   `json:"key"`
	RecipeID  model.ID `json:"recipe_id"`
	Priority  int      `json:"priority"`
	DependsOn []string `json:"depends_on"`
}

// CreateBatchInput is the POST /api/batches payload.
type CreateBatchInput struct {
	Name  string      `json:"name"`
	Items []ItemInput `json:"items"`
}

// Item is a single run slot and its scheduling state.
type Item struct {
	ID         model.ID  `json:"id"`
	Key        string    `json:"key"`
	RecipeID   model.ID  `json:"recipe_id"`
	Priority   int       `json:"priority"`
	DependsOn  []string  `json:"depends_on"`
	State      ItemState `json:"state"`
	RunID      model.ID  `json:"run_id,omitempty"`
	Error      string    `json:"error,omitempty"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// Batch groups ordered, interdependent runs into one pausable production unit.
type Batch struct {
	ID        model.ID   `json:"id"`
	Name      string     `json:"name"`
	State     BatchState `json:"state"`
	Items     []*Item    `json:"items"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// ItemCounts summarizes items by state and is returned with batch details.
type ItemCounts struct {
	Pending   int `json:"pending"`
	Ready     int `json:"ready"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Blocked   int `json:"blocked"`
	Cancelled int `json:"cancelled"`
}

func (b *Batch) Counts() ItemCounts {
	var counts ItemCounts
	for _, item := range b.Items {
		switch item.State {
		case ItemPending:
			counts.Pending++
		case ItemReady:
			counts.Ready++
		case ItemRunning:
			counts.Running++
		case ItemSucceeded:
			counts.Succeeded++
		case ItemFailed:
			counts.Failed++
		case ItemBlocked:
			counts.Blocked++
		case ItemCancelled:
			counts.Cancelled++
		}
	}
	return counts
}

// Detail is the API representation of a batch: the batch plus its counts.
type Detail struct {
	*Batch
	Counts ItemCounts `json:"counts"`
}

// NewBuild assembles a queued batch from validated input, assigning server IDs.
func NewBuild(id model.ID, input CreateBatchInput, now time.Time) *Batch {
	batch := &Batch{ID: id, Name: input.Name, State: Queued, CreatedAt: now, UpdatedAt: now}
	for _, in := range input.Items {
		deps := append([]string(nil), in.DependsOn...)
		batch.Items = append(batch.Items, &Item{
			ID:         model.NewID(),
			Key:        in.Key,
			RecipeID:   in.RecipeID,
			Priority:   in.Priority,
			DependsOn:  deps,
			State:      ItemPending,
			EnqueuedAt: now,
		})
	}
	return batch
}

func cloneBatch(in *Batch) *Batch {
	out := *in
	out.Items = make([]*Item, len(in.Items))
	for i, item := range in.Items {
		copyItem := *item
		copyItem.DependsOn = append([]string(nil), item.DependsOn...)
		out.Items[i] = &copyItem
	}
	return &out
}
