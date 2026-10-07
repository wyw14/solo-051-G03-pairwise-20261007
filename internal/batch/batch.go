package batch

import (
	"fmt"
	"github.com/wyw14/cry-161/internal/model"
	"time"
)

type State string

const (
	Pending   State = "pending"
	Running   State = "running"
	Paused    State = "paused"
	Completed State = "completed"
	Failed    State = "failed"
	Aborted   State = "aborted"
	Blocked   State = "blocked"
)

// RunSpec describes one run inside a batch: which recipe to execute.
type RunSpec struct {
	RecipeID model.ID `json:"recipe_id"`
}

// RunOutcome records how one run of the batch ended.
type RunOutcome struct {
	RunID    model.ID `json:"run_id"`
	RecipeID model.ID `json:"recipe_id"`
	Status   string   `json:"status"`
	Error    string   `json:"error,omitempty"`
}

type Batch struct {
	ID        model.ID     `json:"id"`
	Name      string       `json:"name"`
	Priority  int          `json:"priority"`
	DependsOn []model.ID   `json:"depends_on,omitempty"`
	Specs     []RunSpec    `json:"runs"`
	State     State        `json:"state"`
	Outcomes  []RunOutcome `json:"outcomes,omitempty"`
	Error     string       `json:"error,omitempty"`
	CreatedAt time.Time    `json:"created_at"`
	UpdatedAt time.Time    `json:"updated_at"`
	// resumeState is where Pause interrupted the batch; Resume returns to it.
	resumeState State
}

func (b *Batch) Validate() error {
	if b.Name == "" {
		return fmt.Errorf("batch name is required")
	}
	if len(b.Specs) == 0 {
		return fmt.Errorf("batch requires at least one run")
	}
	for index, spec := range b.Specs {
		if spec.RecipeID == "" {
			return fmt.Errorf("run %d: recipe is required", index)
		}
	}
	for _, dep := range b.DependsOn {
		if dep == b.ID {
			return fmt.Errorf("batch cannot depend on itself")
		}
	}
	return nil
}

func (b *Batch) terminal() bool {
	switch b.State {
	case Completed, Failed, Aborted, Blocked:
		return true
	}
	return false
}
