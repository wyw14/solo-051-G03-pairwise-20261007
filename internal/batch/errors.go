package batch

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrBatchNotFound     = errors.New("batch not found")
	ErrInvalidTransition = errors.New("invalid batch state transition")
	ErrEmptyBatch        = errors.New("batch must contain at least one item")
	ErrDuplicateKey      = errors.New("duplicate item key")
)

// CycleError describes a dependency cycle. Keys lists the nodes that never
// reached zero in-degree during topological sorting.
type CycleError struct {
	Keys []string
}

func (e *CycleError) Error() string {
	return fmt.Sprintf("dependency cycle detected among items: %s", strings.Join(e.Keys, ", "))
}

// MissingDependencyError reports an item that depends on a key absent from the
// batch.
type MissingDependencyError struct {
	Item       string
	Dependency string
}

func (e *MissingDependencyError) Error() string {
	return fmt.Sprintf("item %q depends on unknown item %q", e.Item, e.Dependency)
}
