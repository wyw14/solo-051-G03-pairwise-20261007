package batch

import (
	"github.com/wyw14/cry-161/internal/model"
)

// Graph is the resolved dependency structure of a persisted batch, keyed by
// server item IDs. Edges point from a dependency to its dependent.
type Graph struct {
	// deps[node] is the set of items node depends on.
	deps map[model.ID]map[model.ID]struct{}
	// dependents[node] is the set of items that depend on node.
	dependents map[model.ID]map[model.ID]struct{}
	keyToID    map[string]model.ID
}

// Validate checks the raw input before a batch is created: at least one item,
// non-empty unique keys, every dependency referencing an existing key, and no
// dependency cycle (including self-dependency). It uses input positions as
// node identity, so it is independent of the server IDs assigned later.
func Validate(input CreateBatchInput) error {
	if len(input.Items) == 0 {
		return ErrEmptyBatch
	}
	indexByKey := make(map[string]int, len(input.Items))
	for i, item := range input.Items {
		if item.Key == "" {
			return ErrDuplicateKey // a blank key cannot be referenced safely
		}
		if _, dup := indexByKey[item.Key]; dup {
			return ErrDuplicateKey
		}
		indexByKey[item.Key] = i
	}
	deps := make([]map[int]struct{}, len(input.Items))
	inDegree := make([]int, len(input.Items))
	for i, item := range input.Items {
		deps[i] = map[int]struct{}{}
		seen := map[string]struct{}{}
		for _, depKey := range item.DependsOn {
			dep, ok := indexByKey[depKey]
			if !ok {
				return &MissingDependencyError{Item: item.Key, Dependency: depKey}
			}
			if _, dup := seen[depKey]; dup {
				// A duplicated dependency edge is harmless but signals a bad request.
				continue
			}
			seen[depKey] = struct{}{}
			deps[i][dep] = struct{}{}
		}
		inDegree[i] = len(deps[i])
	}

	// dependents[dep] = nodes that depend on dep.
	dependents := make([]map[int]struct{}, len(input.Items))
	for i := range dependents {
		dependents[i] = map[int]struct{}{}
	}
	for node, set := range deps {
		for dep := range set {
			dependents[dep][node] = struct{}{}
		}
	}

	// Kahn: repeatedly remove zero in-degree nodes; leftovers form a cycle.
	queue := make([]int, 0, len(input.Items))
	for i, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, i)
		}
	}
	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		for dependent := range dependents[node] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}
	if visited < len(input.Items) {
		cyclic := make([]string, 0)
		for i, degree := range inDegree {
			if degree > 0 {
				cyclic = append(cyclic, input.Items[i].Key)
			}
		}
		return &CycleError{Keys: cyclic}
	}
	return nil
}

// GraphFromBatch constructs adjacency maps for a persisted batch whose item
// IDs and keys are fixed.
func GraphFromBatch(batch *Batch) *Graph {
	g := &Graph{
		deps:       make(map[model.ID]map[model.ID]struct{}, len(batch.Items)),
		dependents: make(map[model.ID]map[model.ID]struct{}, len(batch.Items)),
		keyToID:    make(map[string]model.ID, len(batch.Items)),
	}
	for _, item := range batch.Items {
		g.keyToID[item.Key] = item.ID
	}
	for _, item := range batch.Items {
		node := item.ID
		if g.deps[node] == nil {
			g.deps[node] = map[model.ID]struct{}{}
		}
		if g.dependents[node] == nil {
			g.dependents[node] = map[model.ID]struct{}{}
		}
		for _, depKey := range item.DependsOn {
			dep := g.keyToID[depKey]
			g.deps[node][dep] = struct{}{}
			if g.dependents[dep] == nil {
				g.dependents[dep] = map[model.ID]struct{}{}
			}
			g.dependents[dep][node] = struct{}{}
		}
	}
	return g
}

// TransitiveDependents returns every item that depends, directly or
// transitively, on root. An unrelated item is never included.
func (g *Graph) TransitiveDependents(root model.ID) []model.ID {
	result := make([]model.ID, 0)
	seen := map[model.ID]struct{}{root: {}}
	queue := []model.ID{root}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		for dep := range g.dependents[node] {
			if _, ok := seen[dep]; ok {
				continue
			}
			seen[dep] = struct{}{}
			result = append(result, dep)
			queue = append(queue, dep)
		}
	}
	return result
}

// allDepsSucceeded reports whether every dependency of node is in succeeded.
func (g *Graph) allDepsSucceeded(node model.ID, stateOf func(model.ID) ItemState) bool {
	for dep := range g.deps[node] {
		if stateOf(dep) != ItemSucceeded {
			return false
		}
	}
	return true
}

// anyDepFailed reports whether any dependency reached a terminal non-success
// state (failed, blocked or cancelled), which blocks node in turn.
func (g *Graph) anyDepFailed(node model.ID, stateOf func(model.ID) ItemState) bool {
	for dep := range g.deps[node] {
		switch stateOf(dep) {
		case ItemFailed, ItemBlocked, ItemCancelled:
			return true
		}
	}
	return false
}
