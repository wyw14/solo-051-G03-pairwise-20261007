package batch

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

func TestValidate_AcceptsAcyclicGraph(t *testing.T) {
	input := CreateBatchInput{Items: []ItemInput{
		{Key: "a"}, {Key: "b", DependsOn: []string{"a"}},
		{Key: "c", DependsOn: []string{"a"}}, {Key: "d", DependsOn: []string{"b", "c"}},
	}}
	if err := Validate(input); err != nil {
		t.Fatalf("diamond should be valid: %v", err)
	}
}

func TestValidate_Cycles(t *testing.T) {
	cases := map[string]CreateBatchInput{
		"self": {Items: []ItemInput{{Key: "a", DependsOn: []string{"a"}}}},
		"pair": {Items: []ItemInput{{Key: "a", DependsOn: []string{"b"}}, {Key: "b", DependsOn: []string{"a"}}}},
		"ring": {Items: []ItemInput{
			{Key: "a", DependsOn: []string{"c"}},
			{Key: "b", DependsOn: []string{"a"}},
			{Key: "c", DependsOn: []string{"b"}},
		}},
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			err := Validate(input)
			var cycle *CycleError
			if !errors.As(err, &cycle) {
				t.Fatalf("err=%v want *CycleError", err)
			}
			if len(cycle.Keys) == 0 {
				t.Fatal("cycle error should name nodes")
			}
		})
	}
}

func TestValidate_MissingDependency(t *testing.T) {
	input := CreateBatchInput{Items: []ItemInput{
		{Key: "a"}, {Key: "b", DependsOn: []string{"x"}},
	}}
	err := Validate(input)
	var missing *MissingDependencyError
	if !errors.As(err, &missing) {
		t.Fatalf("err=%v want *MissingDependencyError", err)
	}
	if missing.Item != "b" || missing.Dependency != "x" {
		t.Fatalf("missing=(%q,%q) want (b,x)", missing.Item, missing.Dependency)
	}
}

func TestValidate_DuplicateAndBlankAndEmpty(t *testing.T) {
	if err := Validate(CreateBatchInput{}); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("empty err=%v want ErrEmptyBatch", err)
	}
	if err := Validate(CreateBatchInput{Items: []ItemInput{{Key: "a"}, {Key: "a"}}}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("dup err=%v want ErrDuplicateKey", err)
	}
	if err := Validate(CreateBatchInput{Items: []ItemInput{{Key: ""}}}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("blank key err=%v want ErrDuplicateKey", err)
	}
}

func TestGraph_TransitiveDependents(t *testing.T) {
	input := CreateBatchInput{Items: []ItemInput{
		{Key: "a"},
		{Key: "b", DependsOn: []string{"a"}},
		{Key: "c", DependsOn: []string{"a"}},
		{Key: "d", DependsOn: []string{"b", "c"}},
		{Key: "e"}, // unrelated
	}}
	built := NewBuild("batch-1", input, time.Now())
	g := GraphFromBatch(built)
	keyByID := map[model.ID]string{}
	var root model.ID
	for _, it := range built.Items {
		keyByID[it.ID] = it.Key
		if it.Key == "a" {
			root = it.ID
		}
	}
	got := map[string]bool{}
	for _, did := range g.TransitiveDependents(root) {
		got[keyByID[did]] = true
	}
	for _, want := range []string{"b", "c", "d"} {
		if !got[want] {
			t.Errorf("dependents of a missing %q: %v", want, got)
		}
	}
	if got["e"] || got["a"] {
		t.Errorf("closure must not include root or unrelated e: %v", got)
	}
}
