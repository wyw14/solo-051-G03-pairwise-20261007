package batch

import (
	"testing"
	"time"
)

func TestDispatcher_PriorityThenFIFO(t *testing.T) {
	base := time.Now().UTC()
	mk := func(key string, priority int, offset time.Duration) *Item {
		return &Item{Key: key, Priority: priority, EnqueuedAt: base.Add(offset)}
	}
	d := NewDispatcher(3)
	d.Add(mk("t3", 1, 2*time.Millisecond))
	d.Add(mk("t1", 5, 0))
	d.Add(mk("t2", 5, 1*time.Millisecond))
	d.Sort()

	want := []string{"t1", "t2", "t3"}
	for _, key := range want {
		it := d.Pop()
		if it == nil {
			t.Fatalf("expected %s, got nil", key)
		}
		if it.Key != key {
			t.Fatalf("popped %s want %s", it.Key, key)
		}
	}
	if d.Pop() != nil {
		t.Fatal("queue should be empty")
	}
}
