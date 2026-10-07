package chamber

import (
	"testing"

	"github.com/wyw14/cry-161/internal/model"
)

func TestOccupancy_ExclusiveAcquire(t *testing.T) {
	var o Occupancy
	run1 := model.ID("run-1")
	run2 := model.ID("run-2")

	token1, ok := o.Acquire(run1)
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	if holder, _ := o.Current(); holder != run1 {
		t.Fatalf("holder=%q want run-1", holder)
	}
	if _, ok := o.Acquire(run2); ok {
		t.Fatal("second acquire while occupied must fail")
	}
	// Releasing with the wrong holder is rejected.
	if o.Release(run2, token1) {
		t.Fatal("release with wrong holder must fail")
	}
	// Releasing with the right holder but a stale/wrong token is rejected.
	if o.Release(run1, token1+1) {
		t.Fatal("release with wrong token must fail")
	}
	if !o.Release(run1, token1) {
		t.Fatal("valid release should succeed")
	}
	if holder, _ := o.Current(); holder != "" {
		t.Fatalf("holder=%q want empty after release", holder)
	}
	if _, ok := o.Acquire(run2); !ok {
		t.Fatal("chamber must be acquirable again after release")
	}
}

func TestOccupancy_StaleTokenRejected(t *testing.T) {
	var o Occupancy
	run1 := model.ID("run-1")
	run2 := model.ID("run-2")

	token1, _ := o.Acquire(run1)
	if !o.Release(run1, token1) {
		t.Fatal("release run1 should succeed")
	}
	token2, ok := o.Acquire(run2)
	if !ok {
		t.Fatal("acquire run2 should succeed")
	}
	if token1 == token2 {
		t.Fatal("epochs must differ across grants")
	}
	// Run1's late release must not eject run2.
	if o.Release(run1, token1) {
		t.Fatal("stale release from run1 must be rejected")
	}
	if holder, token := o.Current(); holder != run2 || token != token2 {
		t.Fatalf("current=(%q,%d) want (run-2,%d)", holder, token, token2)
	}
}

func TestChamber_Occupant(t *testing.T) {
	c := New("coat-01")
	if c.Occupant() != "" {
		t.Fatal("new chamber should be free")
	}
	runID := model.ID("run-x")
	token, ok := c.Acquire(runID)
	if !ok {
		t.Fatal("acquire should succeed")
	}
	if c.Occupant() != runID {
		t.Fatalf("occupant=%q want run-x", c.Occupant())
	}
	if !c.Release(runID, token) {
		t.Fatal("release should succeed")
	}
	if c.Occupant() != "" {
		t.Fatal("chamber should be free after release")
	}
}
