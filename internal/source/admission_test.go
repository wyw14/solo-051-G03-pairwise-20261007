package source

import (
	"context"
	"testing"
	"time"
)

func TestAdmission_LeaseReleaseOnce(t *testing.T) {
	a := NewAdmission(10)
	lease, err := a.AcquireLease(context.Background(), 4)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if a.Used() != 4 {
		t.Fatalf("used=%d want 4", a.Used())
	}
	if !a.Release(lease) {
		t.Fatal("first release should succeed")
	}
	if a.Release(lease) {
		t.Fatal("duplicate release of same lease must be rejected")
	}
	if a.Used() != 0 {
		t.Fatalf("used=%d want 0", a.Used())
	}
}

func TestAdmission_LateReleaseDoesNotDeductNewLease(t *testing.T) {
	a := NewAdmission(10)
	leaseA, err := a.AcquireLease(context.Background(), 4)
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := a.AcquireLease(context.Background(), 6)
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if a.Used() != 10 {
		t.Fatalf("used=%d want 10", a.Used())
	}
	// A finishes and releases normally.
	if !a.Release(leaseA) {
		t.Fatal("release A should succeed")
	}
	if a.Used() != 6 {
		t.Fatalf("used=%d want 6 (only B live)", a.Used())
	}
	// A late/duplicated release from the old run arrives after B is running.
	if a.Release(leaseA) {
		t.Fatal("stale release A must be rejected")
	}
	if a.Used() != 6 {
		t.Fatalf("stale release changed used=%d want 6", a.Used())
	}
	if !a.Release(leaseB) {
		t.Fatal("release B should succeed")
	}
	if a.Used() != 0 {
		t.Fatalf("used=%d want 0", a.Used())
	}
}

func TestAdmission_UsedEqualsSumOfLive(t *testing.T) {
	cases := []int{2, 3, 5}
	a := NewAdmission(100)
	leases := make([]Lease, 0, len(cases))
	for _, w := range cases {
		l, err := a.AcquireLease(context.Background(), w)
		if err != nil {
			t.Fatalf("acquire %d: %v", w, err)
		}
		leases = append(leases, l)
	}
	if want := 10; a.Used() != want {
		t.Fatalf("used=%d want %d", a.Used(), want)
	}
	// Release out of order: middle, first, last.
	order := []int{1, 0, 2}
	remaining := 10
	for _, i := range order {
		if !a.Release(leases[i]) {
			t.Fatalf("release %d should succeed", i)
		}
		remaining -= cases[i]
		if a.Used() != remaining {
			t.Fatalf("after release %d used=%d want %d", i, a.Used(), remaining)
		}
	}
}

func TestAdmission_AcquireLeaseRejectsWeight(t *testing.T) {
	a := NewAdmission(10)
	if _, err := a.AcquireLease(context.Background(), 0); err == nil {
		t.Fatal("weight 0 must be rejected")
	}
}

func TestAdmission_AcquireWaitsForCapacity(t *testing.T) {
	a := NewAdmission(5)
	holder, err := a.AcquireLease(context.Background(), 5)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	got := make(chan Lease, 1)
	errs := make(chan error, 1)
	go func() {
		l, err := a.AcquireLease(context.Background(), 5)
		if err != nil {
			errs <- err
			return
		}
		got <- l
	}()
	select {
	case <-got:
		t.Fatal("second acquire must block while capacity is exhausted")
	case <-time.After(20 * time.Millisecond):
	}
	a.Release(holder)
	select {
	case l := <-got:
		a.Release(l)
	case err := <-errs:
		t.Fatalf("blocked acquire failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("blocked acquire did not complete after release")
	}
}

func TestController_StopIdempotent(t *testing.T) {
	c := NewController(100)
	ctx := context.Background()
	if err := c.Start(ctx, 3); err != nil {
		t.Fatalf("start: %v", err)
	}
	if c.AdmissionUsed() != 3 {
		t.Fatalf("used=%d want 3", c.AdmissionUsed())
	}
	if !c.Stop() {
		t.Fatal("first stop should release the live lease")
	}
	if c.Stop() {
		t.Fatal("second stop must be a no-op")
	}
	if c.AdmissionUsed() != 0 {
		t.Fatalf("used=%d want 0", c.AdmissionUsed())
	}
}

func TestController_StartFailureRollsBackLease(t *testing.T) {
	c := NewController(100)
	c.ConfigureBusy(true)
	if err := c.Start(context.Background(), 3); err == nil {
		t.Fatal("busy adapter should fail Start")
	}
	if c.AdmissionUsed() != 0 {
		t.Fatalf("used=%d want 0 after failed start", c.AdmissionUsed())
	}
	// A later start with the adapter free must acquire a fresh lease cleanly.
	c.ConfigureBusy(false)
	if err := c.Start(context.Background(), 3); err != nil {
		t.Fatalf("second start: %v", err)
	}
	if c.AdmissionUsed() != 3 {
		t.Fatalf("used=%d want 3", c.AdmissionUsed())
	}
	c.Stop()
}
