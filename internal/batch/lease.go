package batch

import (
	"context"
	"fmt"
	"github.com/wyw14/cry-161/internal/chamber"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/source"
	"github.com/wyw14/cry-161/internal/substrate"
	"sync"
)

// ErrStaleResources is returned when a run is about to start while physical
// resources from a previous run (power quota, carrier, chamber) are still
// held. The orchestrator treats this as an isolation fault and refuses to
// dispatch instead of letting a run reuse dirty resources.
var ErrStaleResources = fmt.Errorf("stale chamber, carrier or power quota")

// Lease ties one run to the physical resources it may use: chamber
// occupancy, the substrate carrier and a source power reservation. Every
// lease carries the pool epoch; Invalidate bumps the epoch so leases from
// earlier epochs can never be used by a later run.
type Lease struct {
	BatchID  model.ID
	Batch    string
	Chamber  string
	Weight   int
	epoch    uint64
	released bool
}

type LeasePool struct {
	mu        sync.Mutex
	epoch     uint64
	chambers  map[string]model.ID
	carriers  map[string]bool
	reserved  int
	chamber   *chamber.Chamber
	carrier   *substrate.Carrier
	admission *source.Admission
}

func NewLeasePool(ch *chamber.Chamber, carrier *substrate.Carrier, admission *source.Admission) *LeasePool {
	return &LeasePool{chambers: map[string]model.ID{}, carriers: map[string]bool{}, chamber: ch, carrier: carrier, admission: admission}
}

func (p *LeasePool) Epoch() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.epoch
}

// QuotaClear reports whether every power quota has been returned. The
// orchestrator asserts this before dispatching a run.
func (p *LeasePool) QuotaClear() bool { return p.admission.Used() == 0 && !p.carrier.Loaded() }

func (p *LeasePool) Acquire(ctx context.Context, batchID model.ID, batchName, chamberName string, weight int) (*Lease, error) {
	if weight <= 0 {
		return nil, fmt.Errorf("invalid power weight")
	}
	p.mu.Lock()
	if _, occupied := p.chambers[chamberName]; occupied {
		p.mu.Unlock()
		return nil, fmt.Errorf("chamber %s is occupied", chamberName)
	}
	if p.carrier.Loaded() || p.admission.Used() != 0 {
		p.mu.Unlock()
		return nil, ErrStaleResources
	}
	p.chambers[chamberName] = batchID
	p.carriers[batchName] = true
	p.mu.Unlock()
	if err := p.admission.Acquire(ctx, weight); err != nil {
		p.mu.Lock()
		delete(p.chambers, chamberName)
		delete(p.carriers, batchName)
		p.mu.Unlock()
		return nil, err
	}
	p.mu.Lock()
	p.reserved += weight
	lease := &Lease{BatchID: batchID, Batch: batchName, Chamber: chamberName, Weight: weight, epoch: p.epoch}
	p.mu.Unlock()
	return lease, nil
}

// Valid reports whether the lease still belongs to the current epoch and has
// not been released. A run checks this between layers; after a failure
// invalidation every older lease reports false.
func (p *LeasePool) Valid(lease *Lease) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if lease.released || lease.epoch != p.epoch {
		return false
	}
	owner, ok := p.chambers[lease.Chamber]
	return ok && owner == lease.BatchID
}

func (p *LeasePool) Release(lease *Lease) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if lease.released {
		return
	}
	lease.released = true
	p.releaseLocked(lease)
}

func (p *LeasePool) releaseLocked(lease *Lease) {
	if lease.epoch != p.epoch {
		// Stale lease: its resources were already force-released by
		// Invalidate. Releasing again would steal quota from a newer lease.
		return
	}
	if owner, ok := p.chambers[lease.Chamber]; ok && owner == lease.BatchID {
		delete(p.chambers, lease.Chamber)
	}
	delete(p.carriers, lease.Batch)
	p.carrier.Unload(lease.Batch)
	p.admission.Release(lease.Weight)
	p.reserved -= lease.Weight
	if p.reserved < 0 {
		p.reserved = 0
	}
}

// Invalidate force-releases every physical resource and bumps the epoch.
// Called on any run failure so a later run can never observe a stale
// chamber, carrier or power quota from the failed one.
func (p *LeasePool) Invalidate() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.epoch++
	for name := range p.carriers {
		p.carrier.Unload(name)
	}
	p.carriers = map[string]bool{}
	p.chambers = map[string]model.ID{}
	if p.reserved > 0 {
		p.admission.Release(p.reserved)
		p.reserved = 0
	}
	if p.chamber != nil {
		p.chamber.SetSourcePower(0)
	}
	return p.epoch
}
