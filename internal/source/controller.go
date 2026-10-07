package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/wyw14/cry-161/internal/model"
)

type Controller struct {
	mu        sync.Mutex
	admission *Admission
	heater    *Heater
	adapter   *Adapter
	power     float64
	lease     Lease
}

func NewController(capacity int) *Controller {
	return &Controller{admission: NewAdmission(capacity), heater: &Heater{}, adapter: NewAdapter()}
}

func (c *Controller) Start(ctx context.Context, power float64) error {
	weight := int(power)
	lease, err := c.admission.AcquireLease(ctx, weight)
	if err != nil {
		return err
	}
	if err := c.adapter.Open(); err != nil {
		c.admission.Release(lease)
		return err
	}
	if err := c.heater.Preheat(ctx); err != nil {
		c.admission.Release(lease)
		return err
	}
	c.mu.Lock()
	c.power = power
	c.lease = lease
	c.mu.Unlock()
	return nil
}

// Stop releases the currently held power lease. It is idempotent and fenced:
// a second or late Stop no longer holds the lease and changes nothing, so it
// can never deduct quota that a later run has acquired. It reports whether a
// live lease was actually released.
func (c *Controller) Stop() bool {
	c.mu.Lock()
	lease := c.lease
	c.lease = Lease{}
	power := c.power
	c.power = 0
	c.mu.Unlock()
	if power <= 0 {
		return false
	}
	return c.admission.Release(lease)
}

func (c *Controller) Power() float64                 { c.mu.Lock(); defer c.mu.Unlock(); return c.power }
func (c *Controller) AdmissionUsed() int             { return c.admission.Used() }
func (c *Controller) ConfigureBusy(value bool)       { c.adapter.SetBusy(value) }
func (c *Controller) ConfigurePreheatFailures(n int) { c.heater.StableAfter = n; c.heater.Attempts = 0 }

func (c *Controller) Layer(ctx context.Context, layer model.Layer) error {
	if err := c.Start(ctx, layer.RateNanometers); err != nil {
		return fmt.Errorf("layer %d: %w", layer.Number, err)
	}
	return nil
}
