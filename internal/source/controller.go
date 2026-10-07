package source

import (
	"context"
	"fmt"
	"github.com/wyw14/cry-161/internal/model"
)

type Controller struct {
	admission *Admission
	heater    *Heater
	adapter   *Adapter
	power     float64
	held      int
}

func NewController(capacity int) *Controller {
	return &Controller{admission: NewAdmission(capacity), heater: &Heater{}, adapter: NewAdapter()}
}
func (c *Controller) Start(ctx context.Context, power float64) error {
	weight := int(power)
	if err := c.admission.Acquire(ctx, weight); err != nil {
		return err
	}
	if err := c.adapter.Open(); err != nil {
		c.admission.Release(weight)
		return err
	}
	if err := c.heater.Preheat(ctx); err != nil {
		c.admission.Release(weight)
		return err
	}
	c.power = power
	c.held += weight
	return nil
}
func (c *Controller) Stop()                          { c.admission.Release(c.held); c.held = 0; c.power = 0 }
func (c *Controller) Power() float64                 { return c.power }
func (c *Controller) Admission() *Admission          { return c.admission }
func (c *Controller) AdmissionUsed() int             { return c.admission.Used() }
func (c *Controller) ConfigureBusy(value bool)       { c.adapter.SetBusy(value) }
func (c *Controller) ConfigurePreheatFailures(n int) { c.heater.StableAfter = n; c.heater.Attempts = 0 }
func (c *Controller) Layer(ctx context.Context, layer model.Layer) error {
	if err := c.Start(ctx, layer.RateNanometers); err != nil {
		return fmt.Errorf("layer %d: %w", layer.Number, err)
	}
	return nil
}
