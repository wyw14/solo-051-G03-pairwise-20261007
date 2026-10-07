package chamber

import (
	"github.com/wyw14/cry-161/internal/model"
	"sync"
	"time"
)

type Chamber struct {
	mu          sync.RWMutex
	name        string
	pressure    float64
	stable      bool
	shutter     string
	sourcePower float64
	updated     time.Time
	interlocks  map[string]bool
	occ         Occupancy
}

func New(name string) *Chamber {
	return &Chamber{name: name, shutter: "closed", interlocks: map[string]bool{"vacuum": false, "cooling": true}, updated: time.Now().UTC()}
}
func (c *Chamber) SetVacuum(pressure float64, stable bool) {
	c.mu.Lock()
	c.pressure = pressure
	c.stable = stable
	c.updated = time.Now().UTC()
	c.mu.Unlock()
}
func (c *Chamber) SetShutter(state string) {
	c.mu.Lock()
	c.shutter = state
	c.updated = time.Now().UTC()
	c.mu.Unlock()
}
func (c *Chamber) SetSourcePower(power float64) {
	c.mu.Lock()
	c.sourcePower = power
	c.updated = time.Now().UTC()
	c.mu.Unlock()
}
func (c *Chamber) SetInterlock(name string, value bool) {
	c.mu.Lock()
	c.interlocks[name] = value
	c.updated = time.Now().UTC()
	c.mu.Unlock()
}
func (c *Chamber) Snapshot() model.ChamberSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return model.ChamberSnapshot{Name: c.name, Pressure: c.pressure, Stable: c.stable, Shutter: c.shutter, SourcePower: c.sourcePower, UpdatedAt: c.updated}
}
func (c *Chamber) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stable && c.interlocks["vacuum"] && c.interlocks["cooling"]
}
