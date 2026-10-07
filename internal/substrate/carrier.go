package substrate

import (
	"errors"
	"fmt"
	"sync"

	"github.com/wyw14/cry-161/internal/model"
)

var ErrCarrierLoaded = errors.New("carrier already loaded")

// Token is a fencing credential handed out by LoadFor. Unloading requires the
// matching batch, run and epoch, so a late unload from a previous run cannot
// eject a later run's carrier.
type Token struct {
	Batch string
	Run   model.ID
	Epoch uint64
}

type Carrier struct {
	mu     sync.Mutex
	batch  string
	runID  model.ID
	epoch  uint64
	loaded bool
}

func NewCarrier() *Carrier { return &Carrier{} }

// LoadFor reserves the carrier for runID within batch, returning an owning
// token. It fails when the carrier is already loaded.
func (c *Carrier) LoadFor(batch string, runID model.ID) (Token, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loaded {
		return Token{}, ErrCarrierLoaded
	}
	c.epoch++
	c.batch = batch
	c.runID = runID
	c.loaded = true
	return Token{Batch: batch, Run: runID, Epoch: c.epoch}, nil
}

// Unload releases the carrier only when the token matches the current load.
func (c *Carrier) Unload(token Token) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded || c.batch != token.Batch || c.runID != token.Run || c.epoch != token.Epoch {
		return false
	}
	c.batch = ""
	c.runID = ""
	c.loaded = false
	return true
}

// Load reserves the carrier without returning an owning token. Retained for
// self-contained audits that pair Load/Unload immediately. Production paths
// must use LoadFor/Unload(Token).
func (c *Carrier) Load(batch string, runID model.ID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loaded {
		return fmt.Errorf("carrier already loaded")
	}
	c.epoch++
	c.batch = batch
	c.runID = runID
	c.loaded = true
	return nil
}

func (c *Carrier) UnloadBatch(batch string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded || c.batch != batch {
		return false
	}
	c.batch = ""
	c.runID = ""
	c.loaded = false
	return true
}

func (c *Carrier) Run(batch string) (model.ID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded || c.batch != batch {
		return "", false
	}
	return c.runID, true
}

func (c *Carrier) Loaded() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.loaded }
