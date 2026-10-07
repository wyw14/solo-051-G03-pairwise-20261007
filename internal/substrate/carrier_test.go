package substrate

import (
	"errors"
	"testing"

	"github.com/wyw14/cry-161/internal/model"
)

func TestCarrier_LoadForUnloadFenced(t *testing.T) {
	c := NewCarrier()
	batchA := "batch-a"
	runA := model.ID("run-a")

	token, err := c.LoadFor(batchA, runA)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !c.Loaded() {
		t.Fatal("carrier should be loaded")
	}
	if _, err := c.LoadFor("batch-b", model.ID("run-b")); !errors.Is(err, ErrCarrierLoaded) {
		t.Fatalf("second load err=%v want ErrCarrierLoaded", err)
	}
	// Wrong run / stale epoch cannot unload.
	if c.Unload(Token{Batch: batchA, Run: model.ID("run-other"), Epoch: token.Epoch}) {
		t.Fatal("unload with wrong run must fail")
	}
	if c.Unload(Token{Batch: batchA, Run: runA, Epoch: token.Epoch + 1}) {
		t.Fatal("unload with stale epoch must fail")
	}
	if !c.Loaded() {
		t.Fatal("carrier must remain loaded after rejected unloads")
	}
	if !c.Unload(token) {
		t.Fatal("valid unload should succeed")
	}
	if c.Loaded() {
		t.Fatal("carrier should be unloaded")
	}
}

func TestCarrier_StaleUnloadKeepsNewLoad(t *testing.T) {
	c := NewCarrier()
	tokenA, err := c.LoadFor("batch-a", model.ID("run-a"))
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	if !c.Unload(tokenA) {
		t.Fatal("unload A should succeed")
	}
	tokenB, err := c.LoadFor("batch-b", model.ID("run-b"))
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	// A's old token must not eject B.
	if c.Unload(tokenA) {
		t.Fatal("stale token A must not unload B")
	}
	run, ok := c.Run("batch-b")
	if !ok || run != model.ID("run-b") {
		t.Fatalf("Run(batch-b)=(%q,%t), want run-b,true", run, ok)
	}
	if !c.Unload(tokenB) {
		t.Fatal("unload B with its own token should succeed")
	}
	if c.Loaded() {
		t.Fatal("carrier should be free after valid unload")
	}
}
