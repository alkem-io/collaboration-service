package service

import (
	"testing"

	"github.com/antst/go-yjs/backend/conformance"
	"github.com/antst/go-yjs/backend/hub"
	"github.com/antst/go-yjs/backend/memory"
)

// TestRegistryConformance runs the core's registry contract against the exact
// implementation this service uses (T047, SC-006).
//
// The service does NOT implement Registry — it uses the shipped
// InProcessRegistry as-is (§X, §XI: no reimplementation of what the core
// provides). Running the suite here therefore validates the *dependency* rather
// than our code, which is deliberate: this service leans on Acquire's coalescing
// for exactly-once first-open restore (FR-004b) and on Invalidate's
// poison-and-signal for the teardown-flush matrix (FR-011a). If a future core
// bump changed either, the assumption those requirements rest on would break
// silently — this suite is what makes that loud.
func TestRegistryConformance(t *testing.T) {
	conformance.Memory(t, func() memory.Registry { return memory.NewRegistry() })
}

// TestShippedInProcessHubConformance runs the fan-out contract against the
// core's shipped single-process hub (T049).
//
// Same reasoning: the single-pod path uses InProcess unmodified, so this asserts
// the shipped default still satisfies the contract this service relies on —
// notably SourceID echo suppression, which the room depends on to avoid
// re-applying its own updates. The Redis implementation (US5) gets the same
// suite when it lands; a custom implementation is held to the contract, not
// exempted from it.
func TestShippedInProcessHubConformance(t *testing.T) {
	conformance.Hub(t, func() hub.Hub { return hub.NewInProcess() })
}
