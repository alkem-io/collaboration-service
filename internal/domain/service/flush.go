package service

import (
	"time"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// Durability state machine (FR-013, research.md D10).
//
//	clean      no unsaved changes
//	dirty      unsaved changes, flush armed
//	undurable  flush failing, STILL SERVING
//	escalated  invalidated, members disconnected, unsaved edits discarded
//
// The governing distinction is that a failed flush means NOT YET DURABLE, which
// is not the same as DIVERGED. The in-memory document remains authoritative and
// correct, so the session keeps serving and retries; invalidation is reserved for
// state that has actually moved out from under us. Tearing a healthy session down
// over one transient backend blip would trade a real outage for a theoretical one.
//
// Escalation exists so the other error does not happen either: edits cannot
// accumulate unbacked forever. Past a bounded number of consecutive failures the
// room is torn down WITHOUT a flush — see the teardown matrix, FR-011a — which
// discards the undurable edits. That loss is accepted, but it is never silent
// (FR-028).

// onFlushSucceeded records a durable save, clearing any undurable state.
func (r *Room) onFlushSucceeded() {
	if r.flushFailures == 0 {
		return
	}
	r.logger.Info("durability restored",
		zap.String("doc", string(r.id)),
		zap.Int("failed_flushes", r.flushFailures),
		zap.Duration("undurable_for", r.undurableFor()))
	r.flushFailures = 0
	r.undurableSince = time.Time{}
	r.metrics.DocumentDurabilityRestored()
	// FR-027's restore half — telling collaborators their work is safe again — is
	// carried by persist's own unconditional `saved` broadcast, which runs
	// immediately after this. Emitting one here too sent the client TWO identical
	// frames with the same version on every recovery, so anything counting saves
	// double-counted exactly when it was already reporting an incident.
	//
	// The transition is what the client reads: save-error, then saved.
}

// onFlushFailed records a failed save and escalates once the configured threshold
// is crossed. The document stays dirty and keeps serving until then.
func (r *Room) onFlushFailed(err error) {
	r.flushFailures++
	if r.undurableSince.IsZero() {
		r.undurableSince = time.Now()
	}
	undurable := r.undurableFor()

	r.metrics.SnapshotFailed()
	// Emitted on EVERY failure, not only at escalation: the degraded window must be
	// visible before anyone is disconnected, or the first signal an operator gets is
	// users being kicked off (FR-026, SC-013).
	r.metrics.DocumentUndurable(r.flushFailures, undurable)

	threshold := r.cfg.Limits.FlushFailureThreshold
	if threshold <= 0 {
		threshold = defaultFlushFailureThreshold
	}
	if r.flushFailures < threshold {
		r.logger.Warn("flush failed; document is not yet durable, retrying",
			zap.String("doc", string(r.id)),
			zap.Int("consecutive", r.flushFailures),
			zap.Int("threshold", threshold),
			zap.Duration("undurable_for", undurable),
			zap.Error(err))
		// Keep serving. Tell collaborators their recent edits are not yet safe.
		r.broadcastControl(model.ControlMessage{
			Kind:   model.ControlSaveError,
			Reason: model.ReasonNotYetDurable,
			Error:  "recent edits are not yet saved; retrying",
		})
		return
	}
	r.escalateUndurable(undurable, err)
}

// escalateUndurable tears the room down after repeated failures.
//
// The unsaved edits are DISCARDED — escalation fires precisely because the store
// is unreachable, so there is nowhere else to put them, and a secondary storage
// path would reintroduce the very adapters this feature removed (FR-029). The
// loss is accepted; what is forbidden is losing it quietly, so it is counted,
// logged with the document and how long it had been failing, and reported to
// collaborators with a reason that says what happened rather than a generic
// disconnect (FR-028, SC-016).
func (r *Room) escalateUndurable(undurable time.Duration, err error) {
	r.logger.Error("durability escalation: discarding unsaved edits and closing the room",
		zap.String("doc", string(r.id)),
		zap.Int("consecutive_failures", r.flushFailures),
		zap.Duration("undurable_for", undurable),
		zap.Error(err))
	r.metrics.DocumentEscalated(undurable)
	// Tear down WITHOUT flushing: the whole reason we are here is that the store
	// will not accept writes, and the teardown matrix forbids persisting a document
	// whose durability is in doubt (FR-011a).
	r.teardown(model.NewSessionEnd(model.CodeEditsNotSaved), nil)
}

// undurableFor reports how long the document has been failing to persist.
func (r *Room) undurableFor() time.Duration {
	if r.undurableSince.IsZero() {
		return 0
	}
	return time.Since(r.undurableSince)
}
