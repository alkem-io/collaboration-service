package service

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// disconnect ejects one connection from the room with a control message, then
// drops it (which forces its awareness eviction). Other members are untouched —
// a limit breach or forced read-only is per-connection (FR-024, constitution §V).
// A failed control send still drops the member (the connection is already gone).
func (r *Room) disconnect(id connID, reason string) {
	m, ok := r.members[id]
	if !ok {
		return
	}
	if frame := encodeControl(model.ControlMessage{Kind: model.ControlRoomClosed, Error: reason}); frame != nil {
		_ = m.conn.Send(frame)
	}
	if r.dropMember(id) {
		r.broadcastControl(model.ControlMessage{Kind: model.ControlRoomUserChange, Users: len(r.members)})
	}
}

// sweepInactive downgrades every collaborator that has not mutated the document
// within CollaboratorInactivity to viewer, emitting a read-only-state control so
// the client disables local editing (FR-014, whiteboard collaborator_inactivity
// parity). The downgrade carries the `inactivity` reason (OPEN-1) on both the
// read-only-state and the additive collaborator-mode frame, so the client mirrors
// today's collaborator-mode UX. Runs on the room loop, so the member map access is
// race-free.
func (r *Room) sweepInactive() {
	if r.cfg.CollaboratorInactivity <= 0 {
		return
	}
	cutoff := time.Now().Add(-r.cfg.CollaboratorInactivity)
	for id, m := range r.members {
		if m.mode != model.ModeCollaborator || m.lastActivity.After(cutoff) {
			continue
		}
		m.mode = model.ModeViewer
		r.members[id] = m
		r.sendModeDowngrade(m, model.ReasonInactivity)
	}
}

// sendModeDowngrade tells a single member it is now read-only for the given
// reason (OPEN-1). It sends the read-only-state frame (backward-compatible: a
// client only reading readOnly keeps working) carrying the reason code, plus the
// additive collaborator-mode frame {mode: viewer, reason} the WS-D client uses to
// preserve its collaborator-mode UX granularity.
func (r *Room) sendModeDowngrade(m roomMember, reason model.CollaboratorModeReason) {
	if frame := encodeControl(model.ControlMessage{
		Kind:     model.ControlReadOnlyState,
		ReadOnly: true,
		Reason:   reason,
	}); frame != nil {
		r.sendMember(m, frame)
	}
	if frame := encodeControl(model.ControlMessage{
		Kind:   model.ControlCollaboratorMode,
		Mode:   model.ModeViewer,
		Reason: reason,
	}); frame != nil {
		r.sendMember(m, frame)
	}
}

// flushContribution emits the north-star contribution metric/event for the window
// just elapsed: the set of distinct actor ids that mutated the document (FR-014).
// It always sets the Prometheus gauge (via Metrics) and, in Alkemio mode, fires
// the RabbitMQ contribution event (via the Contributor port); a no-op contributor
// makes the bus emission free in standalone. The window set is then reset. An
// empty window still sets the gauge to 0 but emits no bus event (nothing to log).
func (r *Room) flushContribution(ctx context.Context) {
	r.metrics.ContributingActors(len(r.contributors))
	if len(r.contributors) == 0 {
		return
	}
	actorIDs := make([]string, 0, len(r.contributors))
	for id := range r.contributors {
		actorIDs = append(actorIDs, id)
	}
	if err := r.deps.Contributor.Contribution(ctx, r.id, actorIDs); err != nil {
		// A missed analytics event MUST NOT break live collaboration (FR-014).
		r.logger.Warn("contribution event emit failed", zap.Error(err))
	}
	r.contributors = make(map[string]struct{})
}

// reEvaluateMembers re-runs per-document authZ for every connected member and
// applies the result (lifecycle document.access_changed, T014):
//
//   - READ revoked (resolveMode → ErrForbidden): the member is DISCONNECTED, not
//     downgraded. A viewer still receives every document update, so leaving a
//     read-revoked member connected as a viewer would let them keep reading the
//     document after their read access was pulled — an access-revocation leak
//     (constitution §V fail-closed). Disconnect is the only correct response.
//   - update-content lost but READ still granted: downgraded to viewer
//     (read-only-state control) so the client disables local editing but stays
//     connected.
//   - viewer that regained update-content: upgraded to collaborator.
//   - any other authZ error (transport failure / open breaker): fail closed by
//     revoking write access (downgrade to viewer); a transient backend error must
//     not silently keep a stale collaborator grant.
//
// Disconnecting mutates r.members, so iterate over a snapshot of the ids rather
// than ranging the live map.
func (r *Room) reEvaluateMembers(ctx context.Context) {
	ids := make([]connID, 0, len(r.members))
	for id := range r.members {
		ids = append(ids, id)
	}
	for _, id := range ids {
		m, ok := r.members[id]
		if !ok {
			continue // already disconnected in this pass.
		}
		newMode, err := r.resolveMode(ctx, model.Identity{ActorID: m.actorID})
		switch {
		case errors.Is(err, ErrForbidden):
			// Read access revoked: eject the member — a viewer still reads updates.
			r.disconnect(id, "read access revoked")
			continue
		case err != nil:
			// Transport/breaker failure: fail closed by revoking write access, but
			// keep the member connected (read access could not be confirmed denied).
			newMode = model.ModeViewer
		}
		if newMode == m.mode {
			continue
		}
		m.mode = newMode
		r.members[id] = m
		if newMode == model.ModeViewer {
			// Lost update-content (or fail-closed): read-only with the access
			// reason (OPEN-1) so the client mirrors today's read-only UX.
			reason := readOnlyReasonForIdentity(model.Identity{ActorID: m.actorID})
			r.sendMember(m, mustReadOnlyControl(true, reason))
		} else {
			// Regained update-content: clear read-only (no reason).
			r.sendMember(m, mustReadOnlyControl(false, ""))
		}
	}
}

// purge runs the owner-delete cascade on the run loop (T015): it tells every
// connected client the room is closing (room-closed), purges the snapshot blob and
// the metadata index, and lets the caller release the room. Idempotent — a
// not-found blob/metadata delete is success (constitution §V, lifecycle-events.md).
func (r *Room) purge(ctx context.Context) error {
	r.broadcastControl(model.ControlMessage{Kind: model.ControlRoomClosed, Error: "document deleted"})
	if r.pointer != "" {
		if err := r.deps.Blob.Delete(ctx, r.pointer); err != nil && !isNotFound(err) {
			return err
		}
	}
	if err := r.deps.Metadata.Delete(ctx, r.id); err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

// mustReadOnlyControl frames a read-only-state control with the given value and
// reason code (OPEN-1; reason is empty when clearing read-only). It never returns
// nil for these fixed inputs (JSON marshalling of a known struct cannot fail), so
// callers may use it inline.
func mustReadOnlyControl(readOnly bool, reason model.ReadOnlyReason) []byte {
	return encodeControl(model.ControlMessage{
		Kind:     model.ControlReadOnlyState,
		ReadOnly: readOnly,
		Reason:   reason,
	})
}
