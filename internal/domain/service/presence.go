package service

import (
	"context"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// disconnect ejects one connection from the room with a control message, then
// drops it (which forces its awareness eviction). Other members are untouched —
// a limit breach or forced read-only is per-connection (FR-024, constitution §V).
// A failed control send still drops the member (the connection is already gone).
func (r *Room) disconnect(id connID, code model.SessionEndCode) {
	m, ok := r.members[id]
	if !ok {
		return
	}
	// ScopeMember, so the client can tell this from a room that ended: the room is
	// still serving everyone else, and only this connection is over.
	end := model.NewSessionEnd(code)
	if frame := encodeControl(end.Control()); frame != nil {
		_ = m.conn.Send(frame)
	}
	m.conn.CloseAfterDrain(end)
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
		ReadOnly: model.ReadOnlyState(true),
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

// purge runs the owner-delete cascade on the run loop (T015): it purges the
// snapshot blob and the metadata index, and lets the caller release the room.
// Telling the connected clients is the teardown funnel's job (document-deleted),
// so the cascade cannot announce a different reason than the one it tears down
// with. Idempotent — a
// not-found blob/metadata delete is success (constitution §V, lifecycle-events.md).
func (r *Room) purge(ctx context.Context) error {
	del, err := r.deps.deleter()
	if err != nil {
		return err
	}
	if err := del.Delete(ctx, persistence.DeleteRequest{DocumentID: backend.DocumentID(r.id)}); err != nil {
		return err
	}
	if err := r.deps.Metadata.Delete(ctx, r.id); err != nil && !isNotFound(err) {
		return err
	}
	return nil
}
