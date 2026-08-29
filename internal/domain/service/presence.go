package service

import (
	"context"

	"github.com/google/uuid"

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
		r.broadcastControl(model.ControlMessage{Kind: model.ControlRoomUserChange, Users: r.activeMemberCount()})
	}
}

// flushContribution emits the north-star contribution metric/event for the window
// just elapsed: the set of distinct actor ids that mutated the document (FR-014).
// It always sets the Prometheus gauge (via Metrics) and, in Alkemio mode, fires
// the RabbitMQ contribution event (via the Contributor port); a no-op contributor
// makes the bus emission free when no bus is wired. The window set is then reset. An
// empty window still sets the gauge to 0 but emits no bus event (nothing to log).
func (r *Room) flushContribution(ctx context.Context) {
	detached := r.detachContributors()
	if len(detached) == 0 {
		return
	}
	if err := r.emitContribution(ctx, detached); err != nil {
		// A transient analytics failure must not erase the elapsed actor set.
		r.mergeContributors(detached)
		r.logger.Warn("contribution event emit failed", zap.Error(err))
	}
}

func (r *Room) detachContributors() map[uuid.UUID]struct{} {
	detached := r.contributors
	r.contributors = make(map[uuid.UUID]struct{})
	r.metrics.ContributingActors(len(detached))
	return detached
}

func (r *Room) mergeContributors(actors map[uuid.UUID]struct{}) {
	for id := range actors {
		r.contributors[id] = struct{}{}
	}
}

func (r *Room) emitContribution(ctx context.Context, actors map[uuid.UUID]struct{}) error {
	actorIDs := make([]uuid.UUID, 0, len(actors))
	for id := range actors {
		actorIDs = append(actorIDs, id)
	}
	return r.deps.Contributor.Contribution(ctx, r.id, actorIDs)
}

// startContributionFlush detaches one periodic batch on the room loop and emits
// it under a bounded context off-loop. At most one batch is in flight; actors
// arriving meanwhile remain in the current set for the next tick or teardown.
func (r *Room) startContributionFlush() {
	if r.contributionFlight != nil {
		return
	}
	detached := r.detachContributors()
	if len(detached) == 0 {
		return
	}
	flight := &contributionFlight{actors: detached, done: make(chan error, 1)}
	r.contributionFlight = flight
	ctx, cancel := r.opCtx()
	go func() {
		defer cancel()
		err := r.emitContribution(ctx, detached)
		// Buffered (cap 1), so this never blocks and the result is available to
		// settleContributionFlight even if the completion below never dispatches.
		flight.done <- err
		// RAW send, deliberately NOT enqueueCtx. This is an internal completion
		// that cannot be dropped: enqueueCtx may refuse (room no longer active) or
		// time out, and either would strand contributionFlight non-nil forever,
		// silently stopping every later flush on this room.
		//
		// The cost is accepted: it may wait behind ordinary collaboration commands
		// on a saturated buffer, which stalls analytics — never document traffic —
		// and it always exits on r.done, so teardown frees it.
		select {
		case r.commands <- command{kind: cmdContributionDone, contribution: flight}:
		case <-r.done:
		}
	}()
}

func (r *Room) finishContributionFlush(flight *contributionFlight) {
	if flight == nil || r.contributionFlight != flight {
		return
	}
	err := <-flight.done
	r.contributionFlight = nil
	if err != nil {
		r.mergeContributors(flight.actors)
		r.logger.Warn("contribution event emit failed", zap.Error(err))
	}
}

// settleContributionFlight reconciles the one periodic attempt at teardown. A
// known failure is merged back so the final flush retries it once.
//
// The wait needs NO timer of its own. The in-flight emit already runs under
// opCtx, so it returns within the backend timeout on its own, and the goroutine
// writes the result to a BUFFERED channel before doing anything else — so this
// receive is bounded by that same deadline and cannot park on a send.
//
// An earlier revision guarded it with a second timer of equal duration. That
// bought nothing and cost a race with itself: whichever fired first decided
// whether a failure was retried or silently dropped, so an identical run could
// go either way. One deadline, owned by the call it bounds.
func (r *Room) settleContributionFlight() {
	flight := r.contributionFlight
	if flight == nil {
		return
	}
	if err := <-flight.done; err != nil {
		r.mergeContributors(flight.actors)
	}
	r.contributionFlight = nil
}

// contributionEnabled reports whether the north-star contribution metric is
// collected at all.
//
// A zero/negative window DISABLES the feature rather than meaning "every tick".
// ONE guard, at the only producer (recordContribution), is what turns it off
// everywhere — deliberately, rather than repeating the check on each emit path:
//   - periodic: newOptionalTicker STOPS the timer at this setting, so the tick
//     never fires;
//   - teardown: the set is empty, so the flush returns before any bus call.
//
// Guards on those paths were written and then removed: no test could tell them
// from their absence, because reaching them requires a state the config cannot
// produce (a populated set with the window off). mergeContributors is not a
// second producer — it only returns a batch that was collected while enabled.
func (r *Room) contributionEnabled() bool {
	return r.cfg.ContributionWindow > 0
}
