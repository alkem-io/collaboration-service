package ws

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// errConnClosed is returned by Send once the connection has been torn down, so
// the room evicts the member from its registry.
var errConnClosed = errors.New("ws: connection closed")

// handshakeSendTimeout bounds how long the handshake batch will wait for queue
// space. A peer that has not drained a single frame in this long is not reading
// at all, and shedding it then is correct rather than merely impatient.
const handshakeSendTimeout = 10 * time.Second

// wsConn adapts a coder/websocket connection to the room's service.Conn port:
// the room fans framed messages out by calling Send, which enqueues onto a
// buffered channel drained by a single writer goroutine. Decoupling the room
// from the socket write this way keeps the room non-blocking — a slow client
// cannot stall the room's single run loop — and makes all writes serialized
// through one goroutine (coder/websocket forbids concurrent writes).
type wsConn struct {
	ctx    context.Context
	conn   *websocket.Conn
	logger *zap.Logger

	send chan outgoing

	// admit guards the TERMINAL BOUNDARY — the `ending` and `closed` state AND
	// the enqueue — as ONE step.
	//
	// An atomic flag is not enough, and the reason is worth stating because it
	// reads as though it would be: a sender that checks the flag and then
	// enqueues has two separable steps, so a close intent admitted between them
	// puts the sender's frame BEHIND the close it was just told had not happened.
	// The writer stops at the intent, so the frame is never written — but the
	// sender was told it was queued, and the port's promise that nothing can be
	// admitted after the end becomes false. Checking and enqueueing under one
	// lock removes the window rather than narrowing it.
	//
	// Every holder does only NON-BLOCKING work while holding it (a select with a
	// default), so it can never stall the room's run loop. Waiting for queue
	// space happens outside it — see sendInitial.
	admit  sync.Mutex
	ending bool

	// space is poked by the writer each time it removes an item, so a bounded
	// waiter can retry admission without holding admit while it waits. Buffered
	// so a signal sent before the waiter parks is not lost; a stale token only
	// costs one extra retry.
	space chan struct{}

	closeOnce sync.Once
	closed    chan struct{}
}

// admission is the outcome of offering an item to the outbound queue.
type admission int

const (
	// admitted: the item is on the queue, in order, and will be written.
	admitted admission = iota
	// refusedTerminal: the connection is ending or already gone. Nothing may be
	// queued, now or later.
	refusedTerminal
	// refusedFull: the queue has no space. The caller decides whether to shed
	// (Send) or wait for room (sendInitial).
	refusedFull
)

// outgoing is one item on the connection's single outbound queue: either a frame
// to write, or the intent to close once everything ahead of it has been written.
//
// Both travel the SAME channel on purpose. That is what makes "the client always
// sees the reason before the close" a property of the queue rather than of
// timing: the session-end control is enqueued first, the intent behind it, and
// the one writer drains them in order. Any design with a separate close signal
// would be racing the frames it is supposed to follow.
type outgoing struct {
	frame []byte
	end   *model.SessionEnd
}

// newWSConn builds the adapter with a bounded outbound queue. startWriter must
// be called to begin draining; Send before that buffers up to the queue depth.
func newWSConn(ctx context.Context, conn *websocket.Conn, buffer int, logger *zap.Logger) *wsConn {
	if buffer <= 0 {
		buffer = 64
	}
	return &wsConn{
		ctx:    ctx,
		conn:   conn,
		logger: logger,
		send:   make(chan outgoing, buffer),
		space:  make(chan struct{}, 1),
		closed: make(chan struct{}),
	}
}

// Send enqueues a framed message for delivery. It never blocks the caller (the
// room run loop): on a full queue it drops the connection (closes it) and
// returns an error so the room evicts the member, rather than stalling every
// other participant for one slow client.
func (c *wsConn) Send(frame []byte) error {
	switch c.offer(outgoing{frame: frame}) {
	case admitted:
		return nil
	case refusedFull:
		// Slow consumer: shed it rather than block the room.
		c.logger.Debug("send queue full; dropping slow connection")
		c.close()
		return errConnClosed
	default:
		return errConnClosed
	}
}

// offer makes the WHOLE admission decision — terminal check and enqueue — under
// one lock, so an item can never be admitted against a boundary that moved while
// it was being checked. It never blocks: the enqueue is a non-blocking send, so
// the room's run loop cannot be stalled here.
func (c *wsConn) offer(item outgoing) admission {
	c.admit.Lock()
	defer c.admit.Unlock()
	if c.ending || c.isClosed() {
		return refusedTerminal
	}
	select {
	case c.send <- item:
		return admitted
	default:
		return refusedFull
	}
}

// admitEnd queues the close intent and closes the boundary behind it, in one
// step. Setting `ending` and enqueueing under the same lock is what makes the
// intent genuinely last: a concurrent offer either wins the lock first and is
// queued ahead of it, or takes the lock after and is refused. There is no
// ordering in which it lands behind.
func (c *wsConn) admitEnd(end model.SessionEnd) admission {
	c.admit.Lock()
	defer c.admit.Unlock()
	if c.ending || c.isClosed() {
		return refusedTerminal
	}
	// Set BEFORE the enqueue and keep it set even if the enqueue fails: once the
	// decision to end has been taken, nothing further may be queued either way.
	c.ending = true
	select {
	case c.send <- outgoing{end: &end}:
		return admitted
	default:
		return refusedFull
	}
}

// isClosed reports whether the hard teardown has run.
//
// Deliberately NOT synchronised with the admission lock. `closed` is a channel,
// so this observation is already race-free, and the only thing a stale answer can
// cost is an item admitted into a queue nothing will drain — which is never
// written and never seen. Taking the lock in close() to linearize it was tried
// and removed: no test could distinguish it, which is the standard for keeping a
// mechanism here.
//
// `ending` is different, and that is why it IS under the lock: it is a plain
// field, and the ORDER of its move against an enqueue is exactly the property
// that keeps a frame from landing behind the close intent.
func (c *wsConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// sendInitial enqueues a handshake frame, WAITING for queue space rather than
// shedding on a full queue.
//
// The shedding policy in Send exists to protect the room's single run loop from
// a slow client. The handshake batch is different on both counts: it is sent
// from the per-connection handler goroutine, where blocking stalls nobody else,
// and the frames are the SERVER's own — a full queue here is not evidence of a
// slow consumer, it just means the writer goroutine has not been scheduled yet.
// Shedding on that would drop legitimate joiners on a small SendBuffer purely on
// scheduler timing, which is a race rather than a policy.
//
// The wait is bounded by ctx (see handshakeSendTimeout): a peer that never makes
// room is genuinely not reading, and is shed like any other.
// It obeys the SAME terminal boundary as Send. It previously did not consult it
// at all, which cost more than tidiness: a teardown racing the handler's initial
// batch could admit the close intent while this was parked waiting for space,
// and since the writer stops at the intent, nothing would ever drain again — so
// the handshake sat here for the full handshakeSendTimeout before shedding a
// connection whose session had already ended.
//
// The wait happens OUTSIDE the admission lock. Holding it while blocking would
// stall Send — and therefore the room's run loop — behind one client's queue
// space, which is precisely what this design exists to prevent.
func (c *wsConn) sendInitial(ctx context.Context, frame []byte) error {
	for {
		switch c.offer(outgoing{frame: frame}) {
		case admitted:
			return nil
		case refusedTerminal:
			return errConnClosed
		}
		// refusedFull: wait for the writer to take something off, then retry the
		// whole admission — the boundary may have moved while we waited, and it
		// must be re-checked rather than assumed.
		select {
		case <-c.space:
		case <-c.closed:
			return errConnClosed
		case <-ctx.Done():
			c.logger.Debug("handshake frame could not be enqueued; dropping connection")
			c.close()
			return errConnClosed
		}
	}
}

// startWriter launches the single writer goroutine that drains the send queue
// onto the socket as binary frames. It exits when the queue closes or a write
// fails.
func (c *wsConn) startWriter() {
	go func() {
		for {
			select {
			case <-c.closed:
				return
			case item := <-c.send:
				c.releaseSpace()
				if item.end != nil {
					// Everything queued ahead of this has been written, including the
					// session-end control, so the reason has reached the client before
					// the close does. Running the close HERE is also what keeps the
					// handshake off the room's run loop — see close() below for why
					// that matters.
					c.closeWith(*item.end)
					return
				}
				if err := c.conn.Write(c.ctx, websocket.MessageBinary, item.frame); err != nil {
					c.logger.Debug("socket write failed; closing", zap.Error(err))
					c.close()
					return
				}
			}
		}
	}()
}

// releaseSpace announces that one queue slot was freed, so a bounded waiter
// (sendInitial) can retry admission. Non-blocking, and the signal is buffered, so
// it is never lost and a stale token only costs one extra retry.
//
// It is called by whatever takes an item OFF the queue — in production that is
// only the writer goroutine.
func (c *wsConn) releaseSpace() {
	select {
	case c.space <- struct{}{}:
	default:
	}
}

// close tears the connection down once. Idempotent.
//
// The signal half is synchronous and cheap: closing c.closed immediately makes
// Send shed (return errConnClosed) and stops the writer goroutine. The socket
// teardown is deliberately OFF this goroutine. close is called from the room's
// single run loop (sendMember → Send → slow-consumer shed; disconnect; persist),
// and coder/websocket's Conn.Close performs the close HANDSHAKE — it writes a
// close frame with an internal 5s timeout and then waits up to 15s for the peer
// and the connection goroutines. On a slow/stalled client whose TCP send buffer
// is full, that write blocks until the 5s deadline fires (the deadline is
// enforced by a context.AfterFunc that force-closes the socket). Running it
// inline would freeze the run loop — which serializes joins/edits/awareness/
// leaves/persist for EVERY member of the room — for up to ~5s per shed, so a
// disconnect-storm of N slow clients would serialize into N×~5s of room-wide
// stall. That is exactly the head-of-line blocking the buffered-channel + single
// writer design exists to prevent, so the handshake must never run on the loop.
//
// CloseNow (no handshake — it just closes the underlying socket) is used instead
// of Close so even the off-loop teardown cannot block on a stalled peer, and it
// is dispatched on its own goroutine so close() returns to the run loop at once.
func (c *wsConn) close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		go func() { _ = c.conn.CloseNow() }()
	})
}

// CloseAfterDrain queues the intent to close once the frames already queued have
// been written (service.Conn).
//
// It never blocks the room loop: the enqueue is non-blocking, and a full queue
// falls back to an immediate close. That fallback loses nothing worth keeping —
// a client whose queue is full is not draining, so a graceful close would not
// reach it either.
func (c *wsConn) CloseAfterDrain(end model.SessionEnd) {
	// admitEnd closes the boundary and queues the intent as ONE step, so only the
	// first end wins and no frame can be admitted behind it.
	if c.admitEnd(end) == refusedFull {
		c.logger.Debug("send queue full; closing without draining",
			zap.String("code", end.Code))
		c.close()
	}
}

// closeWith performs the graceful close for a session end, mapping the domain
// disposition to a WebSocket status. It runs on the writer goroutine.
func (c *wsConn) closeWith(end model.SessionEnd) {
	status, reason := closeStatusFor(end)
	_ = c.conn.Close(status, reason)
	c.closeOnce.Do(func() { close(c.closed) })
}

// closeStatusFor maps a session end to the WebSocket status and reason that
// carry it. The reason is the STABLE code, never prose: it is the same literal
// the control frame carries, so a client that only sees the close still gets a
// value it can branch on.
//
// Every code is listed explicitly rather than derived from the disposition
// alone, because the two transient codes want different statuses: a shutdown is
// the endpoint going away, while a rate-limited member is being asked to come
// back later, and 1013 says that precisely. The default is unreachable
// (NewSessionEnd rejects unknown codes) but degrades on disposition rather than
// inventing a status.
func closeStatusFor(end model.SessionEnd) (websocket.StatusCode, string) {
	switch end.Code {
	case model.CodeServerShutdown:
		return websocket.StatusGoingAway, end.Code
	case model.CodeUpdateRateExceeded, model.CodeUpdateNotAccepted:
		// 1013 Try Again Later for BOTH member-scoped transient causes: the client
		// was refused, nothing about the document is wrong, and reconnecting with
		// backoff is the right response. They stay distinguishable by the close
		// REASON, which is the code itself.
		return websocket.StatusTryAgainLater, end.Code
	case model.CodeDocumentSizeLimitExceeded, model.CodeDocumentDeleted, model.CodeEditsNotSaved,
		model.CodeContentRefused, model.CodeForbidden:
		return websocket.StatusPolicyViolation, end.Code
	default:
		if end.Disposition == model.DispositionTransient {
			return websocket.StatusGoingAway, end.Code
		}
		return websocket.StatusPolicyViolation, end.Code
	}
}

// compile-time assertion that wsConn satisfies the room's outbound port.
var _ service.Conn = (*wsConn)(nil)
