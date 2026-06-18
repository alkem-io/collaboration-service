package ws

import (
	"context"
	"errors"
	"sync"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// errConnClosed is returned by Send once the connection has been torn down, so
// the room evicts the member from its registry.
var errConnClosed = errors.New("ws: connection closed")

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

	send chan []byte

	closeOnce sync.Once
	closed    chan struct{}
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
		send:   make(chan []byte, buffer),
		closed: make(chan struct{}),
	}
}

// Send enqueues a framed message for delivery. It never blocks the caller (the
// room run loop): on a full queue it drops the connection (closes it) and
// returns an error so the room evicts the member, rather than stalling every
// other participant for one slow client.
func (c *wsConn) Send(frame []byte) error {
	select {
	case <-c.closed:
		return errConnClosed
	default:
	}
	select {
	case c.send <- frame:
		return nil
	case <-c.closed:
		return errConnClosed
	default:
		// Slow consumer: shed it rather than block the room.
		c.logger.Debug("send queue full; dropping slow connection")
		c.close()
		return errConnClosed
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
			case frame := <-c.send:
				if err := c.conn.Write(c.ctx, websocket.MessageBinary, frame); err != nil {
					c.logger.Debug("socket write failed; closing", zap.Error(err))
					c.close()
					return
				}
			}
		}
	}()
}

// close tears the connection down once: it signals the writer to stop and closes
// the socket. Idempotent.
func (c *wsConn) close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
	})
}

// compile-time assertion that wsConn satisfies the room's outbound port.
var _ service.Conn = (*wsConn)(nil)
