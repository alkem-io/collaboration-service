package lifecycle

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The lifecycle topology: one main queue and one dead-letter queue, both quorum
// and both durable. Everything routes through the DEFAULT exchange, where the
// routing key IS the queue name, so no exchange is declared or bound.
//
// The DLQ is DIAGNOSTIC, not a retry path. It holds only envelopes this service
// can never act on — unparseable, or a pattern outside the contract — so anything
// in it is a producer/consumer contract mismatch. Transient failures are requeued
// on the main queue and never reach it. Nothing consumes it.
//
// x-delivery-limit=-1 on BOTH queues is load-bearing on RabbitMQ 4.0+, which
// defaults quorum queues to 20 where 3.x was unlimited. Measured on 4.0.5, the
// 21st delivery is DROPPED when a queue has no dead-letter route to divert to. On
// the main queue every requeued transient failure is another delivery. On the DLQ
// the producer is less obvious and is the reason the argument is not merely
// copied: the management UI's "Get messages" with Requeue=yes issues basic.get,
// which counts as a delivery — twenty operator inspections of a stuck poison
// message and the 21st would drop the record.
//
// CROSS-REPO CONTRACT — the main queue's arguments are declared by `server` too
// (src/core/microservices/microservices.module.ts) and MUST match exactly. An
// inequivalent redeclaration fails PRECONDITION_FAILED and the declaring party
// does not start; queue arguments are immutable, so a mismatch is fixed by
// deleting and recreating the queue, not by redeploying. Change this table only
// in lockstep with server, as a planned cutover.
const suffixDLQ = ".dlq"

// queueNames derives every queue name from the configured main queue, so one
// config value names the whole topology and the parts cannot drift apart.
type queueNames struct {
	main string
	dlq  string
}

func namesFor(main string) queueNames {
	return queueNames{main: main, dlq: main + suffixDLQ}
}

// queueSpec is one queue's declaration: the name and the exact argument table.
// Declaration and depth-polling both go through this list, so the arguments used
// to poll are by construction the ones used to declare — a re-declare with a
// different table would fail PRECONDITION_FAILED and take the channel down.
type queueSpec struct {
	name string
	args amqp.Table
}

// topologyFor is the whole topology as data.
//
// Integer arguments are written as int32 by convention, matching the type the
// broker reports back ('signedint'). That is a convention, NOT a requirement:
// measured on 4.0.5, RabbitMQ normalizes integer widths for these arguments, so
// int8/int16/int32/int64 and a plain Go int carrying the same value all redeclare
// equivalently. What IS load-bearing is the VALUE and the PRESENCE — a different
// value, or omitting an argument the queue already has, fails PRECONDITION_FAILED
// and the declaring party does not start.
func topologyFor(n queueNames) []queueSpec {
	return []queueSpec{
		// The main queue: mirrored by the producer (see the coupling note above).
		// Its DLX is what makes Nack(requeue=false) reach the DLQ; without it the
		// broker DISCARDS a rejected message and the poison path silently loses the
		// very thing it exists to make visible.
		{n.main, amqp.Table{
			"x-queue-type":              "quorum",
			"x-delivery-limit":          int32(-1),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": n.dlq,
		}},
		// The DLQ: terminal and diagnostic. No TTL and no dead-lettering — a message
		// here is a producer/consumer contract mismatch waiting for a human. The
		// delivery limit stays unlimited for the operator-inspection reason above:
		// with no dead-letter route, a finite limit would drop the record.
		{n.dlq, amqp.Table{
			"x-queue-type":     "quorum",
			"x-delivery-limit": int32(-1),
		}},
	}
}

// declareTopology declares every queue this consumer owns. Durable throughout: a
// broker restart must not vaporise a pending deletion.
func declareTopology(ch brokerChannel, n queueNames) error {
	for _, q := range topologyFor(n) {
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, q.args); err != nil {
			return fmt.Errorf("declare %s: %w", q.name, err)
		}
	}
	return nil
}
