package lifecycle

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The lifecycle topology: one main queue, three delay tiers, one terminal DLQ.
//
// Everything routes through the DEFAULT exchange, where the routing key IS the
// queue name, so no exchange is declared or bound. Dead-lettering uses
// x-dead-letter-exchange="" with an explicit routing key, which reaches the same
// place without an exchange of our own.
//
// Q1 is the ONLY queue the producer also declares. Its arguments are frozen and
// mirrored by `server`: an inequivalent redeclaration on either side fails
// PRECONDITION_FAILED and the declaring party does not start. Q1 deliberately
// carries NO dead-letter arguments — transfers out of it are explicit confirmed
// publishes by this consumer, not broker dead-lettering.
//
// x-delivery-limit=-1 on Q1 and the DLQ is what keeps that true on RabbitMQ 4.0.
// 4.0 gives quorum queues a default delivery-limit of 20 where 3.x was unlimited,
// and our transfer-failure contract deliberately leaves a delivery UNACKED and
// recycles the channel — every recycle is another delivery. Measured on 4.0.5: the
// 21st delivery DROPS the message, silently, because neither queue has a
// dead-letter exchange to divert it to. A finite limit is only safe where there is
// somewhere to divert to, which is why the retry tiers keep the default: they have
// a DLX, so the limit dead-letters instead of dropping — and they have no consumer,
// so nothing increments a delivery count in the first place.
const (
	suffixRetry30s = ".retry.30s"
	suffixRetry5m  = ".retry.5m"
	suffixRetry30m = ".retry.30m"
	suffixDLQ      = ".dlq"
)

// retryTier is one delay step. The tiers are an operational policy, not a derived
// constant: long enough to outlast an ordinary rollout or backend restart, short
// enough that a revoked grant or an undeleted document is not left to a human to
// notice. Total covered outage before the DLQ is ~35 minutes.
type retryTier struct {
	suffix string
	ttlMS  int32
}

// tierCount is len(retryTiers) as an int32, so attempt bookkeeping never needs an
// unchecked narrowing conversion. TestTierCountMatchesTheSchedule pins the two together.
const tierCount int32 = 3

var retryTiers = []retryTier{
	{suffixRetry30s, 30_000},
	{suffixRetry5m, 300_000},
	{suffixRetry30m, 1_800_000},
}

// queueNames derives every queue name from the configured main queue, so one
// config value names the whole topology and the parts cannot drift apart.
type queueNames struct {
	main  string
	tiers []string
	dlq   string
}

func namesFor(main string) queueNames {
	n := queueNames{main: main, dlq: main + suffixDLQ}
	for _, t := range retryTiers {
		n.tiers = append(n.tiers, main+t.suffix)
	}
	return n
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
	specs := make([]queueSpec, 0, 2+len(retryTiers))
	specs = append(specs,
		// Q1: frozen contract, mirrored by the producer. Every argument here must
		// also be on the producer's declaration with the same VALUE — omitting one it
		// already has is refused just as a different value is.
		queueSpec{n.main, amqp.Table{
			"x-queue-type":     "quorum",
			"x-delivery-limit": int32(-1),
		}},
		// Q5: terminal. No TTL, no dead-lettering — a message here has exhausted the
		// schedule and waits for a human. Its redeliveries come from replay sessions
		// that fail and close their channel, so it needs the same unlimited limit.
		queueSpec{n.dlq, amqp.Table{
			"x-queue-type":     "quorum",
			"x-delivery-limit": int32(-1),
		}},
	)
	// Q2-Q4: no consumer. A message sits for its TTL and is dead-lettered back to
	// Q1 by the broker.
	//
	// x-dead-letter-strategy=at-least-once is what makes that hop durable: without
	// it the internal republish is at-most-once and the delay tier — the mechanism
	// that exists to PROVIDE durability — silently drops messages. It requires
	// x-overflow=reject-publish; at-least-once is refused with drop-head.
	for i, t := range retryTiers {
		specs = append(specs, queueSpec{n.tiers[i], amqp.Table{
			"x-queue-type":              "quorum",
			"x-message-ttl":             t.ttlMS,
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": n.main,
			"x-dead-letter-strategy":    "at-least-once",
			"x-overflow":                "reject-publish",
		}})
	}
	return specs
}

// declareTopology declares every queue this consumer owns. Durable throughout: a
// broker restart must not vaporise a pending deletion or revocation.
func declareTopology(ch brokerChannel, n queueNames) error {
	for _, q := range topologyFor(n) {
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, q.args); err != nil {
			return fmt.Errorf("declare %s: %w", q.name, err)
		}
	}
	return nil
}
