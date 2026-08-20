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

// declareTopology declares every queue this consumer owns.
//
// The TTL is int32 rather than int: AMQP argument types participate in
// declaration equivalence, so a value that arrives as a different numeric type is
// an inequivalent redeclaration and fails PRECONDITION_FAILED against a queue
// declared by anything else.
func declareTopology(ch brokerChannel, n queueNames) error {
	// Q1: frozen contract, mirrored by the producer. Nothing but the queue type.
	if _, err := ch.QueueDeclare(n.main, true, false, false, false, amqp.Table{
		"x-queue-type": "quorum",
	}); err != nil {
		return fmt.Errorf("declare %s: %w", n.main, err)
	}

	// Q5: terminal. No TTL, no dead-lettering — a message here has exhausted the
	// schedule and waits for a human.
	if _, err := ch.QueueDeclare(n.dlq, true, false, false, false, amqp.Table{
		"x-queue-type": "quorum",
	}); err != nil {
		return fmt.Errorf("declare %s: %w", n.dlq, err)
	}

	// Q2-Q4: no consumer. A message sits for its TTL and is dead-lettered back to
	// Q1 by the broker.
	//
	// x-dead-letter-strategy=at-least-once is what makes that hop durable: without
	// it the internal republish is at-most-once and the delay tier — the mechanism
	// that exists to PROVIDE durability — silently drops messages. It requires
	// x-overflow=reject-publish; at-least-once is refused with drop-head.
	for i, t := range retryTiers {
		if _, err := ch.QueueDeclare(n.tiers[i], true, false, false, false, amqp.Table{
			"x-queue-type":              "quorum",
			"x-message-ttl":             t.ttlMS,
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": n.main,
			"x-dead-letter-strategy":    "at-least-once",
			"x-overflow":                "reject-publish",
		}); err != nil {
			return fmt.Errorf("declare %s: %w", n.tiers[i], err)
		}
	}
	return nil
}
