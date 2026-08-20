// Command lifecycle-replay moves lifecycle events from the dead-letter queue back
// onto the main queue.
//
// The dead-letter queue is terminal by design: nothing consumes it, nothing
// expires out of it, and an event that lands there is a deletion or a revocation
// that has NOT been applied. Getting it applied needs a person to fix the cause
// and then put the event back — which is what this does.
//
// It is a command rather than a documented shovel because the move has two
// requirements a shovel does not meet. The attempt count must be cleared, or the
// event returns to the DLQ on its first failure and looks like a replay that
// worked; a shovel preserves headers. And the DLQ copy must be released only
// after the broker has confirmed the republish, or a failed publish loses the
// event outright.
//
// Usage:
//
//	lifecycle-replay -url amqp://... -queue alkemio-collaboration-lifecycle [-limit N] [-dry-run]
//
// Fix the underlying failure FIRST. A replay into a still-broken backend walks the
// whole ladder again and returns to the dead-letter queue about 35 minutes later.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/alkem-io/collaboration-service/internal/adapter/inbound/lifecycle"
)

func main() {
	url := flag.String("url", os.Getenv("RABBITMQ_URL"), "amqp:// connection string (default $RABBITMQ_URL)")
	queue := flag.String("queue", os.Getenv("RABBITMQ_LIFECYCLE_QUEUE"), "the MAIN lifecycle queue; the dead-letter queue is derived from it (default $RABBITMQ_LIFECYCLE_QUEUE)")
	limit := flag.Int("limit", 0, "maximum events to move; 0 means everything currently queued")
	dryRun := flag.Bool("dry-run", false, "report how many events are waiting, move nothing")
	flag.Parse()

	if err := run(*url, *queue, *limit, *dryRun); err != nil {
		fmt.Fprintf(os.Stderr, "lifecycle-replay: %v\n", err)
		os.Exit(1)
	}
}

func run(url, queue string, limit int, dryRun bool) error {
	if url == "" {
		return fmt.Errorf("-url is required (or set RABBITMQ_URL)")
	}
	if queue == "" {
		return fmt.Errorf("-queue is required (or set RABBITMQ_LIFECYCLE_QUEUE)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if dryRun {
		n, err := lifecycle.DeadLetterDepth(ch, queue)
		if err != nil {
			return err
		}
		fmt.Printf("%d event(s) waiting in the dead-letter queue for %s\n", n, queue)
		return nil
	}

	res, err := lifecycle.Replay(ctx, ch, queue, limit)
	// Report what moved even when the run stopped early: the events already
	// republished are on the main queue whether or not the rest made it.
	fmt.Printf("replayed %d event(s) onto %s\n", res.Moved, queue)
	if res.Remaining {
		fmt.Println("the dead-letter queue still holds events; run again once you are satisfied with the result")
	}
	return err
}
