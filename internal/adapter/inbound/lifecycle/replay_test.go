package lifecycle

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestAReplayAckNeverOutvotesAReturnThatIsAlreadyWaiting is the same race as in
// transfer, on the path where losing it is worst.
//
// A mandatory publish to a missing queue produces basic.return then basic.ack. By
// the time republish reaches its select the connection reader may have dispatched
// both, and Go picks at random between two ready cases — so roughly half the time
// the confirm wins and the replay reports success. The caller then acks the
// dead-letter copy, which is the LAST copy of an event that was published nowhere.
//
// A real broker cannot be made to lose this race on demand; the fake delivers both
// answers before the publish call returns, which is exactly the state. Repeated,
// because one pass could win the coin toss.
func TestAReplayAckNeverOutvotesAReturnThatIsAlreadyWaiting(t *testing.T) {
	for i := 0; i < 200; i++ {
		ch := &fakeChannel{}
		ch.confirmAck = true
		ch.returnOnPub = true
		ch.answersBeforePublishReturns = true
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		returns := ch.NotifyReturn(make(chan amqp.Return, 1))

		err := republish(context.Background(), ch, confirms, returns, "lifecycle-q",
			amqp.Delivery{Body: []byte(`{"pattern":"document.deleted","data":{"id":"d"}}`)})
		if err == nil {
			t.Fatalf("iteration %d: republish reported success for a publish the broker RETURNED as unroutable. The caller acks the dead-letter copy on success, and that copy is the last one in existence", i)
		}
	}
}

// TestAReplayClearsTheAttemptCountAndCountsItself pins the two header edits at the
// unit level, so the intent is readable without a broker.
//
// Clearing the attempt count is what gives a replayed event the whole ladder again
// instead of one failure and a return trip to the dead-letter queue. Counting the
// replay is what keeps that from erasing the event's history: after the clear,
// nothing else distinguishes an event a person has already sent round three times
// from one arriving for the first time.
func TestAReplayClearsTheAttemptCountAndCountsItself(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   amqp.Table
		want int32
	}{
		{"straight off the ladder", amqp.Table{headerAttempt: tierCount}, 1},
		{"replayed before", amqp.Table{headerAttempt: tierCount, headerReplays: int32(2)}, 3},
		{"replay count published as a wider int", amqp.Table{headerReplays: int64(9)}, 10},
		{"replay count is garbage", amqp.Table{headerReplays: "many"}, 1},
		{"no headers at all", nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ch := &fakeChannel{confirmAck: true}
			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
			returns := ch.NotifyReturn(make(chan amqp.Return, 1))

			if err := republish(context.Background(), ch, confirms, returns, "lifecycle-q",
				amqp.Delivery{Headers: tc.in, Body: []byte(`{"pattern":"document.deleted"}`)}); err != nil {
				t.Fatalf("republish: %v", err)
			}

			pubs := ch.publishes()
			if len(pubs) != 1 {
				t.Fatalf("published %d message(s), want 1", len(pubs))
			}
			if _, present := pubs[0].msg.Headers[headerAttempt]; present {
				t.Fatalf("the replayed event still carries %s = %v; it would fail once and go straight back to the dead-letter queue",
					headerAttempt, pubs[0].msg.Headers[headerAttempt])
			}
			if got := replaysOf(pubs[0].msg.Headers); got != tc.want {
				t.Fatalf("%s = %d, want %d", headerReplays, got, tc.want)
			}
			if !pubs[0].mandatory {
				t.Fatal("the replay published without mandatory; on the default exchange an unroutable publish is a silent discard that still confirms, so the event would be destroyed")
			}
			if pubs[0].key != "lifecycle-q" {
				t.Fatalf("republished to %q, want the main queue", pubs[0].key)
			}
		})
	}
}

// TestTheReplayCounterSurvivesWhateverArrives keeps the routing decision total
// over a header that crosses a wire, the same way attemptOf is. A count that
// wrapped negative on a corrupt value would read as "never replayed" and send an
// operator round the loop they have already been round.
func TestTheReplayCounterSurvivesWhateverArrives(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want int32
	}{
		{"absent", nil, 0},
		{"zero", int32(0), 0},
		{"typical", int32(3), 3},
		{"wider int", int64(7), 7},
		{"byte", uint8(2), 2},
		{"short", int16(5), 5},
		{"plain int", int(6), 6},
		{"negative", int32(-4), 0},
		{"absurdly large", int64(1) << 40, 1 << 20},
		{"not a number", []byte("x"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var h amqp.Table
			if tc.in != nil {
				h = amqp.Table{headerReplays: tc.in}
			}
			if got := replaysOf(h); got != tc.want {
				t.Fatalf("replaysOf(%v) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// TestReplayRefusesToStartWithoutTheGuaranteesItDependsOn asserts each setup step
// is a hard precondition, not best effort.
//
// Without confirms the republish cannot be verified, so the dead-letter copy would
// be acked on hope. Without a bounded QoS the broker streams the whole dead-letter
// queue into memory unacknowledged, and a failure part-way leaves an unknown number
// of events in limbo instead of exactly one. Without a consumer there is nothing to
// replay. Reporting success — or moving anything — after any of these fails would
// mean acking events that were never republished.
func TestReplayRefusesToStartWithoutTheGuaranteesItDependsOn(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*fakeChannel)
	}{
		{"publisher confirms unavailable", func(f *fakeChannel) { f.confirmErr = errTestBroker }},
		{"prefetch cannot be bounded", func(f *fakeChannel) { f.qosErr = errTestBroker }},
		{"the dead-letter queue cannot be consumed", func(f *fakeChannel) { f.consumeErr = errTestBroker }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ch := &fakeChannel{confirmAck: true}
			tc.set(ch)

			res, err := Replay(context.Background(), ch, "lifecycle-q", 0)
			if err == nil {
				t.Fatal("Replay reported success without the guarantee it depends on")
			}
			if res.Moved != 0 {
				t.Fatalf("Replay moved %d event(s) after failing to start", res.Moved)
			}
			if pubs := ch.publishes(); len(pubs) != 0 {
				t.Fatalf("Replay published %d message(s) after failing to start", len(pubs))
			}
		})
	}
}

// TestReplayStopsAtItsLimit asserts -limit is a real bound and that a stopped run
// says so, since the remaining events are still in the dead-letter queue and the
// operator has to come back for them.
func TestReplayStopsAtItsLimit(t *testing.T) {
	ch := &fakeChannel{confirmAck: true}
	res, err := Replay(context.Background(), ch, "lifecycle-q", 2, func() {
		for i := 0; i < 5; i++ {
			ch.deliver(amqp.Delivery{
				Body:         []byte(`{"pattern":"document.deleted","data":{"id":"d"}}`),
				Acknowledger: &fakeAcker{}, DeliveryTag: uint64(i + 1),
			})
		}
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Moved != 2 {
		t.Fatalf("Replay moved %d event(s), want the limit of 2", res.Moved)
	}
	if !res.Remaining {
		t.Fatal("Replay stopped at its limit but did not report events remaining; the operator would believe the queue was drained")
	}
	if pubs := ch.publishes(); len(pubs) != 2 {
		t.Fatalf("published %d message(s), want 2", len(pubs))
	}
}

// TestReplayReportsWhatItMovedBeforeFailing asserts a run that fails part-way still
// reports the events it already republished. They are on the main queue whether or
// not the rest made it, and an operator who reads "0 moved" after a partial run
// will replay them a second time.
func TestReplayReportsWhatItMovedBeforeFailing(t *testing.T) {
	// The broker acks the first republish and nacks the second, so the run fails
	// at a chosen point rather than racing the loop it is driving.
	ch := &fakeChannel{confirmAck: true, nackFromPublish: 2}
	res, err := Replay(context.Background(), ch, "lifecycle-q", 0, func() {
		for i, id := range []string{"a", "b"} {
			ch.deliver(amqp.Delivery{
				Body:         []byte(`{"pattern":"document.deleted","data":{"id":"` + id + `"}}`),
				Acknowledger: &fakeAcker{},
				DeliveryTag:  uint64(i + 1),
			})
		}
	})
	if err == nil {
		t.Fatal("Replay reported success after a republish the broker nacked")
	}
	if res.Moved != 1 {
		t.Fatalf("Replay reported %d moved, want the 1 it actually republished before failing (published %d)", res.Moved, len(ch.publishes()))
	}
	if !res.Remaining {
		t.Fatal("Replay failed part-way but did not report events remaining")
	}
}

// TestRepublishGivesUpOnASilentBroker asserts the wait for an answer is bounded.
// A broker that neither confirms nor returns must not hold a replay open forever
// with the dead-letter copy unacknowledged and the operator watching a stalled
// command.
func TestRepublishGivesUpOnASilentBroker(t *testing.T) {
	ch := &fakeChannel{}
	// No answers at all: NotifyPublish/NotifyReturn are wired but nothing arrives.
	confirms := make(chan amqp.Confirmation, 1)
	returns := make(chan amqp.Return, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := republish(ctx, ch, confirms, returns, "lifecycle-q", amqp.Delivery{Body: []byte("{}")})
	if err == nil {
		t.Fatal("republish returned success from a broker that never answered")
	}
}

// TestRepublishTreatsAClosedAnswerChannelAsFailure asserts a channel that dies
// mid-publish is a failure rather than a success. amqp091 closes both notify
// channels when the channel faults; reading a zero value from one and treating it
// as an answer would ack the last copy of an event on a dead channel.
func TestRepublishTreatsAClosedAnswerChannelAsFailure(t *testing.T) {
	t.Run("confirms closed", func(t *testing.T) {
		confirms := make(chan amqp.Confirmation, 1)
		returns := make(chan amqp.Return, 1)
		close(confirms)
		if err := republish(context.Background(), &fakeChannel{}, confirms, returns, "q",
			amqp.Delivery{Body: []byte("{}")}); err == nil {
			t.Fatal("a closed confirm channel was read as an acknowledgement")
		}
	})
	t.Run("returns closed", func(t *testing.T) {
		confirms := make(chan amqp.Confirmation, 1)
		returns := make(chan amqp.Return, 1)
		close(returns)
		if err := republish(context.Background(), &fakeChannel{}, confirms, returns, "q",
			amqp.Delivery{Body: []byte("{}")}); err == nil {
			t.Fatal("a closed return channel was read as 'nothing was returned'")
		}
	})
}

// TestRepublishSurfacesAPublishError asserts a publish that never left the process
// is reported rather than waited on.
func TestRepublishSurfacesAPublishError(t *testing.T) {
	ch := &fakeChannel{publishErr: errTestBroker}
	if err := republish(context.Background(), ch, make(chan amqp.Confirmation, 1),
		make(chan amqp.Return, 1), "q", amqp.Delivery{Body: []byte("{}")}); err == nil {
		t.Fatal("a failed publish was reported as success")
	}
}

// TestReplayOnlyReportsTheQueueDrainedWhenItActuallyIs asserts every early exit
// reports work remaining, and that only a quiet queue reports none.
//
// This is the flag an operator reads to decide whether to run again. Getting it
// wrong in the safe direction costs a second run that finds nothing; getting it
// wrong the other way means events sit in the dead-letter queue because the tool
// said the queue was empty. It was previously set per-exit and three of the five
// paths missed it.
func TestReplayOnlyReportsTheQueueDrainedWhenItActuallyIs(t *testing.T) {
	deleted := []byte(`{"pattern":"document.deleted","data":{"id":"d"}}`)

	t.Run("a quiet queue is drained", func(t *testing.T) {
		ch := &fakeChannel{confirmAck: true}
		res, err := Replay(context.Background(), ch, "lifecycle-q", 0)
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}
		if res.Remaining {
			t.Fatal("an empty dead-letter queue was reported as having events remaining")
		}
	})

	t.Run("a cancelled run leaves work", func(t *testing.T) {
		ch := &fakeChannel{confirmAck: true}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		res, err := Replay(ctx, ch, "lifecycle-q", 0)
		if err == nil {
			t.Fatal("a cancelled Replay reported success")
		}
		if !res.Remaining {
			t.Fatal("a cancelled run reported the queue drained; the operator stops looking and the events stay put")
		}
	})

	t.Run("a closed delivery stream leaves work", func(t *testing.T) {
		ch := &fakeChannel{confirmAck: true}
		res, err := Replay(context.Background(), ch, "lifecycle-q", 0, func() {
			ch.mu.Lock()
			d := ch.deliveries
			ch.deliveries = nil
			ch.mu.Unlock()
			close(d)
		})
		if err == nil {
			t.Fatal("Replay reported success after its delivery stream closed under it")
		}
		if !res.Remaining {
			t.Fatal("a run whose channel died reported the queue drained")
		}
	})

	t.Run("a failed republish leaves work", func(t *testing.T) {
		ch := &fakeChannel{confirmAck: false}
		res, err := Replay(context.Background(), ch, "lifecycle-q", 0, func() {
			ch.deliver(amqp.Delivery{Body: deleted, Acknowledger: &fakeAcker{}, DeliveryTag: 1})
		})
		if err == nil {
			t.Fatal("Replay reported success after a nacked republish")
		}
		if !res.Remaining {
			t.Fatal("a run that could not republish reported the queue drained")
		}
	})

	t.Run("a run that could not start leaves work", func(t *testing.T) {
		ch := &fakeChannel{confirmAck: true, confirmErr: errTestBroker}
		res, err := Replay(context.Background(), ch, "lifecycle-q", 0)
		if err == nil {
			t.Fatal("Replay reported success without publisher confirms")
		}
		if !res.Remaining {
			t.Fatal("a run that never started reported the queue drained, having not looked at it")
		}
	})
}
