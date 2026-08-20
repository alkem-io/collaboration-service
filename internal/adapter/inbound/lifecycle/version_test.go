package lifecycle

import (
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestVersionFloorRejectsABrokerThatSilentlyIgnoresTheTopology is the guard whose
// absence would ship a retry tier that never drains.
//
// 3.9.13 accepts every argument this topology declares, echoes them back on
// inspection, and never expires a message — measured, not assumed. So the only
// symptom of running below the floor is that retries stop happening, which no
// amount of logging at WARN would surface to anyone.
func TestVersionFloorRejectsABrokerThatSilentlyIgnoresTheTopology(t *testing.T) {
	err := requireVersionFloor(amqp.Table{"version": "3.9.13"})
	if err == nil {
		t.Fatal("a 3.9.13 broker must be refused; it accepts the arguments and never expires anything")
	}
	if !strings.Contains(err.Error(), MinBrokerVersion.String()) {
		t.Fatalf("the error must name the required version so the fix is actionable, got %v", err)
	}
}

// TestVersionFloorFailsClosedOnAnUnreadableVersion: a broker that cannot be shown
// to support the topology is treated exactly like one that does not. Defaulting to
// "probably fine" reintroduces the silent failure the floor exists to prevent.
func TestVersionFloorFailsClosedOnAnUnreadableVersion(t *testing.T) {
	for _, props := range []amqp.Table{
		{},              // no version key at all
		{"version": ""}, // empty
		{"version": "not-a-version"},
		{"version": 3.13}, // wrong type
	} {
		if err := requireVersionFloor(props); err == nil {
			t.Fatalf("props %v must be refused: an unreadable version cannot be shown to support the topology", props)
		}
	}
}

// TestVersionFloorAcceptsTheProvenBaselineAndAbove guards the other side: a check
// that refused everything would satisfy the tests above while making the service
// unstartable.
func TestVersionFloorAcceptsTheProvenBaselineAndAbove(t *testing.T) {
	for _, v := range []string{
		"3.13.2",            // the exact proven baseline
		"3.13.2-management", // vendor suffix
		"3.13.7",
		"3.14.0",
		"4.0.0",
		"4.0.0~rc.1", // pre-release suffix
		"3.13.2+build7",
	} {
		if err := requireVersionFloor(amqp.Table{"version": v}); err != nil {
			t.Fatalf("version %q must be accepted: %v", v, err)
		}
	}
}

// TestVersionFloorRejectsBelowTheBaselineOnEveryComponent pins the comparison
// itself: a floor that only compared the major would accept 3.9, and one that only
// compared major+minor would accept 3.13.1.
func TestVersionFloorRejectsBelowTheBaselineOnEveryComponent(t *testing.T) {
	for _, v := range []string{"2.99.99", "3.12.99", "3.13.1"} {
		if err := requireVersionFloor(amqp.Table{"version": v}); err == nil {
			t.Fatalf("version %q is below the proven baseline and must be refused", v)
		}
	}
}

// TestBrokerVersionParsingHandlesWhatBrokersActuallyReport asserts the parser is
// total over the shapes a real server_properties version string takes, and that
// anything it cannot read is an ERROR rather than a zero version.
//
// A zero version would compare as below the floor and refuse to start, which
// happens to be safe — but it would report "broker is RabbitMQ 0.0.0", which sends
// whoever reads the log looking for a broker problem that does not exist.
func TestBrokerVersionParsingHandlesWhatBrokersActuallyReport(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want brokerVersion
	}{
		{"3.13.2", brokerVersion{3, 13, 2}},
		{"4.0.0", brokerVersion{4, 0, 0}},
		{"3.13.2-management", brokerVersion{3, 13, 2}},
		{"3.13.2~rc.1", brokerVersion{3, 13, 2}},
		{"3.13.2+build7", brokerVersion{3, 13, 2}},
		{"3.13", brokerVersion{3, 13, 0}},
		{"4", brokerVersion{4, 0, 0}},
		{"3.13.", brokerVersion{3, 13, 0}},
		// More components than we track: the extra is ignored, not an error.
		{"3.13.2.5", brokerVersion{3, 13, 2}},
	} {
		got, err := parseBrokerVersion(tc.raw)
		if err != nil {
			t.Errorf("parseBrokerVersion(%q) errored: %v", tc.raw, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseBrokerVersion(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}

	for _, raw := range []string{"", "rabbit", "-3.13.2", "..", "."} {
		if got, err := parseBrokerVersion(raw); err == nil {
			t.Errorf("parseBrokerVersion(%q) = %v with no error; an unreadable version must be reported as such, not as 0.0.0", raw, got)
		}
	}
}
