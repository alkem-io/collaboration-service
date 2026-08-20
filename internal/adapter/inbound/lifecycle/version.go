package lifecycle

import (
	"fmt"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MinBrokerVersion is the lowest RabbitMQ release this topology is PROVEN to work
// on. It is the exact version the behavioural pin was run against, not a line or a
// guess: quorum queues with x-message-ttl and x-dead-letter-strategy=at-least-once
// were observed expiring into the main queue on 3.13.2.
//
// The floor exists because the failure below it is SILENT. On 3.9.13 the same
// declaration is accepted, the arguments are echoed back on inspection, and
// messages then never expire — the retry tier fills and never drains, with no
// error anywhere. A warning is inadmissible against a failure mode whose only
// symptom is nothing happening.
var MinBrokerVersion = brokerVersion{3, 13, 2}

type brokerVersion struct{ major, minor, patch int }

func (v brokerVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v.major, v.minor, v.patch)
}

// atLeast reports whether v is >= floor.
func (v brokerVersion) atLeast(floor brokerVersion) bool {
	if v.major != floor.major {
		return v.major > floor.major
	}
	if v.minor != floor.minor {
		return v.minor > floor.minor
	}
	return v.patch >= floor.patch
}

// parseBrokerVersion reads a RabbitMQ version string. Vendor suffixes are normal
// ("3.13.2-management", "4.0.0~rc.1", "3.13.2+build7"), so parsing stops at the
// first character that is not a digit or a dot rather than requiring a bare
// triple. A missing patch is treated as zero.
func parseBrokerVersion(raw string) (brokerVersion, error) {
	cut := strings.IndexFunc(raw, func(r rune) bool {
		return r != '.' && (r < '0' || r > '9')
	})
	if cut >= 0 {
		raw = raw[:cut]
	}
	parts := strings.Split(strings.TrimSuffix(raw, "."), ".")
	if len(parts) == 0 || parts[0] == "" {
		return brokerVersion{}, fmt.Errorf("unreadable version %q", raw)
	}
	var out [3]int
	for i := 0; i < 3 && i < len(parts); i++ {
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return brokerVersion{}, fmt.Errorf("unreadable version component %q", parts[i])
		}
		out[i] = n
	}
	return brokerVersion{out[0], out[1], out[2]}, nil
}

// requireVersionFloor fails closed: a broker that does not report a readable
// version is rejected exactly like one that is too old. An unreadable version
// cannot be shown to support the topology, and the whole point of the check is
// that an unsupported broker is indistinguishable from a working one at runtime.
func requireVersionFloor(props amqp.Table) error {
	raw, ok := props["version"].(string)
	if !ok || raw == "" {
		return fmt.Errorf(
			"broker did not report a readable version; this topology requires RabbitMQ >= %s "+
				"(below it, quorum queues accept x-message-ttl and never expire, so retries would never be redelivered)",
			MinBrokerVersion)
	}
	got, err := parseBrokerVersion(raw)
	if err != nil {
		return fmt.Errorf(
			"broker version %q is unreadable (%w); this topology requires RabbitMQ >= %s",
			raw, err, MinBrokerVersion)
	}
	if !got.atLeast(MinBrokerVersion) {
		return fmt.Errorf(
			"broker is RabbitMQ %s; this topology requires >= %s. Below that, a quorum queue ACCEPTS "+
				"x-message-ttl and x-dead-letter-strategy, reports them back, and never expires anything — "+
				"lifecycle retries would accumulate and never be redelivered, with no error",
			got, MinBrokerVersion)
	}
	return nil
}
