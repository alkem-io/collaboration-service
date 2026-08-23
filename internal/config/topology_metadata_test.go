package config

import (
	"strings"
	"testing"
)

// TestADurableBlobStoreBehindAnEphemeralIndexIsRejected is the regression for a
// total, silent, non-retrying outage reachable by mistyping one variable.
//
// METADATA_STORE is the only backend selector that still defaults (to inmemory).
// An operator who sets CHECKPOINT_STORE=file-service and omits, renames or
// misspells METADATA_STORE gets a durable blob store behind an index that is
// EMPTY on every boot. Join refuses every id it has not heard of, and that
// refusal is deliberately rendered as close 1008 "forbidden" — which client-web
// classifies as terminal and never retries. /healthz stays green; the startup log
// prints a plausible-but-wrong topology. Nothing anywhere says "your index is
// empty".
//
// HUB_MODE and CHECKPOINT_STORE were both made mandatory for this exact failure
// mode, with the reasoning written out at their parsers. This applies the same
// reasoning to the pair.
func TestADurableBlobStoreBehindAnEphemeralIndexIsRejected(t *testing.T) {
	cfg := &Config{
		HubMode:         HubInMemory,
		CheckpointStore: CheckpointStoreFileService,
		MetadataStore:   MetadataStoreInMemory,
	}
	err := rejectEphemeralIndexBehindDurableBlobs(cfg)
	if err == nil {
		t.Fatal("file-service checkpoints with an in-memory index booted clean; every connection is refused as unknown, reported as a permission failure, and never retried")
	}
	// The message has to name the fix, because the symptom points at authorization.
	for _, want := range []string{"METADATA_STORE=rabbitmq", "CHECKPOINT_STORE=inline"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name %q; the observable symptom is a permission failure, so the message is the only thing that points at the real cause:\n%v", want, err)
		}
	}
}

// TestTheIntentionalStandaloneTopologyStillBoots is the guard against fixing the
// above by banning the in-memory index outright. Standalone and test runs use it
// deliberately — with in-process blobs, where an empty index on boot is correct
// because the blobs are empty too.
func TestTheIntentionalStandaloneTopologyStillBoots(t *testing.T) {
	cfg := &Config{
		HubMode:         HubInMemory,
		CheckpointStore: CheckpointStoreInline,
		MetadataStore:   MetadataStoreInMemory,
	}
	if err := rejectEphemeralIndexBehindDurableBlobs(cfg); err != nil {
		t.Fatalf("the intentional zero-dependency standalone topology was rejected: %v", err)
	}
}

// TestTheDurableTopologyStillBoots pins the supported production pair.
func TestTheDurableTopologyStillBoots(t *testing.T) {
	cfg := &Config{
		HubMode:         HubInMemory,
		CheckpointStore: CheckpointStoreFileService,
		MetadataStore:   MetadataStoreRabbitMQ,
	}
	if err := rejectEphemeralIndexBehindDurableBlobs(cfg); err != nil {
		t.Fatalf("the supported durable topology was rejected: %v", err)
	}
}

// TestTheEphemeralIndexPairIsRejectedThroughLoad proves the check is actually
// WIRED, not merely present. A unit test on the predicate alone would still pass
// if nobody called it.
func TestTheEphemeralIndexPairIsRejectedThroughLoad(t *testing.T) {
	t.Setenv("HUB_MODE", "inmemory")
	t.Setenv("CHECKPOINT_STORE", "file-service")
	t.Setenv("METADATA_STORE", "inmemory")
	t.Setenv("FILE_SERVICE_URL", "http://file-service:4005")
	t.Setenv("FILE_SERVICE_STORAGE_BUCKET_ID", "bucket-1")
	t.Setenv("AUTH_MODE", "open")

	if _, err := Load(); err == nil {
		t.Fatal("Load accepted durable checkpoints behind an ephemeral index")
	} else if !strings.Contains(err.Error(), "METADATA_STORE=rabbitmq") {
		t.Fatalf("Load error does not name the fix: %v", err)
	}
}
