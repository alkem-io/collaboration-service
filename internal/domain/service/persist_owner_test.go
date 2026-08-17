package service

import (
	"context"
	"testing"
	"time"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestPersistCarriesOwnerRefForward asserts the root-cause fix for the data-
// integrity finding: Room.persist round-trips OwnerRef. A document pre-registered
// with an OwnerRef (the delete cascade key, FR-023) must keep that owner_ref after
// the room materializes and persists its first snapshot — the room loads it in
// loadSnapshot and re-persists it, rather than rebuilding Metadata with a blank
// OwnerRef and dropping it. This defends every MetadataStore backend (the
// in-memory store does a wholesale row replace, so a dropped OwnerRef here would
// be silently wiped to "").
func TestPersistCarriesOwnerRefForward(t *testing.T) {
	const docID = "doc-owned-by-callout"
	const owner = "callout-42"

	meta := metainmem.New()
	open := authopen.New()

	// Pre-register the document (document.created / standalone create): the
	// lifecycle owner is set, but there is no snapshot yet.
	if err := meta.Save(context.Background(), model.Metadata{
		ID:          docID,
		ContentType: model.ContentTypeMemo,
		OwnerRef:    owner,
	}); err != nil {
		t.Fatalf("pre-register metadata: %v", err)
	}

	mgr := NewManager(Deps{Metadata: meta, Blob: blobinline.New(), Auth: open, AuthZ: open}, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   64,
	}, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, docID, model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("edit ")
	waitFor(t, "snapshot persisted", func() bool { return hasControlKind(a, model.ControlSaved) })

	// After the first snapshot save, the metadata row must still carry the
	// pre-registered owner_ref the delete cascade keys off.
	saved, err := meta.Load(context.Background(), docID)
	if err != nil {
		t.Fatalf("reload metadata: %v", err)
	}
	if saved.OwnerRef != owner {
		t.Fatalf("snapshot persist dropped OwnerRef: got %q, want preserved %q", saved.OwnerRef, owner)
	}
	if saved.ContentPointer == "" {
		t.Fatalf("snapshot persist did not set a content pointer (sanity): %+v", saved)
	}
}
