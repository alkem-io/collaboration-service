package service

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

//nolint:gocyclo // Subtests share the same room trace so only the release cause varies.
func TestPreviewReleaseRequiresDurableChangedContent(t *testing.T) {
	// The already-saved release case is covered on a fake clock by
	// TestPreviewRefreshCoalescesSuccessfulSavesWhileOpenAndFinishesOnRelease.
	for _, mode := range []string{"persist on release", "unchanged", "failed save", "memo", "owner deletion", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			cfg := fastConfig()
			cfg.IdleTimeout = 0
			if mode == "persist on release" || mode == "owner deletion" || mode == "shutdown" {
				cfg.SaveDebounce = time.Hour
			}
			manager, deps := testManager(t, cfg)
			if mode == "failed save" {
				// With no periodic retry, this case still reaches idle
				// teardown, but must leave the existing preview alone.
				manager.cfg.SaveDebounce = 0
				manager.deps.Checkpoint = newFailingStore()
			}
			t.Cleanup(manager.Close)
			events := make(chan previewCandidate, 2)
			manager.deps.offerPreview = func(candidate previewCandidate) { events <- candidate }
			client := newFakeClient(t)
			id := model.DocumentID("preview-test-" + mode)
			content := model.ContentTypeWhiteboard
			if mode == "memo" {
				content = model.ContentTypeMemo
			}
			client.join(manager, id, content)
			client.observeUpdates()
			if mode != "unchanged" {
				if mode == "memo" {
					client.insertText("note")
				} else {
					client.addElement("live", map[string]interface{}{"type": "rectangle"})
				}
			}
			switch mode {
			case "owner deletion":
				if err := manager.CloseDeleted(t.Context(), id); err != nil {
					t.Fatal(err)
				}
			case "shutdown":
				manager.Close()
			default:
				client.session.Leave()
			}
			waitFor(t, "room release", func() bool { return manager.RoomCount() == 0 })
			if mode != "persist on release" && mode != "memo" {
				if len(events) != 0 {
					t.Fatal("unexpected preview candidate")
				}
				return
			}
			state, err := deps.storedState(t.Context(), string(id))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case c := <-events:
				if c.id != id || c.kind != content || c.fingerprint != sha256.Sum256(state) {
					t.Fatalf("release offered something other than saved state: %+v", c)
				}
			default:
				t.Fatal("ordinary changed durable release must enqueue one preview")
			}
			if len(events) != 0 {
				t.Fatal("duplicate preview offer")
			}
		})
	}
}

func TestPreviewRefreshCoalescesSuccessfulSavesWhileOpenAndFinishesOnRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := fastConfig()
		cfg.IdleTimeout = 0
		manager, deps := testManager(t, cfg)
		defer manager.Close()
		events := make(chan previewCandidate, 6)
		manager.deps.offerPreview = func(c previewCandidate) { events <- c }
		client := newFakeClient(t)
		const id = model.DocumentID("preview-open")
		client.join(manager, id, model.ContentTypeWhiteboard)
		client.observeUpdates()
		save := func(label string) {
			t.Helper()
			client.addElement(label, map[string]interface{}{"type": "rectangle"})
			requestDurability(client, label)
			synctest.Wait()
			time.Sleep(cfg.SaveDebounce)
			synctest.Wait()
			if got := barrierOutcome(client, label); got == nil || got.Kind != model.ControlPersisted {
				t.Fatalf("save %s was not accepted: %+v", label, got)
			}
		}
		expectLatest := func() previewCandidate {
			t.Helper()
			synctest.Wait()
			state, err := deps.storedState(t.Context(), string(id))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case c := <-events:
				if c.id != id || c.kind != model.ContentTypeWhiteboard || c.fingerprint != sha256.Sum256(state) {
					t.Fatalf("preview did not represent latest saved content: %+v", c)
				}
				return c
			default:
				t.Fatal("expected a preview candidate")
				return previewCandidate{}
			}
		}
		save("first")
		time.Sleep(3 * time.Second)
		save("second")
		if len(events) != 0 {
			t.Fatal("render scheduled for individual saves")
		}
		time.Sleep(2*time.Second - cfg.SaveDebounce)
		first := expectLatest()
		if manager.RoomCount() != 1 {
			t.Fatal("preview activity affected the open room")
		}
		save("third")
		time.Sleep(5 * time.Second)
		second := expectLatest()
		if second.fingerprint == first.fingerprint {
			t.Fatal("next window did not advance to the next save")
		}
		save("fourth")
		client.session.Leave()
		synctest.Wait()
		time.Sleep(time.Nanosecond) // zero idle grace releases on the next timer tick
		final := expectLatest()
		if !final.final || final.fingerprint == second.fingerprint || manager.RoomCount() != 0 {
			t.Fatal("the normal release did not submit the newer save ahead of its window")
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if len(events) != 0 {
			t.Fatal("release left behind a preview timer")
		}
	})
}

func TestPreviewReleaseDoesNotRepeatAPublishedRevision(t *testing.T) {
	for _, kind := range []model.ContentType{model.ContentTypeMemo, model.ContentTypeWhiteboard} {
		t.Run(string(kind), func(t *testing.T) { checkPreviewReleaseDoesNotRepeat(t, kind) })
	}
}

func checkPreviewReleaseDoesNotRepeat(t *testing.T, kind model.ContentType) {
	synctest.Test(t, func(t *testing.T) {
		cfg := fastConfig()
		manager, _ := testManager(t, cfg)
		defer manager.Close()
		offers := 0
		manager.deps.offerPreview = func(c previewCandidate) {
			offers++
			c.published.Store(true)
		}
		client := newFakeClient(t)
		client.join(manager, "preview-published", kind)
		client.observeUpdates()
		if kind == model.ContentTypeMemo {
			client.insertText("published memo")
		} else {
			client.addElement("published-shape", map[string]any{"type": "rectangle"})
		}
		synctest.Wait()
		time.Sleep(cfg.SaveDebounce + previewSaveWindow)
		synctest.Wait()
		if offers != 1 {
			t.Fatalf("open-room offers = %d, want 1", offers)
		}
		client.session.Leave()
		synctest.Wait()
		time.Sleep(cfg.IdleTimeout)
		synctest.Wait()
		if manager.RoomCount() != 0 || offers != 1 {
			t.Fatalf("rooms = %d, offers = %d; release must not repeat a published revision", manager.RoomCount(), offers)
		}
		// Reopening consumes the persisted checkpoint, not the in-memory
		// receipt. Treat that checkpoint as the initial revision so the
		// next read-only visit doesn't accidentally trigger another write.
		reopened := newFakeClient(t)
		reopened.joinExisting(manager, "preview-published", kind)
		synctest.Wait()
		if kind == model.ContentTypeMemo {
			if got := reopened.text(); got != "published memo" {
				t.Fatalf("reopen did not use the saved memo: %q", got)
			}
		} else {
			reopened.withDoc(func(doc *ycrdt.Doc) {
				if !hasElement(doc, "published-shape") {
					t.Fatal("reopen did not use the saved whiteboard")
				}
			})
		}
		reopened.session.Leave()
		synctest.Wait()
		time.Sleep(cfg.IdleTimeout)
		synctest.Wait()
		if manager.RoomCount() != 0 || offers != 1 {
			t.Fatalf("unchanged cold reopen re-published: rooms=%d offers=%d", manager.RoomCount(), offers)
		}
	})
}

func TestPendingSaveWindowDoesNotPublishAfterForcedTeardown(t *testing.T) {
	for _, reason := range []string{"owner deletion", "shutdown"} {
		t.Run(reason, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := fastConfig()
				manager, _ := testManager(t, cfg)
				defer manager.Close()
				manager.deps.offerPreview = func(previewCandidate) { t.Error("forced teardown left a live preview window") }
				client := newFakeClient(t)
				const id = model.DocumentID("preview-window-cancel")
				client.join(manager, id, model.ContentTypeMemo)
				client.observeUpdates()
				client.insertText("saved before teardown")
				requestDurability(client, "saved")
				synctest.Wait()
				time.Sleep(cfg.SaveDebounce)
				synctest.Wait()
				if got := barrierOutcome(client, "saved"); got == nil || got.Kind != model.ControlPersisted {
					t.Fatalf("preview window was not armed by a successful save: %+v", got)
				}
				if reason == "owner deletion" {
					if err := manager.CloseDeleted(t.Context(), id); err != nil {
						t.Fatal(err)
					}
				} else {
					manager.Close()
				}
				time.Sleep(previewSaveWindow)
				synctest.Wait()
				if manager.RoomCount() != 0 {
					t.Fatal("forced teardown left a room registered")
				}
			})
		})
	}
}

func TestUnreadableCheckpointCannotStartARoomOrGenerateAReplacement(t *testing.T) {
	manager, deps := testManager(t, fastConfig())
	t.Cleanup(manager.Close)
	const id = model.DocumentID("corrupt-retain-last-content")
	if err := manager.PreRegister(t.Context(), model.Metadata{
		ID: id, ContentType: model.ContentTypeWhiteboard, ContentPointer: string(id), Migrated: true,
	}); err != nil {
		t.Fatal(err)
	}
	unreadable := []byte("bad persisted v2 encoding")
	if err := deps.putState(t.Context(), string(id), unreadable); err != nil {
		t.Fatal(err)
	}
	manager.deps.offerPreview = func(previewCandidate) { t.Error("unreadable state scheduled derivative work") }
	client := newFakeClient(t)
	_, _, err := manager.Join(t.Context(), JoinRequest{ID: id, Content: model.ContentTypeWhiteboard, Conn: client})
	if err == nil || manager.RoomCount() != 0 {
		t.Fatalf("corrupt state created a room: %d, %v", manager.RoomCount(), err)
	}
	got, readErr := deps.storedState(t.Context(), string(id))
	if readErr != nil || !bytes.Equal(got, unreadable) {
		t.Fatalf("original unreadable bytes changed: %q, %v", got, readErr)
	}
}

// A close-triggered derivative must use the last durable bytes, not the state
// vector or the last periodic save. Verify what the normal room release makes
// available after a delete-only edit, before any derivative code participates.
func TestWhiteboardReleaseStoresClearWithUnchangedStateVector(t *testing.T) {
	cfg := fastConfig()
	cfg.SaveDebounce = time.Hour // normal room release is the only save here
	cfg.IdleTimeout = 0
	manager, deps := testManager(t, cfg)
	t.Cleanup(manager.Close)
	const id model.DocumentID = "release-clear"

	seed := newRoomDoc("seed-clear")
	t.Cleanup(seed.Destroy)
	addElement(seed, "last", map[string]interface{}{"type": "rectangle"})
	snapshot, err := ycrdt.EncodeStateAsUpdateV2(seed, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := deps.putState(t.Context(), string(id), snapshot); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient(t)
	client.join(manager, id, model.ContentTypeWhiteboard)
	waitFor(t, "seed reaches the client", func() bool {
		var found bool
		client.withDoc(func(doc *ycrdt.Doc) { found = hasElement(doc, "last") })
		return found
	})
	client.observeUpdates()
	read := func() []byte {
		t.Helper()
		snapshot, err := deps.storedState(t.Context(), string(id))
		if err != nil {
			t.Fatalf("load checkpoint: %v", err)
		}
		return snapshot
	}
	before := read()
	client.withDoc(func(doc *ycrdt.Doc) { doc.GetMap("elements").Delete("last") })
	stillSaved := read()
	if !bytes.Equal(stillSaved, before) {
		t.Fatal("expected no durable edit before the final release flush")
	}
	client.session.Leave()
	waitFor(t, "room release", func() bool { return manager.RoomCount() == 0 })

	after := read()
	if bytes.Equal(before, after) {
		t.Fatal("final release did not change the checkpoint")
	}
	oldVector, err := ycrdt.EncodeStateVectorFromUpdateV2(before)
	if err != nil {
		t.Fatal(err)
	}
	newVector, err := ycrdt.EncodeStateVectorFromUpdateV2(after)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(oldVector, newVector) {
		t.Fatal("fixture no longer isolates a delete-set-only update")
	}
	decoded := newRoomDoc("verify-clear")
	t.Cleanup(decoded.Destroy)
	if err := ycrdt.ApplyUpdateV2(decoded, after, nil); err != nil {
		t.Fatal(err)
	}
	if got := elementsLen(decoded); got != 0 {
		t.Fatalf("cleared snapshot has %d elements", got)
	}
}

// A short leave followed by a join is not a close with a saved revision.
// The new editor sees the same unsaved room, and a later ordinary release
// makes those bytes durable without an extra durability request.
func TestWhiteboardRejoinDuringIdleGraceKeepsTheUnflushedRoom(t *testing.T) {
	synctest.Test(t, checkWhiteboardRejoinDuringIdleGrace)
}

func checkWhiteboardRejoinDuringIdleGrace(t *testing.T) {
	cfg := fastConfig()
	cfg.SaveDebounce = time.Hour
	cfg.IdleTimeout = time.Second
	manager, deps := testManager(t, cfg)
	t.Cleanup(manager.Close)
	const id model.DocumentID = "release-grace"

	a := newFakeClient(t)
	a.join(manager, id, model.ContentTypeWhiteboard)
	a.observeUpdates()
	a.addElement("new", map[string]interface{}{"type": "rectangle"})
	a.session.Leave()
	synctest.Wait()

	b := newFakeClient(t)
	b.joinExisting(manager, id, model.ContentTypeWhiteboard)
	waitFor(t, "in-memory edit after rejoin", func() bool {
		var found bool
		b.withDoc(func(doc *ycrdt.Doc) { found = hasElement(doc, "new") })
		return found
	})
	if _, err := deps.storedState(t.Context(), string(id)); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("grace-period reconnect unexpectedly saved the room: %v", err)
	}
	b.session.Leave()
	synctest.Wait()
	time.Sleep(cfg.IdleTimeout)
	waitFor(t, "normal idle release", func() bool { return manager.RoomCount() == 0 })
	checkpoint, err := deps.storedState(t.Context(), string(id))
	if err != nil {
		t.Fatalf("idle release left no checkpoint: %v", err)
	}
	decoded := newRoomDoc("verify-grace")
	t.Cleanup(decoded.Destroy)
	if err := ycrdt.ApplyUpdateV2(decoded, checkpoint, nil); err != nil || !hasElement(decoded, "new") {
		t.Fatalf("normal close did not persist the grace-period edit: %v", err)
	}
}
