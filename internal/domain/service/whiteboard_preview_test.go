package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
	"github.com/alkem-io/collaboration-service/internal/domain/service/whiteboardrender"
)

func previewTestCandidate(id model.DocumentID, content string) previewCandidate {
	return previewCandidate{id: id, kind: model.ContentTypeWhiteboard, fingerprint: sha256.Sum256([]byte(content))}
}

func TestFinalPreviewOfferCoversOnlyAnUnsuccessfulActivePublication(t *testing.T) {
	for _, result := range []string{"success", "failed", "invalidated"} {
		t.Run(result, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := previewTestCandidate("released", "final saved content")
				c.published = new(atomic.Bool)
				resume := make(chan struct{})
				calls := 0
				worker := newPreviewWorker(1, func(ctx context.Context, got previewCandidate) {
					calls++
					if calls == 1 {
						select {
						case <-resume:
							if result == "success" {
								got.published.Store(true)
							}
						case <-ctx.Done():
						}
					}
				})
				defer worker.close()
				if !worker.offer(c) {
					t.Fatal("initial offer refused")
				}
				synctest.Wait()
				c.final = true
				if calls != 1 || !worker.offer(c) {
					t.Fatal("final release failed to retain its in-flight revision")
				}
				if result == "invalidated" {
					worker.invalidate(c.id)
				}
				close(resume)
				synctest.Wait()
				want := 1
				if result == "failed" {
					want++
				}
				if calls != want {
					t.Fatalf("publication attempts = %d, want %d", calls, want)
				}
			})
		})
	}
}

func TestPreviewWorkerCoalescesBoundsAndInvalidates(t *testing.T) {
	started := make(chan previewCandidate, 5)
	release := make(chan struct{})
	worker := newPreviewWorker(2, func(ctx context.Context, candidate previewCandidate) {
		started <- candidate
		select {
		case <-ctx.Done():
		case <-release:
		}
	})
	t.Cleanup(worker.close)
	read := func(want previewCandidate) {
		t.Helper()
		select {
		case got := <-started:
			if got != want {
				t.Fatalf("projection candidate = %+v, want %+v", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("render did not start")
		}
	}
	first := previewTestCandidate("running", "first content")
	published := previewTestCandidate("published", "unchanged content")
	published.published = new(atomic.Bool)
	published.published.Store(true)
	b := previewTestCandidate("pending", "old")
	c := previewTestCandidate("cancelled", "content")
	newB := previewTestCandidate("pending", "new")
	if !worker.offer(first) {
		t.Fatal("first offer should be accepted")
	}
	read(first)
	if !worker.offer(published) || !worker.offer(b) || !worker.offer(c) || !worker.offer(newB) {
		t.Fatal("pending boards or a newer revision did not fit")
	}
	if worker.offer(previewTestCandidate("overflow", "content")) {
		t.Fatal("accepted work beyond the pending bound")
	}
	worker.invalidate(c.id)
	worker.invalidate(first.id) // cancels a running job without waiting for it
	read(newB)
	close(release)
	worker.close()
	if worker.offer(first) {
		t.Fatal("worker accepted an offer after shutdown")
	}
	select {
	case got := <-started:
		t.Fatalf("unexpected render of removed/duplicate work: %+v", got)
	default:
	}
}

func TestPreviewWorkerNewRevisionCancelsAnOlderRender(t *testing.T) {
	cancelled := make(chan struct{})
	started := make(chan previewCandidate, 2)
	a := previewTestCandidate("same", "1")
	b := previewTestCandidate("same", "2")
	worker := newPreviewWorker(1, func(ctx context.Context, candidate previewCandidate) {
		started <- candidate
		if candidate == a {
			<-ctx.Done()
			close(cancelled)
		}
	})
	t.Cleanup(worker.close)
	worker.offer(a)
	select {
	case got := <-started:
		if got != a {
			t.Fatalf("started %+v, want old revision", got)
		}
	case <-time.After(time.Second):
		t.Fatal("first render did not start")
	}
	worker.offer(a)
	worker.offer(b)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("superseded projection stayed active")
	}
	select {
	case got := <-started:
		if got != b {
			t.Fatalf("started %+v, want newer revision", got)
		}
	case <-time.After(time.Second):
		t.Fatal("new projection did not start")
	}
}

type blockedPreviewCreation struct {
	previewFilesFixture
	started, stopped, release chan struct{}
}

func (f *blockedPreviewCreation) CreatePreview(ctx context.Context, _ uuid.UUID, _, mediaType string, _ []byte) (bool, error) {
	if mediaType != "text/plain" {
		return false, errors.New("wrong preview generator")
	}
	close(f.started)
	<-ctx.Done()
	close(f.stopped)
	<-f.release // keep the worker busy while the test observes later admissions
	return false, ctx.Err()
}

func TestManagerOwnerInvalidationCancelsAnActiveMemoPublication(t *testing.T) {
	deps := newTestDeps()
	files := &blockedPreviewCreation{started: make(chan struct{}), stopped: make(chan struct{}), release: make(chan struct{})}
	deps.PreviewFiles, deps.PreviewCheckpoint = files, deps.store
	m := NewManager(deps.Deps, fastConfig(), nil, zap.NewNop())
	t.Cleanup(m.Close)
	defer close(files.release)
	id := model.DocumentID(uuid.NewString())
	if err := m.PreRegister(t.Context(), model.Metadata{
		ID: id, ContentType: model.ContentTypeMemo, ContentPointer: uuid.NewString(),
		StorageBucketID: uuid.NewString(), Migrated: true,
	}); err != nil {
		t.Fatal(err)
	}
	doc := ycrdt.NewDoc("memo-before-owner-removal")
	defer doc.Destroy()
	doc.GetXMLFragment(memoRoot).Push(ycrdt.ArrayAny{ycrdt.NewYXmlElement("paragraph")})
	state, err := ycrdt.EncodeStateAsUpdateV2(doc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := deps.putState(t.Context(), string(id), state); err != nil {
		t.Fatal(err)
	}
	candidate := previewCandidate{id: id, kind: model.ContentTypeMemo, fingerprint: sha256.Sum256(state)}
	m.offerPreview(candidate)
	select {
	case <-files.started:
	case <-time.After(time.Second):
		t.Fatal("memo publication did not start")
	}
	if err := m.CloseDeleted(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	select {
	case <-files.stopped:
	case <-time.After(time.Second):
		t.Fatal("owner invalidation did not cancel artifact creation")
	}
	// Even if a closing room had acquired the candidate before the event, it
	// cannot race the invalidation to enqueue it again.
	candidate.fingerprint = [sha256.Size]byte{1}
	m.offerPreview(candidate)
	m.previews.mu.Lock()
	defer m.previews.mu.Unlock()
	if len(m.previews.queue) != 0 || m.previews.active == candidate {
		t.Fatal("tombstoned document accepted derivative work")
	}
}

func TestMemoPublicationRefusesMisidentifiedSourceAndMalformedTree(t *testing.T) {
	for _, invalid := range []string{"invalid id", "unexpected type", "wrong stored type", "missing source", "missing bucket", "corrupt checkpoint", "unsupported tree", "untrusted output row"} {
		t.Run(invalid, func(t *testing.T) {
			id, bucket := uuid.New(), uuid.New()
			meta := model.Metadata{
				ID: model.DocumentID(id.String()), ContentType: model.ContentTypeMemo,
				ContentPointer: uuid.NewString(), StorageBucketID: bucket.String(),
			}
			doc := ycrdt.NewDoc("ineligible-memo")
			defer doc.Destroy()
			root := doc.GetXMLFragment(memoRoot)
			if invalid == "unsupported tree" {
				root.Push(ycrdt.ArrayAny{ycrdt.NewYXmlElement("unsupported")})
			}
			state, err := ycrdt.EncodeStateAsUpdateV2(doc, nil)
			if err != nil {
				t.Fatal(err)
			}
			if invalid == "corrupt checkpoint" {
				state = []byte("not a valid v2 update")
			}
			c := previewCandidate{id: meta.ID, kind: model.ContentTypeMemo, fingerprint: sha256.Sum256(state)}
			files := &previewFilesFixture{}
			var metadataErr error
			switch invalid {
			case "invalid id":
				c.id = "not a document identifier"
			case "unexpected type":
				c.kind = model.ContentType("unsupported")
			case "wrong stored type":
				meta.ContentType = model.ContentTypeWhiteboard
			case "missing source":
				metadataErr = model.ErrNotFound
			case "missing bucket":
				meta.StorageBucketID = ""
			case "untrusted output row":
				ref := previewSlotReference(id, previewMarkdown)
				files.slots = map[string]port.PreviewFile{ref: {
					ID: uuid.New(), BucketID: bucket, ExternalReference: ref, MIME: "text/html",
				}}
			}
			preview := documentPreview{
				files: files, logger: zap.NewNop(),
				saved: savedProjection{
					metadata: projectionIndex{load: func(context.Context, model.DocumentID) (model.Metadata, error) {
						return meta, metadataErr
					}},
					checkpoints: projectionCheckpoint{load: func(context.Context, backend.DocumentID) (persistence.Checkpoint, error) {
						return persistence.Checkpoint{Update: state, Encoding: persistence.EncodingV2}, nil
					}},
					maxBytes: PreviewSnapshotLimit,
				},
			}
			// Both mutating methods on files deliberately panic if reached.
			preview.run(t.Context(), c)
		})
	}
}

type publicationFiles struct {
	previewFilesFixture
	written []uuid.UUID
	created []string
	write   func(uuid.UUID, []byte) error
	create  func(uuid.UUID, string, []byte) (bool, error)
}

func (f *publicationFiles) CreatePreview(_ context.Context, bucket uuid.UUID, ref, _ string, png []byte) (bool, error) {
	f.created = append(f.created, ref)
	return f.create(bucket, ref, png)
}

func (f *publicationFiles) ReplacePreview(_ context.Context, id uuid.UUID, _ string, png []byte) error {
	f.written = append(f.written, id)
	return f.write(id, png)
}

//nolint:gocyclo // One trace applies the same ownership and publication assertions to each failure point.
func TestWhiteboardPublicationRechecksTheSavedSourceAndKeepsExistingOutputsOnFailure(t *testing.T) {
	for _, failure := range []string{"", "both missing", "full missing", "clear with full missing", "invalid slot", "both slots invalid", "shared slot", "stale initially", "changed during render", "owner removed during render", "bucket changed during render", "render rejected", "write failed", "write conflict", "write missing", "create failed", "create race", "cancelled"} {
		t.Run("case "+failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			board, bucket, full, card := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			meta := model.Metadata{ID: model.DocumentID(board.String()), ContentType: model.ContentTypeWhiteboard,
				StorageBucketID: bucket.String(), ContentPointer: uuid.NewString(), OwnerRef: "owner"}
			doc := ycrdt.NewDoc("saved")
			defer doc.Destroy()
			if failure != "clear with full missing" {
				doc.GetMap("appState").Set("viewBackgroundColor", "#ff0000")
			}
			state, err := ycrdt.EncodeStateAsUpdateV2(doc, nil)
			if err != nil {
				t.Fatal(err)
			}
			candidate := previewTestCandidate(meta.ID, string(state))
			candidate.published = new(atomic.Bool)
			fullRef, cardRef := previewSlotReference(board, previewFull), previewSlotReference(board, previewCard)
			slots := map[string]port.PreviewFile{
				fullRef: {ID: full, BucketID: bucket, ExternalReference: fullRef, MIME: "image/png"},
				cardRef: {ID: card, BucketID: bucket, ExternalReference: cardRef, MIME: "image/png"},
			}
			var want []uuid.UUID
			var wantCreate []string
			switch failure {
			case "both missing":
				clear(slots)
				wantCreate = []string{fullRef, cardRef}
			case "full missing", "clear with full missing", "create failed", "create race":
				delete(slots, fullRef)
				want = []uuid.UUID{card}
				wantCreate = []string{fullRef}
			case "invalid slot":
				row := slots[fullRef]
				row.AuthorizationID = uuid.New()
				slots[fullRef] = row
				want = []uuid.UUID{card}
			case "both slots invalid":
				for ref, row := range slots {
					row.AuthorizationID = uuid.New()
					slots[ref] = row
				}
			case "shared slot":
				row := slots[cardRef]
				row.ID = full
				slots[cardRef] = row
			case "stale initially":
				candidate.fingerprint = sha256.Sum256(nil)
			case "", "write failed", "write conflict", "write missing":
				want = []uuid.UUID{full, card}
			}
			stored := map[uuid.UUID]string{full: "prior full", card: "prior card"}
			files := &publicationFiles{previewFilesFixture: previewFilesFixture{slots: slots}, write: func(id uuid.UUID, png []byte) error {
				if id == full {
					switch failure {
					case "write failed":
						return errors.New("offline")
					case "write conflict":
						return port.ErrPreviewFileConflict
					case "write missing":
						return port.ErrPreviewFileMissing
					}
				}
				stored[id] = string(png)
				return nil
			}, create: func(to uuid.UUID, ref string, png []byte) (bool, error) {
				expected := map[string]string{fullRef: "new full", cardRef: "new card"}
				if bucket != to || expected[ref] != string(png) {
					t.Fatalf("invalid private creation: %v, %q, %q", to, ref, png)
				}
				switch failure {
				case "create failed":
					return false, errors.New("storage refused creation")
				case "create race":
					return false, nil
				default:
					return true, nil
				}
			}}
			removed := false
			p := documentPreview{
				saved: savedProjection{
					metadata: projectionIndex{load: func(context.Context, model.DocumentID) (model.Metadata, error) {
						if removed {
							return model.Metadata{}, model.ErrNotFound
						}
						return meta, nil
					}},
					checkpoints: projectionCheckpoint{load: func(context.Context, backend.DocumentID) (persistence.Checkpoint, error) {
						return persistence.Checkpoint{Encoding: persistence.EncodingV2, Update: state}, nil
					}}, maxBytes: PreviewSnapshotLimit,
				},
				files: files, logger: zap.NewNop(),
				renderBoard: func(_ context.Context, projected *ycrdt.Doc, _ previewFileSource) (whiteboardrender.Result, error) {
					if failure == "shared slot" || failure == "stale initially" || failure == "both slots invalid" {
						t.Fatal("render started without an eligible current source and slot")
					}
					if got := projected.GetMap("appState").Get("viewBackgroundColor"); (got == nil) != (failure == "clear with full missing") {
						t.Fatalf("wrong saved state passed to renderer: %v", got)
					}
					switch failure {
					case "changed during render":
						state = []byte("newer state with same file ID")
					case "owner removed during render":
						removed = true
					case "bucket changed during render":
						meta.StorageBucketID = uuid.NewString()
					case "render rejected":
						return whiteboardrender.Result{Full: []byte("partial")}, errors.New("failed")
					case "cancelled":
						cancel()
					}
					return whiteboardrender.Result{Full: []byte("new full"), Card: []byte("new card")}, nil
				},
			}
			p.run(ctx, candidate)
			switch failure {
			case "", "both missing", "full missing", "clear with full missing":
				if !candidate.wasPublished() {
					t.Fatal("fully published preview has no publication receipt")
				}
			default:
				if candidate.wasPublished() {
					t.Fatal("partial, stale or failed publication received a success receipt")
				}
			}
			if !slices.Equal(files.written, want) {
				t.Fatalf("wrote %v, want %v", files.written, want)
			}
			if !slices.Equal(files.created, wantCreate) {
				t.Fatalf("created %v, want %v", files.created, wantCreate)
			}
			wantFull, wantCard := "prior full", "prior card"
			if failure == "" {
				wantFull = "new full"
			}
			if slices.Contains(want, card) {
				wantCard = "new card"
			}
			if stored[full] != wantFull || stored[card] != wantCard {
				t.Fatalf("outputs changed incorrectly: full=%q card=%q", stored[full], stored[card])
			}
		})
	}
}
