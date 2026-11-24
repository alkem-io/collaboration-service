package service

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service/whiteboardrender"
)

//nolint:gocyclo // One trace keeps queue pressure, concurrent readers and the durability assertions synchronized.
func TestBlockedPreviewAndFullQueueDoNotDelayAnotherBoardDurability(t *testing.T) {
	cfg := fastConfig()
	cfg.IdleTimeout = 0
	manager, _ := testManager(t, cfg)
	t.Cleanup(manager.Close)
	started := make(chan struct{})
	draw := make(chan struct{})
	rendered := make(chan error, 1)
	measureRenderer := os.Getenv("COLLAB_MEASURE_PREVIEWS") == "1"
	worker := newPreviewWorker(4, func(ctx context.Context, c previewCandidate) {
		if c.id == "held-preview" {
			close(started)
			if measureRenderer {
				select {
				case <-draw:
					rendered <- renderLoadFixture(ctx)
				case <-ctx.Done():
					return
				}
			}
			<-ctx.Done() // emulate a hung write cancelling at the outbound context deadline
		}
	})
	t.Cleanup(worker.close)
	offers := make(chan bool, 1)
	manager.deps.offerPreview = func(c previewCandidate) { offers <- worker.offer(c) }
	writer := newFakeClient(t)
	writer.join(manager, "independent-board", model.ContentTypeWhiteboard)
	writer.observeUpdates()
	save := func(id string) time.Duration {
		t.Helper()
		begin := time.Now()
		writer.addElement(id, map[string]interface{}{"type": "rectangle"})
		requestDurability(writer, id)
		waitFor(t, id, func() bool { return barrierOutcome(writer, id) != nil })
		if got := barrierOutcome(writer, id); got.Kind != model.ControlPersisted {
			t.Fatalf("durability ack while preview blocked: %+v", got)
		}
		return time.Since(begin)
	}
	baseline := save("baseline")

	closing := newFakeClient(t)
	closing.join(manager, "held-preview", model.ContentTypeWhiteboard)
	closing.observeUpdates()
	closing.addElement("previewed", map[string]interface{}{"type": "rectangle"})
	closing.session.Leave()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("did not receive close-triggered preview")
	}
	waitFor(t, "closing room released while its preview is blocked", func() bool { return manager.RoomCount() == 1 })
	if !<-offers {
		t.Fatal("close candidate was refused")
	}
	for i := 0; i < 4; i++ {
		if !worker.offer(previewTestCandidate(model.DocumentID(fmt.Sprintf("queued-%d", i)), "fixture")) {
			t.Fatal("pending buffer refused its advertised capacity")
		}
	}
	if worker.offer(previewTestCandidate("overflow", "fixture")) {
		t.Fatal("accepted work over the bound")
	}

	memoDoc := ycrdt.NewDoc("memo-while-preview-blocked")
	t.Cleanup(memoDoc.Destroy)
	memoDoc.GetXMLFragment("default").Push(ycrdt.ArrayAny{ycrdt.NewYXmlElement("paragraph")})
	savedMemo, err := ycrdt.EncodeStateAsUpdateV2(memoDoc, nil)
	if err != nil {
		t.Fatal(err)
	}
	readMemo := NewMemoMarkdownReader(
		projectionIndex{load: func(_ context.Context, id model.DocumentID) (model.Metadata, error) {
			return model.Metadata{ID: id, ContentType: model.ContentTypeMemo}, nil
		}},
		projectionCheckpoint{load: func(context.Context, backend.DocumentID) (persistence.Checkpoint, error) {
			return persistence.Checkpoint{Update: savedMemo, Encoding: persistence.EncodingV2}, nil
		}},
	)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for range 10 {
				if body, err := readMemo(t.Context(), "cold-memo"); err != nil || body != "" {
					t.Errorf("independent memo read failed: %q, %v", body, err)
					return
				}
			}
		})
	}
	close(draw)
	loaded := save("during-preview")
	wg.Wait()
	if measureRenderer {
		select {
		case err := <-rendered:
			if err != nil {
				t.Fatalf("representative dense preview failed: %v", err)
			}
		case <-time.After(4 * time.Second):
			t.Fatal("representative render did not settle")
		}
	}
	t.Logf("source barrier: %s baseline, %s with held preview + four queued + concurrent cold memo reads", baseline, loaded)
}

func renderLoadFixture(ctx context.Context) error {
	doc := ycrdt.NewDoc("dense-preview-load")
	defer doc.Destroy()
	for i := range 2300 {
		addElement(doc, fmt.Sprintf("node%d", i), map[string]any{
			"id": fmt.Sprintf("node%d", i), "index": fmt.Sprintf("%04d", i), "type": "rectangle",
			"x": float64(i%80) * 21, "y": float64(i/80) * 23, "width": 20., "height": 20.,
			"opacity": 100., "strokeWidth": 1., "strokeStyle": "solid", "fillStyle": "solid",
			"strokeColor": "#303030", "backgroundColor": "#21ac5f", "seed": i + 1,
		})
	}
	result, err := whiteboardrender.Render(ctx, doc, nil)
	if err == nil && (len(result.Full) == 0 || len(result.Card) == 0) {
		return fmt.Errorf("renderer did not return both PNGs")
	}
	return err
}
