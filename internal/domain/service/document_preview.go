package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
	"github.com/alkem-io/collaboration-service/internal/domain/service/whiteboardrender"
)

// A candidate holds only a saved snapshot identity and its publication receipt.
type previewCandidate struct {
	id          model.DocumentID
	kind        model.ContentType
	fingerprint [sha256.Size]byte
	published   *atomic.Bool
	final       bool
}

func (c previewCandidate) wasPublished() bool {
	return c.published != nil && c.published.Load()
}

const previewSaveWindow = 5 * time.Second

func (r *Room) notePreviewSave(snapshot []byte) {
	if r.deps.offerPreview == nil {
		return
	}
	fingerprint := sha256.Sum256(snapshot)
	if r.savedPreview == fingerprint {
		return
	}
	r.savedPreview = fingerprint
	if r.previewTimer == nil {
		r.previewTimer = time.NewTimer(previewSaveWindow)
	}
}

func (r *Room) offerSavedPreview(final bool) {
	if r.deps.offerPreview == nil || r.savedPreview == r.initialPreview {
		return
	}
	if r.previewOffer.published == nil || r.previewOffer.fingerprint != r.savedPreview {
		r.previewOffer = previewCandidate{
			id: r.id, kind: r.content, fingerprint: r.savedPreview, published: new(atomic.Bool),
		}
	}
	if !r.previewOffer.wasPublished() {
		candidate := r.previewOffer
		candidate.final = final
		r.deps.offerPreview(candidate)
	}
}

func (c previewCandidate) documentUUID() (uuid.UUID, bool) {
	id, err := uuid.Parse(string(c.id))
	return id, err == nil && id != uuid.Nil && (c.kind == model.ContentTypeMemo || c.kind == model.ContentTypeWhiteboard)
}

// previewWorker owns one render goroutine and a short FIFO of compact saved
// candidates. The room only takes the queue lock; it never waits on rendering,
// checkpoint reads or publication.
type previewWorker struct {
	ctx    context.Context
	stop   context.CancelFunc
	wake   chan struct{}
	done   chan struct{}
	mu     sync.Mutex
	queue  []previewCandidate
	active previewCandidate
	cancel context.CancelFunc
}

func newPreviewWorker(capacity int, render func(context.Context, previewCandidate)) *previewWorker {
	ctx, cancel := context.WithCancel(context.Background())
	w := &previewWorker{
		ctx: ctx, stop: cancel, wake: make(chan struct{}, 1), done: make(chan struct{}),
		queue: make([]previewCandidate, 0, capacity),
	}
	go w.run(render)
	return w
}

func (w *previewWorker) close() {
	w.stop()
	<-w.done
}

func (w *previewWorker) offer(c previewCandidate) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ctx.Err() != nil {
		return false
	}
	if c.wasPublished() {
		return true
	}
	final := c.final
	c.final = false // scheduling intent is not part of the work identity
	slot := slices.IndexFunc(w.queue, func(pending previewCandidate) bool { return pending.id == c.id })
	sameActive := w.cancel != nil && w.active == c
	if sameActive && !final {
		return true
	}
	if slot < 0 && len(w.queue) == cap(w.queue) {
		return false
	}
	if w.cancel != nil && w.active.id == c.id && !sameActive {
		w.cancel()
	}
	if slot >= 0 {
		w.queue[slot] = c
		return true
	}
	w.queue = append(w.queue, c)
	select {
	case w.wake <- struct{}{}:
	default: // the one wake-up already covers all pending candidates
	}
	return true
}

func (w *previewWorker) invalidate(id model.DocumentID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.queue = slices.DeleteFunc(w.queue, func(c previewCandidate) bool { return c.id == id })
	if w.cancel != nil && w.active.id == id {
		w.cancel()
	}
}

func (w *previewWorker) next() (previewCandidate, context.Context, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ctx.Err() != nil || len(w.queue) == 0 {
		return previewCandidate{}, nil, false
	}
	w.active = w.queue[0]
	w.queue = slices.Delete(w.queue, 0, 1)
	ctx, cancel := context.WithCancel(w.ctx)
	w.cancel = cancel
	return w.active, ctx, true
}

func (w *previewWorker) run(render func(context.Context, previewCandidate)) {
	defer close(w.done)
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.wake:
			for {
				candidate, ctx, ok := w.next()
				if !ok {
					break
				}
				// A final release may queue the active revision once. Skip it
				// if the in-flight attempt published the artifact.
				if !candidate.wasPublished() {
					render(ctx, candidate)
				}
				w.mu.Lock()
				w.cancel()
				w.cancel = nil
				w.active = previewCandidate{}
				w.mu.Unlock()
			}
		}
	}
}

// PreviewSnapshotLimit bounds saved derivative reads without changing editing or persistence capacity.
const PreviewSnapshotLimit = 1 << 20

func newDocumentPreviewWorker(deps Deps, logger *zap.Logger) *previewWorker {
	worker := documentPreview{
		saved: savedProjection{metadata: deps.Metadata, checkpoints: deps.PreviewCheckpoint, maxBytes: PreviewSnapshotLimit},
		files: deps.PreviewFiles, logger: logger,
		renderBoard: renderBoardPreview,
	}
	return newPreviewWorker(4, worker.run)
}

type documentPreview struct {
	saved       savedProjection
	files       port.PreviewFiles
	renderBoard func(context.Context, *ycrdt.Doc, previewFileSource) ([]byte, error)
	logger      *zap.Logger
}

func renderBoardPreview(ctx context.Context, doc *ycrdt.Doc, src previewFileSource) ([]byte, error) {
	return whiteboardrender.Render(ctx, doc, func(ctx context.Context, locator string, limit int64) (string, []byte, error) {
		asset, err := src.asset(ctx, locator, limit)
		return asset.mime, asset.bytes, err
	})
}

// An invisible Markdown reference definition keeps blanks nonzero and
// keeps an iframe-first body classified as text/plain by file-service.
// The direct memo endpoint returns unwrapped Markdown.
const memoPreviewHeader = "[//alkemio-preview-v1//]: # (Generated memo preview)\n\n"

func (p documentPreview) render(ctx context.Context, doc *ycrdt.Doc, src previewFileSource, kind model.ContentType) ([]byte, error) {
	if kind == model.ContentTypeMemo {
		md, err := renderSavedMemo(ctx, doc)
		if err != nil {
			return nil, err
		}
		return []byte(memoPreviewHeader + md), nil
	}
	return p.renderBoard(ctx, doc, src)
}

func (p documentPreview) run(ctx context.Context, candidate previewCandidate) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	if p.publish(ctx, candidate) && candidate.published != nil {
		candidate.published.Store(true)
	}
}

func (p documentPreview) log(stage, outcome string, variant previewVariant) {
	p.logger.Info("document preview", zap.String("stage", stage), zap.String("outcome", outcome),
		zap.String("variant", string(variant)))
}

func (p documentPreview) publish(ctx context.Context, c previewCandidate) bool {
	id, ok := c.documentUUID()
	if !ok {
		p.log("source", "invalid", "")
		return false
	}
	meta, bytes, err := p.saved.load(ctx, c.id, c.kind)
	if err != nil {
		p.log("source", "unavailable", "")
		return false
	}
	if sha256.Sum256(bytes) != c.fingerprint {
		p.log("source", "stale", "")
		return false
	}
	bucket, bucketErr := uuid.Parse(meta.StorageBucketID)
	checkpoint, pointerErr := uuid.Parse(meta.ContentPointer)
	if bucketErr != nil || bucket == uuid.Nil || pointerErr != nil || checkpoint == uuid.Nil {
		p.log("source", "invalid", "")
		return false
	}
	src := previewFileSource{files: p.files, bucket: bucket, checkpoint: checkpoint}
	variant := previewFull
	if c.kind == model.ContentTypeMemo {
		variant = previewMarkdown
	}
	target := p.outputSlot(ctx, src, id, variant)
	if !target.valid() {
		return false
	}
	doc, err := decodeProjection(ctx, c.id, bytes)
	if err != nil {
		p.log("source", "decode-failed", "")
		return false
	}
	rendered, err := p.render(ctx, doc, src, c.kind)
	doc.Destroy()
	if err != nil || ctx.Err() != nil {
		p.log("render", "failed", "")
		return false
	}
	return p.publishCurrent(ctx, c, meta, bucket, target, rendered)
}

type previewTarget struct {
	id             uuid.UUID
	variant        previewVariant
	firstReference string
}

func (out previewTarget) valid() bool {
	return out.id != uuid.Nil || out.firstReference != ""
}

func (p documentPreview) outputSlot(ctx context.Context, src previewFileSource, id uuid.UUID, variant previewVariant) previewTarget {
	file, err := src.slot(ctx, id, variant)
	switch {
	case errors.Is(err, port.ErrPreviewFileMissing):
		return previewTarget{variant: variant, firstReference: previewSlotReference(id, variant)}
	case err != nil:
		p.log("slot", "failed", variant)
	default:
		return previewTarget{id: file, variant: variant}
	}
	return previewTarget{}
}

// Recheck saved content and ownership immediately before publishing the artifact.
func (p documentPreview) publishCurrent(ctx context.Context, c previewCandidate, meta model.Metadata, bucket uuid.UUID, out previewTarget, content []byte) bool {
	canonical, state, err := p.saved.load(ctx, c.id, c.kind)
	if err != nil || canonical.ContentPointer != meta.ContentPointer ||
		canonical.StorageBucketID != meta.StorageBucketID || canonical.OwnerRef != meta.OwnerRef ||
		canonical.AuthorizationPolicyID != meta.AuthorizationPolicyID || sha256.Sum256(state) != c.fingerprint {
		p.log("publish", "stale", out.variant)
		return false
	}
	if out.firstReference != "" {
		created, err := p.files.CreatePreview(ctx, bucket, out.firstReference, out.variant.mediaType(), content)
		switch {
		case err != nil:
			p.log("create", "failed", out.variant)
		case created:
			p.log("create", "created", out.variant)
		default:
			// A different request won the reference. Its bytes were not
			// replaced, so do not claim that the candidate was published.
			p.log("create", "already-exists", out.variant)
		}
		return err == nil && created
	}
	err = p.files.ReplacePreview(ctx, out.id, out.variant.mediaType(), content)
	switch {
	case err == nil:
		p.log("publish", "replaced", out.variant)
	case errors.Is(err, port.ErrPreviewFileConflict):
		p.log("publish", "conflict", out.variant)
	default:
		p.log("publish", "failed", out.variant)
	}
	return err == nil
}
