package service

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

const poisonLocator = "data:image/png;base64,AAAA"

// poisonUpdate returns an update that sets files[key] on a whiteboard, built the
// way a real client would: from a doc synchronized with the room's.
func poisonUpdate(t *testing.T, room *Room, key, value string) []byte {
	t.Helper()
	client := newRoomDoc(string(room.id))
	applyConvention(client, model.ContentTypeWhiteboard)
	state, err := ycrdt.EncodeStateAsUpdateV2(room.doc, nil)
	if err != nil {
		t.Fatalf("encode room state: %v", err)
	}
	if err := ycrdt.ApplyUpdateV2(client, state, nil); err != nil {
		t.Fatalf("seed client: %v", err)
	}
	sv := ycrdt.EncodeStateVector(client)
	client.GetMap(assetsRoot).Set(key, value)
	// v1, which is what y-protocols puts on the wire and what applyUpdate decodes.
	// Encoding a v2 delta here would hand the chokepoint bytes its decoder does not
	// read, and the test would pass for the wrong reason.
	delta, err := ycrdt.EncodeStateAsUpdate(client, sv)
	if err != nil {
		t.Fatalf("encode delta: %v", err)
	}
	return delta
}

// TestTheAssetsRootContract pins the bounded locator schema itself.
func TestTheAssetsRootContract(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		valid bool
	}{
		{"a reference locator", "blob://asset/abc123", true},
		{"a plain id", "abc123", true},
		{"inline data uri", "data:image/png;base64,AAAA", false},
		{"inline data uri, capitalised", "DATA:image/png;base64,AAAA", false},
		{"inline data uri, mixed case", "DaTa:image/png;base64,AAAA", false},
		{"inline data uri behind whitespace", "   data:image/png;base64,AAAA", false},
		{"inline data uri behind a tab/newline", "\t\n data:x", false},
		{"empty", "", false},
		{"whitespace only", "   \t\n ", false},
		{"at the byte limit", strings.Repeat("a", maxLocatorBytes), true},
		{"one byte over", strings.Repeat("a", maxLocatorBytes+1), false},
		// Over the limit only once its padding is counted: the bound is on what was
		// actually sent, not on what survives trimming.
		{"over the limit only counting whitespace", strings.Repeat("a", maxLocatorBytes-10) + strings.Repeat(" ", 20), false},
		{"multibyte over the BYTE limit but under the rune count", strings.Repeat("é", maxLocatorBytes/2+1), false},
		{"not a string", 42, false},
		// The fork trims with JavaScript's String.prototype.trim(), which strips
		// U+FEFF. strings.TrimSpace does not — so a BOM-prefixed data: URI would slip
		// past a TrimSpace-based check, reach storage, and wedge clients on encode.
		{"data uri behind a BOM", "\uFEFFdata:image/png;base64,AAAA", false},
		{"BOM only", "\uFEFF", false},
		{"BOM and spaces only", "\uFEFF \t \uFEFF", false},
		// The other direction: Go's TrimSpace strips U+0085 (NEL) but JS does not, so
		// this must stay a nonempty opaque locator rather than becoming "empty".
		{"U+0085 is not whitespace to JavaScript", "\u0085", true},
		{"line and paragraph separators are trimmed", "\u2028\u2029data:x", false},
		{"non-breaking space is trimmed", "\u00a0data:x", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newRoomDoc("contract")
			applyConvention(d, model.ContentTypeWhiteboard)
			d.GetMap(assetsRoot).Set("k", tc.value)
			err := validateAssetsRoot(d)
			if tc.valid && err != nil {
				t.Fatalf("valid locator rejected: %v", err)
			}
			if !tc.valid && err == nil {
				t.Fatal("invalid locator accepted")
			}
		})
	}
}

// TestPoisonFromAClientNeverReachesTheLiveDocument is the headline RED.
//
// A structurally valid Yjs update can carry an inline data: locator into the
// files root. Clients accept such a document cleanly and then throw on every
// subsequent encode, so discarding and reseeding from the server reloads the same
// poison forever. It has to be refused on the way in.
func TestPoisonFromAClientNeverReachesTheLiveDocument(t *testing.T) {
	room := whiteboardRoom(t)
	before := ycrdt.EncodeStateVector(room.doc)
	beforeFiles := len(room.doc.GetMap(assetsRoot).Keys())
	spy := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: spy, mode: model.ModeCollaborator}
	wasDirty := room.dirty

	got := room.applyUpdate(poisonUpdate(t, room, "bad", poisonLocator), updateOrigin{src: 1})

	if got != applyRejectedSchema {
		t.Fatalf("applyUpdate = %v, want applyRejectedSchema", got)
	}
	if string(ycrdt.EncodeStateVector(room.doc)) != string(before) {
		t.Fatal("the live document changed; poison reached the authoritative state")
	}
	if n := len(room.doc.GetMap(assetsRoot).Keys()); n != beforeFiles {
		t.Fatalf("files root has %d keys, want %d — poison landed", n, beforeFiles)
	}
	if spy.frames != 0 {
		t.Fatalf("%d frame(s) broadcast for a rejected update", spy.frames)
	}
	if room.dirty != wasDirty {
		t.Fatal("a rejected update marked the room dirty; it would be persisted")
	}
}

// TestPoisonFromAPeerPodIsAlsoRefused: an over-budget peer update is applied to
// avoid cross-pod divergence, but poison is not. Converging every pod on an
// unusable document is not convergence worth having.
func TestPoisonFromAPeerPodIsAlsoRefused(t *testing.T) {
	room := whiteboardRoom(t)
	before := ycrdt.EncodeStateVector(room.doc)

	got := room.applyUpdate(poisonUpdate(t, room, "bad", poisonLocator), updateOrigin{src: 0, peer: true})

	if got != applyRejectedSchema {
		t.Fatalf("peer applyUpdate = %v, want applyRejectedSchema", got)
	}
	if string(ycrdt.EncodeStateVector(room.doc)) != string(before) {
		t.Fatal("peer poison reached the live document")
	}
}

// TestTheRoomStaysUsableAfterARejection asserts a rejection costs one update, not
// the room: the shadow is rebuilt from the live doc, so the next update validates
// against the right state.
//
// The follow-up update comes from a DIFFERENT writer, which is all this proves. The
// rejected writer itself cannot simply continue — its refused struct leaves a gap
// in its own clock sequence and anything it sends next stays pending behind it, so
// its session is ended with CodeContentRefused.
func TestTheRoomStaysUsableAfterARejection(t *testing.T) {
	room := whiteboardRoom(t)
	if got := room.applyUpdate(poisonUpdate(t, room, "bad", poisonLocator), updateOrigin{src: 1}); got != applyRejectedSchema {
		t.Fatalf("setup: want applyRejectedSchema, got %v", got)
	}

	good := poisonUpdate(t, room, "ok", "blob://asset/legit")
	if got := room.applyUpdate(good, updateOrigin{src: 2}); got != applyOK {
		t.Fatalf("a valid update after a rejection = %v, want applyOK", got)
	}
	if v := room.doc.GetMap(assetsRoot).Get("ok"); v != "blob://asset/legit" {
		t.Fatalf("the valid update did not land: files[ok] = %v", v)
	}
}

// TestAPartiallyAppliedCandidateDoesNotPoisonLaterValidation is the len-1 case,
// measured: an update truncated at its final byte applies IN FULL to the shadow
// and then errors. Reusing that shadow would validate the next update against a
// state the live document never had.
func TestAPartiallyAppliedCandidateDoesNotPoisonLaterValidation(t *testing.T) {
	room := whiteboardRoom(t)
	full := poisonUpdate(t, room, "sneak", poisonLocator)

	if got := room.applyUpdate(full[:len(full)-1], updateOrigin{src: 1}); got != applyCandidateFailed {
		t.Fatalf("a truncated update = %v, want applyCandidateFailed", got)
	}
	if k := room.doc.GetMap(assetsRoot).Keys(); len(k) != 0 {
		t.Fatalf("the truncated update mutated the live document: %v", k)
	}

	// The discriminator is a VALID update, not the poison again. The contaminated
	// shadow holds the poison; re-sending the poison would be rejected either way,
	// so it proves nothing. A LEGITIMATE update validated against that shadow would
	// be refused for someone else's poison — the room would be wedged for everyone.
	good := poisonUpdate(t, room, "ok", "blob://asset/legit")
	if got := room.applyUpdate(good, updateOrigin{src: 2}); got != applyOK {
		t.Fatalf("a valid update after a partial candidate apply = %v, want applyOK — the shadow was not rebuilt and is still holding the poison", got)
	}
	if v := room.doc.GetMap(assetsRoot).Get("ok"); v != "blob://asset/legit" {
		t.Fatalf("the valid update did not land: files[ok] = %v", v)
	}
}

// TestAMemoIsNeverGivenAFilesRoot is load-bearing scope, not an optimization.
// Accessing a named root MATERIALIZES it in Yjs, so a memo must never be validated
// in a way that adds a files map to a document whose convention is a single
// XmlFragment.
//
// This assertion got STRONGER when memos gained a validation shadow. Previously a
// memo was never validated at all, so it trivially grew no files root. Now it IS
// validated on every update, and the check must reach its images through
// XmlFragment("default") — the root applyConvention already materializes — without
// ever touching GetMap("files").
//
// Asserted on a room built exactly as newRoom builds one, but not running: reading
// a live room's document from the test goroutine races its run loop, which is the
// single writer.
func TestAMemoIsNeverGivenAFilesRoot(t *testing.T) {
	for _, content := range []model.ContentType{model.ContentTypeMemo, model.ContentTypeWhiteboard} {
		t.Run(string(content), func(t *testing.T) {
			r := newBareRoom(t)
			r.content = content
			applyConvention(r.doc, content)
			before, err := ycrdt.EncodeStateAsUpdateV2(r.doc, nil)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			if err := r.initShadow(r.doc); err != nil {
				t.Fatalf("initShadow: %v", err)
			}
			// BOTH conventions carry a shadow now: both hold blob locators that must
			// be references, a whiteboard in `files` and a memo in an image `src`.
			if r.shadow == nil {
				t.Fatalf("%s has no validation shadow; its updates would go unchecked", content)
			}
			// Run the real validation against the live doc, so the no-files-root claim
			// is tested against what actually executes rather than against setup alone.
			if err := r.validateSchema(r.doc); err != nil {
				t.Fatalf("validating a clean %s: %v", content, err)
			}

			for _, k := range r.doc.ToJSON().Keys() {
				if k == assetsRoot && content == model.ContentTypeMemo {
					t.Fatal("a memo document grew a files root — inspecting a root materializes it")
				}
			}
			after, err := ycrdt.EncodeStateAsUpdateV2(r.doc, nil)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if string(before) != string(after) {
				t.Fatalf("%s: the document's encoded state changed merely by preparing and running validation", content)
			}
		})
	}
}

// whiteboardRoom builds a bare whiteboard room with its validation shadow, the
// same way newRoom does: convention applied, shadow cloned from the live doc.
func whiteboardRoom(t *testing.T) *Room {
	t.Helper()
	r := newBareRoom(t)
	r.content = model.ContentTypeWhiteboard
	applyConvention(r.doc, model.ContentTypeWhiteboard)
	shadow, err := cloneDoc(r.doc, string(r.id))
	if err != nil {
		t.Fatalf("build shadow: %v", err)
	}
	r.shadow = shadow
	return r
}

// TestAPoisonedCheckpointFailsMaterialization is the cold-load RED: a document
// poisoned before validation existed must not come back as a live room.
func TestAPoisonedCheckpointFailsMaterialization(t *testing.T) {
	deps := newTestDeps()
	ctx := context.Background()
	const doc model.DocumentID = "poisoned-checkpoint"

	// A stored whiteboard carrying an inline locator.
	seed := newRoomDoc(string(doc))
	applyConvention(seed, model.ContentTypeWhiteboard)
	seed.GetMap(assetsRoot).Set("bad", poisonLocator)
	snapshot, err := ycrdt.EncodeStateAsUpdateV2(seed, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := deps.store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: backend.DocumentID(doc), Encoding: persistence.EncodingV2,
		Update: snapshot, StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	if err := deps.meta.Save(ctx, model.Metadata{
		ID: doc, ContentType: model.ContentTypeWhiteboard, ContentPointer: string(doc),
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(deps.Deps, fastConfig(), nil, zap.NewNop())
	t.Cleanup(mgr.Close)

	client := newFakeClient(t)
	_, _, joinErr := mgr.Join(ctx, JoinRequest{ID: doc, Content: model.ContentTypeWhiteboard, Conn: client})
	if joinErr == nil {
		t.Fatal("a poisoned checkpoint materialized a live room; clients would reload the poison forever")
	}
	if mgr.RoomCount() != 0 {
		t.Fatalf("%d live room(s) after a refused materialization", mgr.RoomCount())
	}
	if residentInRegistry(t, mgr.registry, doc) {
		t.Fatal("a refused materialization left the document resident in the registry")
	}
}

// TestARefusedUpdateIsNotReportedAsApplied covers the bug this slice corrected:
// applyUpdate logged a decode failure and returned success anyway, so dispatchSync
// reported `applied` for an update that never landed — recording contributor
// activity and arming the save timer for nothing.
//
// Driven through dispatchSync, because that is where the wrong answer was consumed.
func TestARefusedUpdateIsNotReportedAsApplied(t *testing.T) {
	room := whiteboardRoom(t)
	poison := poisonUpdate(t, room, "bad", poisonLocator)

	for _, tc := range []struct {
		name             string
		body             []byte
		wantSchemaReject bool
	}{
		{"schema rejection", poison, true},
		{"undecodable bytes", poison[:len(poison)-1], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			framed := protocol.EncodeUpdate(tc.body)
			var reply bytes.Buffer
			outcome, err := room.dispatchSync(framed, &reply, 1, true)
			if err != nil {
				t.Fatalf("dispatchSync: %v", err)
			}
			if outcome.applied {
				t.Fatal("a refused update was reported as applied; it records contributor activity and arms the save timer for an edit that never landed")
			}
			if outcome.rejectedSchema != tc.wantSchemaReject {
				t.Fatalf("rejectedSchema = %v, want %v", outcome.rejectedSchema, tc.wantSchemaReject)
			}
			if outcome.rejectedTooLarge {
				t.Fatal("a schema/decode refusal was reported as a size rejection; the client would be disconnected")
			}
		})
	}
}

// TestPoisonOnTheRealRoomPath drives a LIVE room through the Manager, because the
// unit pins above use a bare room with no update observer and no run loop — where
// "nothing was broadcast" and "the room stayed usable" are true by construction
// rather than by the code under test.
//
// Here the observer is attached, the run loop is running, and a second member is
// watching. That is the configuration in which a broadcast would actually happen.
func TestPoisonOnTheRealRoomPath(t *testing.T) {
	mgr, deps := testManager(t, fastConfig())
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "wb-real-path"

	author := newFakeClient(t)
	author.join(mgr, doc, model.ContentTypeWhiteboard)
	author.observeUpdates()

	watcher := newFakeClient(t)
	watcher.join(mgr, doc, model.ContentTypeWhiteboard)
	watcher.observeUpdates()

	mgr.mu.Lock()
	room := mgr.rooms[doc]
	mgr.mu.Unlock()
	if room == nil {
		t.Fatal("no live room")
	}

	// The author emits the poison from its OWN document, which observeUpdates
	// forwards to the server — exactly how a browser produces one. Building it by
	// reading room.doc would race the run loop, the document's single writer.
	author.withDoc(func(d *ycrdt.Doc) {
		d.GetMap(assetsRoot).Set("bad", poisonLocator)
	})

	// Drain the run loop deterministically instead of sleeping. The command channel
	// is FIFO, so: the poison is already queued; queue a no-op command behind it;
	// then join a third member, whose cmdJoin is queued behind THAT and whose reply
	// only comes back once the run loop has processed both. When Join returns, the
	// poison has been handled and a persist has actually run — so if the rejection
	// had marked the room dirty, a snapshot would exist by now.
	room.enqueue(command{kind: cmdLeave})
	barrier := newFakeClient(t)
	barrier.join(mgr, doc, model.ContentTypeWhiteboard)

	// The watcher must never end up holding it. Asserted on CONTENT rather than on
	// a frame count: the initial sync from a member's own join is also a document
	// frame and can land after any baseline, so counting frames answers a different
	// question than "did the poison reach anyone".
	if watcher.hasElement("bad") {
		t.Fatal("the watcher's document received the rejected update")
	}
	var watcherFiles []string
	watcher.withDoc(func(d *ycrdt.Doc) {
		if f := d.GetMap(assetsRoot); f != nil {
			watcherFiles = f.Keys()
		}
	})
	if len(watcherFiles) != 0 {
		t.Fatalf("the watcher's files root holds %v; a rejected update was broadcast", watcherFiles)
	}

	// The offender is ended, and ONLY the offender: replaying the same malformed
	// local state on an automatic reconnect could never converge.
	if !hasControlCode(author, model.CodeContentRefused) {
		t.Fatal("the sender was not ended with content-refused")
	}
	// Rolling compatibility is deliberately ordered: an old consumer sees the
	// legacy refusal before the typed end closes the socket, while a new consumer
	// treats the typed end as authoritative. Remove this assertion together with
	// ControlUpdateRejected after both consumers have deployed.
	legacyIndex, typedIndex := -1, -1
	for i, control := range author.controlMessages() {
		if control.Kind == model.ControlUpdateRejected {
			legacyIndex = i
		}
		if control.Kind == model.ControlSessionEnd && control.Code == model.CodeContentRefused {
			typedIndex = i
		}
	}
	if legacyIndex < 0 || typedIndex < 0 || legacyIndex >= typedIndex {
		t.Fatalf("rolling refusal order legacy=%d typed=%d, want legacy before typed", legacyIndex, typedIndex)
	}
	if hasControlCode(watcher, model.CodeContentRefused) {
		t.Fatal("a bystander was ended for another client's rejected update")
	}
	if _, err := deps.storedState(context.Background(), string(doc)); err == nil {
		t.Fatal("a rejected update produced a stored snapshot; it was treated as dirty and persisted")
	}

	// And the room is still usable for OTHER writers.
	watcher.addElement("el-good", map[string]interface{}{"x": 42.0})
	waitFor(t, "the valid update to reach another member", func() bool {
		return barrier.hasElement("el-good")
	})
}

// TestPeerPoisonGoesThroughTheRealPeerPath drives the cross-pod origin through
// handlePeer rather than calling applyUpdate directly, so a change that bypassed
// the chokepoint on that path would go RED here.
func TestPeerPoisonGoesThroughTheRealPeerPath(t *testing.T) {
	room := whiteboardRoom(t)
	before := ycrdt.EncodeStateVector(room.doc)

	mutated := room.handlePeer(poisonUpdate(t, room, "bad", poisonLocator), false)

	if mutated {
		t.Fatal("handlePeer reported a mutation for a rejected peer update; the save timer would be armed")
	}
	if string(ycrdt.EncodeStateVector(room.doc)) != string(before) {
		t.Fatal("peer poison reached the live document through handlePeer")
	}
}

// TestAMemoMalformedUpdateKeepsItsPreExistingBehaviour is a scope regression.
//
// This slice adds an assets-root contract for WHITEBOARDS. A memo has no such
// contract and no shadow, so nothing about it may change — in particular a
// malformed memo update must not become a schema policy decision, must not tear the
// room down, and must keep whatever applied/dirty semantics it had. Without a
// candidate nothing proves the live document was untouched, so "correcting" the
// report here would decide a question this change has no business deciding.
func TestAMemoMalformedUpdateKeepsItsPreExistingBehaviour(t *testing.T) {
	room := newBareRoom(t) // content: memo
	if room.shadow != nil {
		t.Fatal("a memo room has a validation shadow")
	}

	// Bytes the live apply will refuse.
	got := room.applyUpdate([]byte{0xff, 0xff, 0xff, 0xff}, updateOrigin{src: 1})

	// The exact preserved contract, not merely "not a schema rejection": before this
	// change a no-shadow live apply error logged and reported applyOK, so the caller
	// recorded activity and armed a save on whatever the observer had set. Anything
	// else here — including applyCandidateFailed — would be a behaviour change this
	// slice has no mandate to make.
	if got != applyOK {
		t.Fatalf("a malformed memo update returned %v, want applyOK — its pre-existing behaviour must be preserved exactly", got)
	}
}

// TestAnOverBudgetPeerUpdateIsStillSchemaChecked pins the interaction between the
// two guards, which pull in opposite directions for a peer.
//
// An over-budget PEER update is applied anyway: refusing it would diverge this pod
// from the one that already accepted it, and MaxDocBytes skew is an operational
// problem, not a correctness one. Schema is the opposite — poison is poison
// whichever pod it came from, and accepting it "for convergence" would converge
// every pod on a document no client can encode. So the size branch must fall
// through to the candidate rather than around it.
func TestAnOverBudgetPeerUpdateIsStillSchemaChecked(t *testing.T) {
	room := whiteboardRoom(t)
	room.cfg.Limits.MaxDocBytes = 1 // every update is over budget
	before := ycrdt.EncodeStateVector(room.doc)

	poison := poisonUpdate(t, room, "bad", poisonLocator)
	if got := room.applyUpdate(poison, updateOrigin{src: 0, peer: true}); got != applyRejectedSchema {
		t.Fatalf("over-budget peer poison = %v, want applyRejectedSchema", got)
	}
	if string(ycrdt.EncodeStateVector(room.doc)) != string(before) {
		t.Fatal("over-budget peer poison reached the live document")
	}

	// And a well-formed over-budget peer update IS still applied, unchanged by this
	// slice: convergence beats the local byte budget for peers.
	good := poisonUpdate(t, room, "ok", "blob://asset/legit")
	if got := room.applyUpdate(good, updateOrigin{src: 0, peer: true}); got != applyOK {
		t.Fatalf("over-budget peer update = %v, want applyOK (peers are not rejected for size)", got)
	}
}

// TestALocalOverBudgetUpdateIsRejectedBeforeTheCandidate asserts the size check
// still short-circuits for a LOCAL write, so an oversized update does not pay for a
// candidate apply it can never pass.
func TestALocalOverBudgetUpdateIsRejectedBeforeTheCandidate(t *testing.T) {
	room := whiteboardRoom(t)
	room.cfg.Limits.MaxDocBytes = 1

	good := poisonUpdate(t, room, "ok", "blob://asset/legit")
	if got := room.applyUpdate(good, updateOrigin{src: 1}); got != applyRejectedTooLarge {
		t.Fatalf("over-budget local update = %v, want applyRejectedTooLarge", got)
	}
}

// TestWhiteboardMessagesSurviveTheSharedRule pins every whiteboard rejection
// string verbatim.
//
// The locator VALUE rule was factored out of validateAssetsRoot so the memo image
// check enforces the same rule rather than a second copy of it. Factoring is
// exactly the kind of change that silently reshapes an error message, and these
// strings are the operator's only view of why an update was refused. Asserted on
// the full message, not on a substring, so a reworded or re-ordered subject fails.
func TestWhiteboardMessagesSurviveTheSharedRule(t *testing.T) {
	for _, tc := range []struct {
		val  interface{}
		want string
	}{
		{42, `files["k"] is int, want a string locator`},
		{"", `files["k"] is empty`},
		{"data:image/png;base64,x", `files["k"] is an inline data: locator, not a reference`},
		{strings.Repeat("a", maxLocatorBytes+1), `files["k"] is 2049 bytes, over the 2048-byte locator limit`},
	} {
		d := ycrdt.NewDoc("wb")
		d.GetMap(assetsRoot).Set("k", tc.val)
		err := validateAssetsRoot(d)
		if err == nil {
			t.Errorf("%#v was accepted", tc.val)
			continue
		}
		if err.Error() != tc.want {
			t.Errorf("message = %q, want %q", err.Error(), tc.want)
		}
	}
}
