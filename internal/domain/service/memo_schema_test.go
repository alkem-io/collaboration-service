package service

import (
	"context"
	"strings"
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// memoRoom builds a bare memo room with its validation shadow, the same way
// newRoom does: convention applied, shadow cloned from the live doc.
func memoRoom(t *testing.T) *Room {
	t.Helper()
	r := newBareRoom(t)
	r.content = model.ContentTypeMemo
	applyConvention(r.doc, model.ContentTypeMemo)
	shadow, err := cloneDoc(r.doc, string(r.id))
	if err != nil {
		t.Fatalf("build shadow: %v", err)
	}
	r.shadow = shadow
	return r
}

// setMemoImage writes a paragraph > image[src] into the memo's fragment, which is
// the shape y-prosemirror produces for a TipTap image node.
//
// The paragraph is inserted FIRST and the image is added to the integrated element
// retrieved back out of the fragment. Nesting one detached element inside another
// before integration silently produces a tree QuerySelectorAll cannot see — the
// first version of this helper did exactly that. The rejection rows in the table
// below catch it on their own (a validator that sees no images refuses nothing),
// which is why there is no separate fixture assertion here: one was written,
// probed, and removed after breaking the fixture proved the table already REDs
// with and without it.
func setMemoImage(t *testing.T, d *ycrdt.Doc, src interface{}) {
	t.Helper()
	frag := d.GetXMLFragment(memoRoot)
	frag.Insert(0, ycrdt.ArrayAny{ycrdt.NewYXmlElement("paragraph")})
	para, ok := frag.Get(0).(*ycrdt.YXmlElement)
	if !ok {
		t.Fatalf("fragment[0] is %T, want a paragraph element", frag.Get(0))
	}
	img := ycrdt.NewYXmlElement("image")
	if src != nil {
		img.SetAttribute("src", src)
	}
	para.Insert(0, ycrdt.ArrayAny{img})
}

// updateFrom encodes a document's whole state as a v1 update, the shape a client
// sends.
func updateFrom(t *testing.T, d *ycrdt.Doc) []byte {
	t.Helper()
	u, err := ycrdt.EncodeStateAsUpdate(d, nil)
	if err != nil {
		t.Fatalf("encode update: %v", err)
	}
	return u
}

// TestTheMemoImageLocatorContract pins the value rule on a memo's image src. It is
// the SAME rule the whiteboard files root enforces — shared via validateLocator —
// so the two conventions cannot drift into disagreeing about what a locator is.
func TestTheMemoImageLocatorContract(t *testing.T) {
	for _, tc := range []struct {
		name   string
		src    interface{}
		reject bool
	}{
		{"ordinary file-service locator", "https://files.alkem.io/api/v1/documents/abc-123", false},
		{"relative locator", "/api/v1/documents/abc-123", false},
		{"opaque id", "abc-123", false},
		{"inline data uri", "data:image/png;base64,iVBORw0KGgo=", true},
		{"mixed-case scheme", "DATA:image/png;base64,iVBORw0KGgo=", true},
		{"upper-case scheme", "DATA:IMAGE/PNG;BASE64,iVBORw0KGgo=", true},
		// The ECMAScript trim set, each of which String.prototype.trim() strips. A
		// literal prefix comparison would walk straight past every one of them.
		{"BOM-prefixed", "\ufeffdata:image/png;base64,iVBORw0KGgo=", true},
		{"tab-prefixed", "\tdata:image/png;base64,iVBORw0KGgo=", true},
		{"line-separator-prefixed", "\u2028data:image/png;base64,iVBORw0KGgo=", true},
		{"nbsp-prefixed", "\u00a0data:image/png;base64,iVBORw0KGgo=", true},
		{"empty", "", true},
		{"whitespace only", " \t\n", true},
		{"non-string", 42, true},
		// Attribute ABSENT, not merely empty. Skipped, deliberately matching the
		// whiteboard rule: there a key absent from `files` is never inspected and
		// only a PRESENT non-string value is refused. It is also the safe direction —
		// an editor that creates the node before its upload resolves would otherwise
		// have the update refused and its generation reset mid-edit.
		{"src attribute absent", nil, false},
		// Exactly at the bound is VALID; one byte over is not. Asserted on BYTES.
		{"exactly 2048 bytes", strings.Repeat("a", maxLocatorBytes), false},
		{"2049 bytes", strings.Repeat("a", maxLocatorBytes+1), true},
		// Multibyte boundary: 1024 two-byte runes is exactly 2048 BYTES but only
		// 1024 runes. A rune-based bound would wrongly accept the 2050-byte case.
		{"1024 two-byte runes = 2048 bytes", strings.Repeat("é", maxLocatorBytes/2), false},
		{"1025 two-byte runes = 2050 bytes", strings.Repeat("é", maxLocatorBytes/2+1), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := memoRoom(t)
			setMemoImage(t, r.doc, tc.src)
			err := validateMemoImages(r.doc)
			if tc.reject && err == nil {
				t.Fatalf("src %#v was accepted; it must be refused", tc.src)
			}
			if !tc.reject && err != nil {
				t.Fatalf("src %#v was refused: %v", tc.src, err)
			}
		})
	}
}

// TestNonImageMemoNodesAreUnaffected: the check is scoped to image elements, so
// ordinary prose — including an element carrying a data: attribute that is not an
// image src — is untouched.
func TestNonImageMemoNodesAreUnaffected(t *testing.T) {
	r := memoRoom(t)
	// Integrated the same way setMemoImage does — nesting detached elements yields
	// a tree QuerySelectorAll cannot see, which would make this pass vacuously.
	frag := r.doc.GetXMLFragment(memoRoot)
	frag.Insert(0, ycrdt.ArrayAny{ycrdt.NewYXmlElement("paragraph")})
	para, ok := frag.Get(0).(*ycrdt.YXmlElement)
	if !ok {
		t.Fatalf("fragment[0] is %T, want a paragraph element", frag.Get(0))
	}
	link := ycrdt.NewYXmlElement("link")
	link.SetAttribute("href", "data:text/html,<h1>hi</h1>")
	para.Insert(0, ycrdt.ArrayAny{link})
	if n := len(frag.QuerySelectorAll("link")); n != 1 {
		t.Fatalf("fixture holds %d link node(s), want 1 — the validator would see nothing", n)
	}

	if err := validateMemoImages(r.doc); err != nil {
		t.Fatalf("a non-image node was refused: %v", err)
	}
}

// TestAValidMemoImageIsApplied is the companion that keeps the check honest: an
// ordinary locator must still land, or the validator is just breaking memos.
func TestAValidMemoImageIsApplied(t *testing.T) {
	r := memoRoom(t)

	client := ycrdt.NewDoc("client")
	applyConvention(client, model.ContentTypeMemo)
	setMemoImage(t, client, "https://files.alkem.io/api/v1/documents/abc-123")

	if got := r.applyUpdate(updateFrom(t, client), updateOrigin{src: 1}); got != applyOK {
		t.Fatalf("applyUpdate = %v, want applyOK for an ordinary locator", got)
	}
	if n := len(r.doc.GetXMLFragment(memoRoot).QuerySelectorAll("image")); n != 1 {
		t.Fatalf("the live memo holds %d image node(s), want 1", n)
	}
}

// TestMemoPoisonOnTheRealRoomPath drives a LIVE room through the Manager: run
// loop attached, update observer attached, a second member watching. That is the
// configuration in which a broadcast and a checkpoint would actually happen, so
// "nothing was relayed and nothing was stored" is tested rather than true by
// construction.
func TestMemoPoisonOnTheRealRoomPath(t *testing.T) {
	mgr, deps := testManager(t, fastConfig())
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "memo-real-path"

	author := newFakeClient(t)
	author.join(mgr, doc, model.ContentTypeMemo)
	author.observeUpdates()

	watcher := newFakeClient(t)
	watcher.join(mgr, doc, model.ContentTypeMemo)
	watcher.observeUpdates()

	mgr.mu.Lock()
	room := mgr.rooms[doc]
	mgr.mu.Unlock()
	if room == nil {
		t.Fatal("no live room")
	}

	// Emitted from the author's OWN document, exactly how a browser produces one.
	author.withDoc(func(d *ycrdt.Doc) {
		setMemoImage(t, d, "data:image/png;base64,iVBORw0KGgo=")
	})

	// Drain the run loop deterministically: the poison is queued, a no-op command goes
	// behind it, and a third join behind that only returns once both have run.
	room.enqueue(command{kind: cmdLeave})
	barrier := newFakeClient(t)
	barrier.join(mgr, doc, model.ContentTypeMemo)

	var watcherImages int
	watcher.withDoc(func(d *ycrdt.Doc) {
		watcherImages = len(d.GetXMLFragment(memoRoot).QuerySelectorAll("image"))
	})
	if watcherImages != 0 {
		t.Fatalf("the watcher holds %d image node(s); a rejected update was broadcast", watcherImages)
	}

	if !hasControlCode(author, model.CodeContentRefused) {
		t.Fatal("the sender was not ended with content-refused")
	}
	if !hasControlKind(author, model.ControlUpdateRejected) {
		t.Fatal("the sender did not receive the legacy update-rejected compatibility signal")
	}
	if hasControlCode(watcher, model.CodeContentRefused) {
		t.Fatal("a bystander was ended for another client's rejected update")
	}
	// A snapshot legitimately EXISTS here, and asserting its absence would be wrong:
	// the fixture emits the paragraph and the image as two updates, and the
	// paragraph is a perfectly valid edit that marks the room dirty. What must never
	// happen is the one the defect report measured — the checkpoint PRESERVING the
	// inline bytes. So the assertion is on content, not on existence.
	stored, err := deps.storedState(context.Background(), string(doc))
	if err == nil {
		reloaded := ycrdt.NewDoc("reloaded")
		if aerr := ycrdt.ApplyUpdateV2(reloaded, stored, nil); aerr != nil {
			t.Fatalf("decoding the stored snapshot: %v", aerr)
		}
		for i, node := range reloaded.GetXMLFragment(memoRoot).QuerySelectorAll("image") {
			el, ok := node.(*ycrdt.YXmlElement)
			if !ok {
				continue
			}
			if v, isStr := el.GetAttribute("src").(string); isStr && strings.HasPrefix(strings.ToLower(v), "data:") {
				t.Fatalf("the checkpoint preserved an inline data: locator at image[%d]; the poison reached durable storage", i)
			}
		}
	}
}
