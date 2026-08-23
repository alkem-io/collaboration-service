package service

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// assetsRoot is the Excalidraw scene root that holds file locators.
const assetsRoot = "files"

// maxLocatorBytes bounds a single locator. The limit is on the UTF-8 byte length
// of the ORIGINAL value, not the trimmed one: what a store has to hold is what
// was sent.
const maxLocatorBytes = 2048

// validateAssetsRoot enforces the bounded locator schema on a whiteboard's
// `files` root (packages/element/src/yjs/schema.ts).
//
// A locator is a reference to a blob somewhere else. An inline `data:` URI is not
// a reference — it is the payload, smuggled into the CRDT. Clients accept such a
// document cleanly on receipt and then throw on every subsequent encode, so the
// document becomes unusable while remaining perfectly valid Yjs; discarding and
// reseeding from the server reloads the same poison forever. That is why this is
// checked on the way IN rather than repaired later.
//
// SCOPE. This is a bounded locator schema and nothing more. It is NOT update
// integrity: a semantically corrupted but structurally valid update is applied
// identically by any document that receives it and is not detectable here.
// Measured: of 2220 silent divergences produced by single-bit corruption, this
// check caught zero, and it is not intended to.
// trimECMAScript trims exactly what JavaScript's String.prototype.trim() trims.
//
// It is NOT strings.TrimSpace, and the difference is a bypass rather than a
// nicety. The fork's schema uses value.trim(), which strips U+FEFF (the BOM) —
// Go's TrimSpace does not, so "\uFEFFdata:image/..." would pass this validator,
// reach durable storage, and then wedge every client on encode: precisely the
// defect the validator exists to prevent. In the other direction TrimSpace strips
// U+0085 (NEL) while JS does not, which would make the service reject a locator
// the client considers a perfectly ordinary opaque string.
//
// The set is ECMAScript WhiteSpace + LineTerminator: TAB, VT, FF, ZWNBSP, every
// Unicode Space_Separator (which covers SP and NBSP), LF, CR, LS and PS.
func trimECMAScript(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		switch r {
		case '\t', '\v', '\f', '\uFEFF', '\n', '\r', '\u2028', '\u2029':
			return true
		}
		return unicode.Is(unicode.Zs, r)
	})
}

// validateLocator applies the bounded-locator VALUE rule to one string.
//
// Extracted so the memo image-src check and the whiteboard files check enforce
// exactly the same rule rather than two drifting copies of it. The returned
// errors are phrased to read as a suffix ("is empty"), so each caller supplies
// its own subject and the whiteboard messages stay byte-identical to what they
// were before the split.
func validateLocator(v string) error {
	// The byte bound is on the original: trimming is for the emptiness and
	// prefix checks, not for deciding how much data was actually sent.
	if len(v) > maxLocatorBytes {
		return fmt.Errorf("is %d bytes, over the %d-byte locator limit", len(v), maxLocatorBytes)
	}
	trimmed := trimECMAScript(v)
	if trimmed == "" {
		return errors.New("is empty")
	}
	// Case-insensitive on the TRIMMED value: leading whitespace or a capitalised
	// scheme would otherwise walk straight past a literal prefix comparison.
	if strings.HasPrefix(strings.ToLower(trimmed), "data:") {
		return errors.New("is an inline data: locator, not a reference")
	}
	return nil
}

func validateAssetsRoot(doc *ycrdt.Doc) error {
	// No nil guard on GetMap. It returns nil only when a root was created LOCALLY as
	// another type before being asked for as a map, and this service has no such
	// producer: applyConvention selects the whiteboard roots as maps before any
	// ingress validation, and a decoded named root arrives generic and is upgraded
	// by GetMap. A checkpoint carrying a conflicting root was not reproducible.
	files := doc.GetMap(assetsRoot)
	for _, key := range files.Keys() {
		raw := files.Get(key)
		v, ok := raw.(string)
		if !ok {
			return fmt.Errorf("files[%q] is %T, want a string locator", key, raw)
		}
		if err := validateLocator(v); err != nil {
			return fmt.Errorf("files[%q] %w", key, err)
		}
	}
	return nil
}

// cloneDoc builds an independent document holding the same state as src.
//
// Used to seed the validation shadow and to rebuild it after a rejection. It is
// the expensive operation in this design — a full encode plus a full apply,
// measured at ~0.7ms for 200 elements and ~14ms for 2000 — which is why it runs
// at room construction and on the rejection path only, never per update.
func cloneDoc(src *ycrdt.Doc, guid string) (*ycrdt.Doc, error) {
	state, err := ycrdt.EncodeStateAsUpdateV2(src, nil)
	if err != nil {
		return nil, fmt.Errorf("encoding source state: %w", err)
	}
	dst := newRoomDoc(guid)
	if err := ycrdt.ApplyUpdateV2(dst, state, nil); err != nil {
		// Destroy what we are abandoning: a Doc registers handlers and accelerators,
		// so returning without this leaks one whole document per failure.
		dst.Destroy()
		return nil, fmt.Errorf("seeding clone: %w", err)
	}
	return dst, nil
}

// memoRoot is the fragment y-prosemirror binds a memo to.
const memoRoot = "default"

// validateMemoImages enforces the SAME bounded-locator rule on a memo's image
// `src` attributes that validateAssetsRoot enforces on a whiteboard's `files`
// entries.
//
// The invariant is Anton's and it is one invariant, not two: a collaboration
// document carries REFERENCES to blobs, never blob bytes. A whiteboard smuggles
// them through `files`; a memo smuggles them through an image node's `src`. The
// value rule is shared (validateLocator) precisely so the two cannot drift.
//
// MAX_DOC_BYTES does not bound this: an inline payload is orders of magnitude
// below the limit and passes it cleanly. An authenticated WebSocket is an
// untrusted boundary, so "a client would not send that" is not a control.
//
// MISSING src is SKIPPED, mirroring the whiteboard rule: there a key absent from
// `files` is never inspected and only a PRESENT non-string value is refused. An
// editor that creates the node before its upload resolves must not have the
// update refused and its generation reset mid-edit.
func validateMemoImages(doc *ycrdt.Doc) error {
	// GetXMLFragment is safe to call: applyConvention already materializes this
	// exact root for every memo, so inspecting it grows the document nothing it
	// does not already have. That is why the whiteboard-only reasoning in
	// initShadow does not apply here.
	for i, node := range doc.GetXMLFragment(memoRoot).QuerySelectorAll("image") {
		el, ok := node.(*ycrdt.YXmlElement)
		if !ok {
			continue
		}
		if !el.HasAttribute("src") {
			continue
		}
		raw := el.GetAttribute("src")
		v, ok := raw.(string)
		if !ok {
			return fmt.Errorf("image[%d] src is %T, want a string locator", i, raw)
		}
		if err := validateLocator(v); err != nil {
			return fmt.Errorf("image[%d] src %w", i, err)
		}
	}
	return nil
}

// validateSchema applies the document convention's bounded-locator rule.
//
// One dispatch point, so the candidate-apply machinery in applyUpdate stays
// convention-agnostic and a new convention cannot silently inherit "no checks".
func (r *Room) validateSchema(doc *ycrdt.Doc) error {
	switch r.content {
	case model.ContentTypeWhiteboard:
		return validateAssetsRoot(doc)
	case model.ContentTypeMemo:
		return validateMemoImages(doc)
	}
	return nil
}
