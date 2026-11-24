package whiteboardrender

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"strings"
	"testing"

	yjs "github.com/antst/go-yjs/crdt"
)

func TestAssetSVGRejectsUnsupportedAndExternalReferencesBeforeRendering(t *testing.T) {
	for name, source := range map[string]string{
		"entity":          `<!DOCTYPE svg SYSTEM "https://outside.invalid/entity"><svg/>`,
		"style import":    `<?xml-stylesheet href="https://outside.invalid/style"?><svg/>`,
		"comment only":    "<!-- no image -->",
		"bad XML":         "<svg><g></svg>",
		"two roots":       "<svg/><svg/>",
		"foreign root":    `<svg xmlns="https://outside.invalid/"/>`,
		"outside text":    "<svg/> unexpected",
		"nested":          "<svg>" + strings.Repeat("<g>", 33) + strings.Repeat("</g>", 33) + "</svg>",
		"too many nodes":  "<svg>" + strings.Repeat("<rect/>", 4_001) + "</svg>",
		"too many attrs":  "<svg" + strings.Repeat(` value="1"`, 65) + "/>",
		"input bound":     "<svg><!--" + strings.Repeat("a", 512<<10) + "--></svg>",
		"embedded script": "<svg><script/></svg>",
		"link to file":    `<svg><use href="file:///private"/></svg>`,
		"remote paint":    `<svg><rect fill="url(https://outside.invalid/a)"/></svg>`,
		"remote CSS":      `<svg><rect style="fill:url(https://outside.invalid/a)"/></svg>`,
		"encoded CSS":     `<svg><rect style="fill:ur\6c(x)"/></svg>`,
		"base URL":        `<svg xml:base="https://outside.invalid/"/>`,
		"event":           `<svg onload="anything"/>`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkAssetSVG(source); err == nil {
				t.Fatal("unsupported SVG passed the safe-asset validator")
			}
		})
	}
	if err := checkAssetSVG(`<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"><defs><path id="g" d="M0 0L1 1"/></defs><use href="#g" style="fill:url('#a');stroke:url(#b)"/></svg>`); err != nil {
		t.Fatalf("embedded, local-only paint was rejected: %v", err)
	}
}

func TestAssetByteAndPixelBoundsAndDuplicateDecode(t *testing.T) {
	doc := yjs.NewDoc("asset-budget")
	defer doc.Destroy()
	files := doc.GetMap("files")
	files.Set("a", "locator")
	files.Set("b", "locator")
	var tiny bytes.Buffer
	if err := png.Encode(&tiny, image.NewRGBA(image.Rect(0, 0, 2, 3))); err != nil {
		t.Fatal(err)
	}
	calls := 0
	a := assetImages{locations: files, pngs: make(map[string][]byte), load: func(_ context.Context, locator string, maxBytes int64) (string, []byte, error) {
		calls++
		if locator != "locator" || maxBytes != maxAssetBytes {
			t.Fatalf("unexpected asset request: %s, %d", locator, maxBytes)
		}
		return "image/png", tiny.Bytes(), nil
	}}
	if _, err := a.get(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.get(t.Context(), "b"); err != nil || calls != 1 || a.pixels != 6 {
		t.Fatalf("duplicate caused another decode or changed budget: calls=%d pixels=%d err=%v", calls, a.pixels, err)
	}
	files.Set("b", "other")
	a.pixels = maxAssetPixels
	if _, err := a.get(t.Context(), "b"); err == nil || calls != 1 {
		t.Fatal("exhausted pixel budget fetched unrelated bytes")
	}
	a.pixels = 0
	a.bytes = maxAssetBytes - 1
	a.load = func(context.Context, string, int64) (string, []byte, error) { return "image/png", tiny.Bytes(), nil }
	if _, err := a.get(t.Context(), "b"); err == nil {
		t.Fatal("oversized storage response was accepted")
	}
	if _, _, err := normalizeRaster(t.Context(), "image/png", tiny.Bytes(), 5); err == nil {
		t.Fatal("asset was decoded despite exceeding remaining pixels")
	}
	if _, _, err := normalizeRaster(t.Context(), "image/jpeg", tiny.Bytes(), maxAssetPixels); err == nil {
		t.Fatal("mismatched persisted MIME was accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := normalizeAsset(ctx, nil, "image/png", tiny.Bytes(), maxAssetPixels); !errors.Is(err, context.Canceled) {
		t.Fatalf("asset validation ignored cancellation: %v", err)
	}
}

func TestBadSavedImageCannotReplaceEitherPreview(t *testing.T) {
	pixels := image.NewRGBA(image.Rect(0, 0, 4, 4))
	pixels.Set(0, 0, color.Black)
	var pngData bytes.Buffer
	if err := png.Encode(&pngData, pixels); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		props map[string]any
		data  []byte
	}{
		{"empty asset", nil, nil},
		{"truncated after image header", nil, pngData.Bytes()[:35]},
		{"invalid flip", map[string]any{"scale": yjs.ArrayAny{3., 1.}}, pngData.Bytes()},
		{"crop leaves stored image", map[string]any{"crop": yjs.MakeObject("x", 3., "y", 0., "width", 2., "height", 2., "naturalWidth", 4., "naturalHeight", 4.)}, pngData.Bytes()},
		{"dimensions of asset changed", map[string]any{"crop": yjs.MakeObject("x", 1., "y", 0., "width", 2., "height", 2., "naturalWidth", 8., "naturalHeight", 4.)}, pngData.Bytes()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := yjs.NewDoc(tc.name)
			defer doc.Destroy()
			doc.GetMap("files").Set("asset", "locator")
			props := map[string]any{
				"id": "image", "index": "a0", "type": "image", "width": 64., "height": 48.,
				"fileId": "asset", "opacity": 100., "strokeColor": "transparent", "backgroundColor": "transparent",
				"scale": yjs.ArrayAny{1., 1.},
			}
			for k, v := range tc.props {
				props[k] = v
			}
			addRenderElement(doc, props)
			out, err := Render(t.Context(), doc, func(_ context.Context, locator string, _ int64) (string, []byte, error) {
				if locator != "locator" {
					t.Fatal("wrong persisted asset")
				}
				return "image/png", tc.data, nil
			})
			if err == nil || out.Full != nil || out.Card != nil {
				t.Fatalf("invalid asset returned a successful placeholder or partial image: %v", err)
			}
		})
	}
}
