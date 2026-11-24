package whiteboardrender

import (
	"bytes"
	"context"
	"errors"
	"image/color"
	"image/png"
	"math"
	"strconv"
	"testing"

	yjs "github.com/antst/go-yjs/crdt"
)

func addRenderElement(doc *yjs.Doc, props map[string]any) {
	node := yjs.NewYMap(nil)
	doc.GetMap("elements").Set(props["id"].(string), node)
	for key, val := range props {
		node.Set(key, val)
	}
}

func TestRenderSavedShapeAndClear(t *testing.T) {
	doc := yjs.NewDoc("shape-preview")
	defer doc.Destroy()
	doc.GetMap("appState").Set("viewBackgroundColor", "#ffffff")
	addRenderElement(doc, map[string]any{
		"id": "shape", "index": "a0", "type": "rectangle", "x": -80., "y": -20., "width": 250., "height": 80.,
		"opacity": 100., "strokeWidth": 1., "strokeStyle": "solid", "fillStyle": "solid",
		"strokeColor": "#ff0000", "backgroundColor": "#ff0000", "seed": 58.,
	})
	before, err := yjs.EncodeStateAsUpdateV2(doc, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Render(t.Context(), doc, nil)
	if err != nil {
		t.Fatal(err)
	}
	check := func(out Result, expected color.NRGBA) {
		t.Helper()
		for _, tc := range []struct {
			b    []byte
			w, h int
		}{{out.Full, 1800, 720}, {out.Card, 410, 256}} {
			img, err := png.Decode(bytes.NewReader(tc.b))
			if err != nil {
				t.Fatal(err)
			}
			if img.Bounds().Dx() != tc.w || img.Bounds().Dy() != tc.h {
				t.Fatalf("unexpected PNG size: %v", img.Bounds())
			}
			if got := color.NRGBAModel.Convert(img.At(tc.w/2, tc.h/2)); got != expected {
				t.Fatalf("center pixel = %v, want %v", got, expected)
			}
		}
	}
	check(out, color.NRGBA{R: 255, A: 255})
	after, err := yjs.EncodeStateAsUpdateV2(doc, nil)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("render changed the saved doc")
	}
	doc.GetMap("elements").Delete("shape")
	out, err = Render(t.Context(), doc, nil)
	if err != nil {
		t.Fatal(err)
	}
	check(out, color.NRGBA{R: 255, G: 255, B: 255, A: 255})
}

func TestRenderMissingAssetInvalidSVGAndCancellationAreNotPartial(t *testing.T) {
	doc := yjs.NewDoc("image-preview")
	defer doc.Destroy()
	addRenderElement(doc, map[string]any{
		"id": "image", "index": "a0", "type": "image", "width": 64., "height": 48.,
		"fileId": "image-key", "opacity": 100., "strokeColor": "transparent", "backgroundColor": "transparent",
	})
	if out, err := Render(t.Context(), doc, nil); err == nil || out.Full != nil || out.Card != nil {
		t.Fatalf("missing image produced a partial preview: %v", err)
	}
	doc.GetMap("files").Set("image-key", "locator")
	if out, err := Render(t.Context(), doc, func(_ context.Context, locator string, limit int64) (string, []byte, error) {
		if locator != "locator" || limit <= 0 {
			t.Fatal("wrong asset request")
		}
		return "image/svg+xml", []byte(`<svg xmlns="http://www.w3.org/2000/svg" width="64" height="48"><foreignObject/></svg>`), nil
	}); err == nil || out.Full != nil || out.Card != nil {
		t.Fatalf("unsupported SVG produced a preview: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if out, err := Render(ctx, doc, nil); !errors.Is(err, context.Canceled) || out.Full != nil || out.Card != nil {
		t.Fatalf("cancelled render: %v", err)
	}
}

func TestMinimalAndSharpFreehandStrokesStayVisible(t *testing.T) {
	point := func(x, y float64) yjs.ArrayAny { return yjs.ArrayAny{x, y} }
	for _, tc := range []struct {
		name     string
		points   yjs.ArrayAny
		simulate bool
	}{
		{"pen tap", yjs.ArrayAny{point(0, 0)}, false},
		{"repeated stationary sample", yjs.ArrayAny{point(0, 0), point(0, 0)}, false},
		{"short mouse stroke", yjs.ArrayAny{point(0, 0), point(2, 1)}, true},
		{"sharp reversal with jitter", yjs.ArrayAny{point(0, 0), point(1, 0), point(90, 0), point(10, 0), point(8, 1)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := yjs.NewDoc(tc.name)
			defer doc.Destroy()
			addRenderElement(doc, map[string]any{
				"id": "stroke", "index": "a0", "type": "freedraw", "width": 90., "height": 1.,
				"opacity": 100., "strokeWidth": 2., "strokeColor": "#000000", "backgroundColor": "transparent",
				"simulatePressure": tc.simulate, "points": tc.points, "pressures": yjs.ArrayAny{.5, .5},
			})
			out, err := Render(t.Context(), doc, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, preview := range [][]byte{out.Full, out.Card} {
				img, err := png.Decode(bytes.NewReader(preview))
				if err != nil {
					t.Fatal(err)
				}
				ink := 0
				for y := range img.Bounds().Dy() {
					for x := range img.Bounds().Dx() {
						r, _, _, a := img.At(x, y).RGBA()
						if a == 0xffff && r < 0x8000 {
							ink++
						}
					}
				}
				if ink < 3 || ink > img.Bounds().Dx()*img.Bounds().Dy()/4 {
					t.Fatalf("stroke was missing or filled the preview: %d ink pixels in %v", ink, img.Bounds())
				}
			}
		})
	}
}

func TestInvalidSavedGeometryDoesNotReturnImages(t *testing.T) {
	for _, tc := range []struct {
		name  string
		props map[string]any
	}{
		{"missing freehand samples", map[string]any{"type": "freedraw", "simulatePressure": true}},
		{"unknown text font", map[string]any{"type": "text", "text": "Text", "fontFamily": 99, "fontSize": 20., "lineHeight": 1.2}},
		{"arrow with missing endpoint", map[string]any{"type": "arrow", "points": yjs.ArrayAny{yjs.ArrayAny{0., 0.}}}},
		{"invalid frame transform", map[string]any{"type": "frame", "angle": math.Pi / 6}},
		{"no visible root geometry", map[string]any{"containerId": "broken"}},
		{"reflexive clip on rotated frame", map[string]any{"type": "frame", "angle": math.Pi / 6, "frameId": "broken"}},
		{"arrowhead without a supported shape", map[string]any{
			"type": "arrow", "points": yjs.ArrayAny{yjs.ArrayAny{0., 0.}, yjs.ArrayAny{90., 40.}},
			"endArrowhead": "unsupported",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := yjs.NewDoc(tc.name)
			defer doc.Destroy()
			props := map[string]any{
				"id": "broken", "index": "a0", "type": "rectangle", "width": 90., "height": 40.,
				"opacity": 100., "strokeWidth": 2., "strokeColor": "#000000", "backgroundColor": "transparent",
			}
			for k, v := range tc.props {
				props[k] = v
			}
			addRenderElement(doc, props)
			out, err := Render(t.Context(), doc, nil)
			if !errors.Is(err, errInvalidScene) || out.Full != nil || out.Card != nil {
				t.Fatalf("invalid saved geometry returned output: %v", err)
			}
		})
	}
}

func TestUntitledFramesAndEmptyEmbedsStillRenderTheirPlaceholders(t *testing.T) {
	for _, kind := range []string{"frame", "magicframe", "embeddable", "iframe"} {
		t.Run(kind, func(t *testing.T) {
			doc := yjs.NewDoc(kind)
			defer doc.Destroy()
			addRenderElement(doc, map[string]any{
				"id": "item", "index": "a0", "type": kind, "width": 180., "height": 90.,
				"opacity": 100., "strokeWidth": 2., "fillStyle": "solid",
				"strokeColor": "transparent", "backgroundColor": "transparent",
			})
			out, err := Render(t.Context(), doc, func(context.Context, string, int64) (string, []byte, error) {
				t.Fatal("placeholder attempted asset I/O")
				return "", nil, nil
			})
			if err != nil || !previewContainsInk(t, out.Full) || !previewContainsInk(t, out.Card) {
				t.Fatalf("empty %s collapsed to a blank preview: %v", kind, err)
			}
		})
	}
}

func previewContainsInk(t *testing.T, preview []byte) bool {
	t.Helper()
	img, err := png.Decode(bytes.NewReader(preview))
	if err != nil {
		t.Fatal(err)
	}
	for y := range img.Bounds().Dy() {
		for x := range img.Bounds().Dx() {
			r, g, b, a := img.At(x, y).RGBA()
			if a == 0xffff && min(r, g, b) < 0xe000 {
				return true
			}
		}
	}
	return false
}

func TestInvalidSceneRefusesBothOutputsBeforeResolvingAssets(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*yjs.Doc, *yjs.YMap, *yjs.YMap)
	}{
		{"huge scene", func(_ *yjs.Doc, root, _ *yjs.YMap) {
			for i := 0; i < 2601; i++ {
				root.Set(strconv.Itoa(i), true)
			}
		}},
		{"non-map entry", func(_ *yjs.Doc, root, _ *yjs.YMap) { root.Set("malformed", true) }},
		{"wrong field type", func(_ *yjs.Doc, _, shape *yjs.YMap) { shape.Set("width", "wide") }},
		{"unsupported shape", func(_ *yjs.Doc, _, shape *yjs.YMap) { shape.Set("type", "not-in-editor") }},
		{"overflow coordinate", func(_ *yjs.Doc, _, shape *yjs.YMap) { shape.Set("x", 1e9) }},
		{"negative width", func(_ *yjs.Doc, _, shape *yjs.YMap) { shape.Set("width", -1) }},
		{"invalid roundness", func(_ *yjs.Doc, _, shape *yjs.YMap) { shape.Set("roundness", yjs.MakeObject("type", 99)) }},
		{"over-budget hatching", func(_ *yjs.Doc, _, shape *yjs.YMap) {
			shape.Set("fillStyle", "hachure")
			shape.Set("width", 9e5)
			shape.Set("height", 9e5)
		}},
		{"invalid background", func(d *yjs.Doc, _, _ *yjs.YMap) {
			d.GetMap("appState").Set("viewBackgroundColor", `url("https://outside.invalid")`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := yjs.NewDoc(tc.name)
			defer d.Destroy()
			addRenderElement(d, map[string]any{
				"id": "shape", "index": "a0", "type": "rectangle", "width": 80., "height": 40.,
				"opacity": 100., "strokeWidth": 1., "strokeStyle": "solid", "fillStyle": "solid",
				"strokeColor": "#003300", "backgroundColor": "#00ff00", "seed": 58.,
			})
			root := d.GetMap("elements")
			tc.edit(d, root, root.Get("shape").(*yjs.YMap))
			out, err := Render(t.Context(), d, func(context.Context, string, int64) (string, []byte, error) {
				t.Fatal("invalid geometry triggered asset I/O")
				return "", nil, nil
			})
			if err == nil || out.Full != nil || out.Card != nil {
				t.Fatalf("invalid input produced partial output: %+v, %v", out, err)
			}
		})
	}
}
