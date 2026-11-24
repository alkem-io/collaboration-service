// Package whiteboardrender derives full-board images from a saved Excalidraw
// Y.Doc. The projection and paint path never join a room or modify source data.
package whiteboardrender

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"math"
	"slices"
	"strings"
	"time"

	yjs "github.com/antst/go-yjs/crdt"
	"github.com/gen2brain/gav1d/avif"
	"github.com/mgilbir/aster"
	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/webp"
)

const (
	maxUniqueImages = 40
	maxAssetBytes   = 3 << 20
	maxAssetSide    = 2048
	maxAssetPixels  = 3 << 20
	maxPaintBytes   = 8 << 20
)

// AssetLoader reads one saved opaque locator under the current remaining byte budget.
type AssetLoader func(context.Context, string, int64) (mime string, contents []byte, err error)

// Result carries the two complete PNGs. On failure neither artifact is returned.
type Result struct {
	Full, Card []byte
}

var errInvalidScene = errors.New("saved whiteboard cannot be rendered")

// Render fits the same complete saved scene to each bounded raster. The
// loader receives only references from the saved files root; it remains the
// caller's job to enforce storage ownership and never resolve arbitrary URLs.
func Render(ctx context.Context, doc *yjs.Doc, load AssetLoader) (out Result, err error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return render(ctx, doc, load, 1800, 720)
}

func render(ctx context.Context, doc *yjs.Doc, load AssetLoader, fullWidth, fullHeight int) (out Result, err error) {
	defer func() {
		if recover() != nil {
			// Refuse malformed projected geometry and guard the service against
			// panics inside the drawing engines. Never return a partial result.
			out, err = Result{}, errInvalidScene
		}
	}()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	items := savedElements(doc)
	opts := []aster.Option{aster.WithMemoryLimit(64 << 20), aster.WithPixelWorkLimit(64 << 20)}
	engine, err := aster.New(append(opts, fontOptions(items)...)...)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = engine.Close() }()
	raster, err := preparePaint(ctx, doc, items, engine, load)
	if err != nil {
		return Result{}, err
	}
	full, err := raster(fullWidth, fullHeight)
	if err != nil {
		return Result{}, err
	}
	card, err := raster(410, 256)
	if err != nil {
		return Result{}, err
	}
	return Result{Full: full, Card: card}, nil
}

func preparePaint(ctx context.Context, doc *yjs.Doc, items []savedElement, engine *aster.Converter, load AssetLoader) (func(int, int) ([]byte, error), error) {
	frames, labels := structuralElements(items)
	ids := make(map[string]bool, len(items))
	for _, e := range items {
		ids[e.ID] = true
	}
	left, top, w, h := view(items, frames, labels, ids)
	if math.IsNaN(left) || math.IsNaN(top) || math.IsInf(left, 0) || math.IsInf(top, 0) || w <= 0 || h <= 0 {
		return nil, errInvalidScene
	}
	background, exists := doc.GetMap("appState").Get("viewBackgroundColor").(string)
	if !exists {
		background = "#ffffff"
	} else if !paintColor.MatchString(background) {
		return nil, errInvalidScene
	}
	p := scenePainter{
		background: background, frames: frames, labels: labels, ids: ids,
		assets: assetImages{locations: doc.GetMap("files"), engine: engine, load: load, pngs: make(map[string][]byte)},
	}
	fmt.Fprintf(&p.svg, `<rect x="%g" y="%g" width="%d" height="%d" fill="%s"/>`, left, top, w, h, background)
	if err := p.paint(ctx, items); err != nil {
		return nil, err
	}
	return func(width, height int) ([]byte, error) {
		svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="%g %g %d %d">%s</svg>`,
			width, height, left, top, w, h, p.svg.String())
		return engine.SVGToPNG(svg, aster.WithContext(ctx))
	}, nil
}

type scenePainter struct {
	background     string
	frames, labels map[string]savedElement
	ids            map[string]bool
	assets         assetImages
	svg            strings.Builder
}

func (p *scenePainter) paint(ctx context.Context, items []savedElement) error {
	// The editor's inert embed pass is above ordinary elements. Retain saved
	// fractional ordering within each pass.
	slices.SortStableFunc(items, func(a, b savedElement) int {
		switch {
		case isEmbed(a) == isEmbed(b):
			return 0
		case isEmbed(a):
			return 1
		default:
			return -1
		}
	})
	for i, e := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.ids[e.ContainerID] {
			continue
		}
		clipped := beginFrameClip(&p.svg, i, e, p.frames)
		alpha := 1.0
		if f, ok := p.frames[e.FrameID]; ok {
			alpha = f.Opacity / 100
		}
		if err := p.paintElement(ctx, i, e, alpha); err != nil {
			return err
		}
		if clipped {
			p.svg.WriteString("</g>")
		}
		if p.svg.Len() > maxPaintBytes {
			return errors.New("saved whiteboard exceeds paint bound")
		}
	}
	return nil
}

func (p *scenePainter) paintElement(ctx context.Context, i int, e savedElement, alpha float64) error {
	e.Opacity *= alpha
	label := p.labels[e.ID]
	label.Opacity *= alpha
	switch e.Type {
	case "image":
		image, err := p.assets.get(ctx, e.FileID)
		if err != nil {
			return err
		}
		paintImage(&p.svg, i, e, image)
	case "text":
		paintText(&p.svg, e)
	case "frame", "magicframe":
		paintFrame(&p.svg, e)
	case "embeddable", "iframe":
		paintEmbed(&p.svg, e)
	case "arrow", "line":
		paintLinear(&p.svg, i, e, label, p.background)
	case "freedraw":
		paintFreehand(&p.svg, e)
	case "rectangle", "ellipse", "diamond":
		paintShape(&p.svg, e)
		if label.ID != "" {
			paintText(&p.svg, label)
		}
	default:
		return errors.New("unsupported saved whiteboard element")
	}
	return nil
}

type assetImages struct {
	locations     *yjs.YMap
	engine        *aster.Converter
	load          AssetLoader
	pngs          map[string][]byte
	bytes, pixels int64
}

func (a *assetImages) get(ctx context.Context, id string) ([]byte, error) {
	locator, ok := a.locations.Get(id).(string)
	if !ok || locator == "" || a.load == nil {
		return nil, errors.New("saved whiteboard has no usable asset locator")
	}
	if png, ok := a.pngs[locator]; ok {
		return png, nil
	}
	if len(a.pngs) >= maxUniqueImages || a.bytes >= maxAssetBytes || a.pixels >= maxAssetPixels {
		return nil, errors.New("saved whiteboard exceeds image-work budget")
	}
	mime, raw, err := a.load(ctx, locator, maxAssetBytes-a.bytes)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || int64(len(raw)) > maxAssetBytes-a.bytes {
		return nil, errInvalidScene
	}
	png, pixels, err := normalizeAsset(ctx, a.engine, mime, raw, maxAssetPixels-a.pixels)
	if err != nil {
		return nil, err
	}
	a.pixels += pixels
	a.bytes += int64(len(raw))
	a.pngs[locator] = png
	return png, nil
}

func normalizeAsset(ctx context.Context, engine *aster.Converter, mime string, raw []byte, remaining int64) ([]byte, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if mime == "image/svg+xml" {
		source := string(raw)
		if err := checkAssetSVG(source); err != nil {
			return nil, 0, err
		}
		output, err := engine.SVGToPNG(source, aster.WithContext(ctx))
		if err != nil {
			return nil, 0, err
		}
		cfg, err := png.DecodeConfig(bytes.NewReader(output))
		if err != nil {
			return nil, 0, err
		}
		pixels, err := boundedPixels(cfg, remaining)
		if err != nil {
			return nil, 0, err
		}
		return output, pixels, nil
	}
	return normalizeRaster(ctx, mime, raw, remaining)
}

func normalizeRaster(ctx context.Context, mime string, raw []byte, remaining int64) ([]byte, int64, error) {
	var cfg image.Config
	var err error
	if mime == "image/avif" {
		cfg, err = avif.DecodeConfig(bytes.NewReader(raw))
	} else {
		var format string
		cfg, format, err = image.DecodeConfig(bytes.NewReader(raw))
		if err == nil && "image/"+format != mime {
			return nil, 0, errors.New("stored asset does not match declared image MIME")
		}
	}
	if err != nil {
		return nil, 0, errors.New("stored asset does not match declared image MIME")
	}
	pixels, err := boundedPixels(cfg, remaining)
	if err != nil {
		return nil, 0, err
	}
	var decoded image.Image
	if mime == "image/avif" {
		decoded, err = avif.Decode(bytes.NewReader(raw), avif.Options{AutoRotate: true, FrameSizeLimit: int(remaining)})
	} else {
		decoded, _, err = image.Decode(bytes.NewReader(raw))
	}
	if err != nil {
		return nil, 0, err
	}
	bounds := decoded.Bounds()
	if _, err := boundedPixels(image.Config{Width: bounds.Dx(), Height: bounds.Dy()}, remaining); err != nil {
		return nil, 0, err
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	var pngData bytes.Buffer
	if err := (&png.Encoder{CompressionLevel: png.BestSpeed}).Encode(&pngData, decoded); err != nil {
		return nil, 0, err
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	return pngData.Bytes(), pixels, nil
}

func boundedPixels(cfg image.Config, remaining int64) (int64, error) {
	pixels := int64(cfg.Width) * int64(cfg.Height)
	if cfg.Width <= 0 || cfg.Height <= 0 || cfg.Width > maxAssetSide || cfg.Height > maxAssetSide || pixels > remaining {
		return 0, errors.New("stored asset exceeds decoded bounds")
	}
	return pixels, nil
}

func decode(data []byte) image.Image {
	decoded, err := png.Decode(bytes.NewReader(data))
	must(err)
	return decoded
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
