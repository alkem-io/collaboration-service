package whiteboardrender

import (
	"encoding/base64"
	"fmt"
	"html"
	"math"
	"strings"
	"unicode/utf16"

	rough "github.com/d2lang/rough-go"
)

func isEmbed(e savedElement) bool { return e.Type == "embeddable" || e.Type == "iframe" }

func paintEmbed(paint *strings.Builder, e savedElement) {
	shape := e
	if e.BackgroundColor == "transparent" && e.StrokeColor == "transparent" {
		shape.BackgroundColor, shape.FillStyle, shape.Roughness = "#d3d3d3", "solid", 0
	} else if e.Type == "iframe" {
		if e.StrokeColor == "transparent" {
			shape.StrokeColor = "#000000"
		}
		if e.BackgroundColor == "transparent" {
			shape.BackgroundColor = "#f4f4f6"
		}
	}
	paintShape(paint, shape)
	caption := e.Link
	switch {
	case e.Type == "iframe":
		caption = "IFrame element"
	case caption == "":
		caption = "Empty Web-Embed"
	}
	textUnits := len(utf16.Encode([]rune(caption)))
	size := max(min(e.Width/2, e.Width/float64(textUnits)), e.Width/30)
	caption = wrapEmbedCaption(caption, size, e.Width-20)
	ink := e.StrokeColor
	if ink == "transparent" {
		ink = "#000000"
	}
	h := size * 1.15 * float64(1+strings.Count(caption, "\n"))
	paintText(paint, savedElement{X: e.X, Y: e.Y + (e.Height-h)/2, Width: e.Width, Height: h, Angle: e.Angle,
		Text: caption, TextAlign: "center", FontFamily: 2, FontSize: size, LineHeight: 1.15, StrokeColor: ink, Opacity: 100})
}

func view(items []savedElement, frames, labels map[string]savedElement, ids map[string]bool) (float64, float64, int, int) {
	const pad = 10
	if len(items) == 0 {
		return -pad, -pad, 2 * pad, 2 * pad
	}
	l, t, r, b := math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)
	include := func(x0, y0, x1, y1 float64) {
		l, t, r, b = min(l, x0), min(t, y0), max(r, x1), max(b, y1)
	}
	for _, e := range items {
		if _, inFrame := frames[e.FrameID]; (inFrame && !isFrame(e)) || ids[e.ContainerID] {
			continue
		}
		x0, y0, x1, y1 := elementBounds(e)
		include(x0, y0, x1, y1)
		if label, ok := labels[e.ID]; ok {
			if e.Type == "arrow" {
				label = arrowLabel(e, label)
				// The editor includes the label's counter-rotated extent in
				// the arrow bounds and its world extent as a text element.
				if e.Angle != 0 {
					include(counterRotatedLabelBounds(label, (x0+x1)/2, (y0+y1)/2, e.Angle))
				}
			}
			include(elementBounds(label))
		}
		if isFrame(e) {
			t = min(t, e.Y-frameNameOffset-frameNameHeight)
		}
	}
	return l - pad, t - pad, int(r - l + 2*pad), int(b - t + 2*pad)
}

func counterRotatedLabelBounds(label savedElement, cx, cy, angle float64) (float64, float64, float64, float64) {
	x, y := label.X+label.Width/2-cx, label.Y+label.Height/2-cy
	s, c := math.Sincos(-angle)
	x, y = cx+x*c-y*s, cy+x*s+y*c
	w := (label.Width*math.Abs(c) + label.Height*math.Abs(s)) / 2
	h := (label.Width*math.Abs(s) + label.Height*math.Abs(c)) / 2
	return x - w, y - h, x + w, y + h
}

func elementBounds(e savedElement) (float64, float64, float64, float64) {
	switch e.Type {
	case "arrow", "line":
		return linearBounds(e)
	case "freedraw":
		px, py, qx, qy := pointBounds(e.Points)
		cx, cy := (px+qx)/2, (py+qy)/2
		s, c := math.Sincos(e.Angle)
		l, t, r, b := math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)
		for _, p := range e.Points {
			dx, dy := p[0]-cx, p[1]-cy
			x, y := e.X+cx+dx*c-dy*s, e.Y+cy+dx*s+dy*c
			l, t, r, b = min(l, x), min(t, y), max(r, x), max(b, y)
		}
		return l, t, r, b
	}
	cx, cy := e.X+e.Width/2, e.Y+e.Height/2
	s, c := math.Sincos(e.Angle)
	hw, hh := e.Width/2, e.Height/2
	dx, dy := hw*math.Abs(c)+hh*math.Abs(s), hw*math.Abs(s)+hh*math.Abs(c)
	switch e.Type {
	case "ellipse":
		dx, dy = math.Hypot(hw*c, hh*s), math.Hypot(hw*s, hh*c)
	case "diamond":
		dx, dy = max(hw*math.Abs(c), hh*math.Abs(s)), max(hw*math.Abs(s), hh*math.Abs(c))
	}
	return cx - dx, cy - dy, cx + dx, cy + dy
}

func paintImage(paint *strings.Builder, i int, e savedElement, content []byte) {
	sx, sy := e.Scale[0], e.Scale[1]
	if math.Abs(sx) != 1 || math.Abs(sy) != 1 {
		panic("invalid saved image flip")
	}
	cx, cy := e.Width/2, e.Height/2
	fmt.Fprintf(paint, `<g transform="translate(%g %g) rotate(%g %g %g) translate(%g %g) scale(%g %g) translate(%g %g)">`,
		e.X, e.Y, e.Angle*180/math.Pi, cx, cy, cx, cy, sx, sy, -cx, -cy)
	ix, iy, iw, ih := 0.0, 0.0, e.Width, e.Height
	if crop := e.Crop; crop != nil {
		if crop.Width <= 0 || crop.Height <= 0 || crop.X < 0 || crop.Y < 0 ||
			crop.X+crop.Width > crop.NaturalWidth || crop.Y+crop.Height > crop.NaturalHeight {
			panic("invalid saved image crop")
		}
		bounds := decode(content).Bounds()
		if crop.NaturalWidth != float64(bounds.Dx()) || crop.NaturalHeight != float64(bounds.Dy()) {
			panic("saved image crop and fixture asset dimensions disagree")
		}
		scaleX, scaleY := e.Width/crop.Width, e.Height/crop.Height
		ix, iy = -crop.X*scaleX, -crop.Y*scaleY
		iw, ih = crop.NaturalWidth*scaleX, crop.NaturalHeight*scaleY
		fmt.Fprintf(paint, `<defs><clipPath id="image%d"><rect width="%g" height="%g"/></clipPath></defs><g clip-path="url(#image%d)">`,
			i, e.Width, e.Height, i)
	}
	fmt.Fprintf(paint, `<image x="%g" y="%g" width="%g" height="%g" opacity="%g" preserveAspectRatio="none" href="data:image/png;base64,%s"/>`,
		ix, iy, iw, ih, e.Opacity/100, base64.StdEncoding.EncodeToString(content))
	if e.Crop != nil {
		paint.WriteString("</g>")
	}
	paint.WriteString("</g>")
}

func paintText(paint *strings.Builder, e savedElement) {
	font, ok := fontFaces[e.FontFamily]
	if !ok {
		panic("font family outside the text probe")
	}
	if e.fontCSS != "" {
		font.name = e.fontCSS
	}
	dx, anchor := 0.0, "start"
	switch e.TextAlign {
	case "center":
		dx, anchor = e.Width/2, "middle"
	case "right":
		dx, anchor = e.Width, "end"
	}
	// Pinned font-metadata.ts: alphabetic baseline for Excalifont at a saved
	// line height. These are stored lines, not a layout engine or rewrap.
	lineHeight := e.FontSize * e.LineHeight
	baseline := e.Y + (lineHeight+e.FontSize*font.vertical)/2
	fmt.Fprintf(paint, `<g transform="rotate(%g %g %g)">`, e.Angle*180/math.Pi, e.X+e.Width/2, e.Y+e.Height/2)
	for line := range strings.SplitSeq(e.Text, "\n") {
		fmt.Fprintf(paint, `<text x="%g" y="%g" text-anchor="%s" font-family="%s" font-size="%g" fill="%s" opacity="%g" xml:space="preserve">%s</text>`,
			e.X+dx, baseline, anchor, font.name, e.FontSize, e.StrokeColor, e.Opacity/100, html.EscapeString(line))
		baseline += lineHeight
	}
	paint.WriteString("</g>")
}

func paintShape(paint *strings.Builder, e savedElement) {
	options, dashes := roughStyle(e)
	g := rough.NewGenerator(nil, &rough.Surface{Width: e.Width, Height: e.Height})
	var d rough.Drawable
	switch e.Type {
	case "rectangle", "embeddable", "iframe":
		w, h := e.Width, e.Height
		if e.Roundness == nil {
			d = g.Rectangle(0, 0, w, h, options)
			break
		}
		r := e.Roundness.radius(min(w, h))
		options.PreserveVertices = rough.Bool(true)
		d = g.Path(fmt.Sprintf("M %g 0 L %g 0 Q %g 0 %g %g L %g %g Q %g %g %g %g L %g %g Q 0 %g 0 %g L 0 %g Q 0 0 %g 0",
			r, w-r, w, w, r, w, h-r, w, h, w-r, h, r, h, h, h-r, r, r), options)
	case "ellipse":
		options.CurveFitting = rough.Float64(1)
		d = g.Ellipse(e.Width/2, e.Height/2, e.Width, e.Height, options)
	case "diamond":
		cx, cy := math.Floor(e.Width/2)+1, math.Floor(e.Height/2)+1
		w, h := e.Width, e.Height
		if e.Roundness == nil {
			d = g.Polygon([]rough.Point{{cx, 0}, {w, cy}, {cx, h}, {0, cy}}, options)
			break
		}
		x, y := e.Roundness.radius(cx), e.Roundness.radius(cy)
		options.PreserveVertices = rough.Bool(true)
		d = g.Path(fmt.Sprintf("M %g %g L %g %g C %g %g %g %g %g %g L %g %g C %g %g %g %g %g %g L %g %g C 0 %g 0 %g %g %g L %g %g C %g 0 %g 0 %g %g",
			cx+x, y, w-x, cy-y, w, cy, w, cy, w-x, cy+y, cx+x, h-y, cx, h, cx, h, cx-x, h-y, x, cy+y, cy, cy, x, cy-y, cx-x, y, cx, cx, cx+x, y), options)
	default:
		panic("element is outside the fixture subset")
	}
	fmt.Fprintf(paint, `<g transform="translate(%g %g) rotate(%g %g %g)">`,
		e.X, e.Y, e.Angle*180/math.Pi, e.Width/2, e.Height/2)
	paintPaths(paint, g.ToPaths(d), e.Opacity, dashes)
	paint.WriteString("</g>")
}

func roughStyle(e savedElement) (*rough.Options, string) {
	weight := e.StrokeWidth
	var dashes string
	switch e.StrokeStyle {
	case "dashed":
		weight += 0.5
		dashes = fmt.Sprintf(` stroke-dasharray="8,%g"`, 8+e.StrokeWidth)
	case "dotted":
		weight += 0.5
		dashes = fmt.Sprintf(` stroke-dasharray="1.5,%g"`, 6+e.StrokeWidth)
	}
	r := effectiveRoughness(e)
	options := &rough.Options{
		Seed: rough.Float64(e.Seed), Stroke: rough.String(e.StrokeColor), StrokeWidth: rough.Float64(weight),
		DisableMultiStroke: rough.Bool(dashes != ""), FillWeight: rough.Float64(e.StrokeWidth / 2),
		HachureGap: rough.Float64(e.StrokeWidth * 4), Roughness: rough.Float64(r),
		PreserveVertices: rough.Bool(e.Roughness < 2), FillStyle: rough.String(e.FillStyle),
		FixedDecimalPlaceDigits: rough.Float64(2),
	}
	fill := e.BackgroundColor != "transparent"
	switch e.Type {
	case "arrow":
		fill = false
	case "line":
		fill = fill && len(e.Points) >= 3 && dist(e.Points[0], e.Points[len(e.Points)-1]) <= 8
	}
	if fill {
		options.Fill = rough.String(e.BackgroundColor)
	}
	return options, dashes
}

func effectiveRoughness(e savedElement) float64 {
	r := e.Roughness
	wide, narrow := max(e.Width, e.Height), min(e.Width, e.Height)
	if !(narrow >= 20 && wide >= 50 || narrow >= 15 && e.Roundness != nil ||
		(e.Type == "arrow" || e.Type == "line") && wide >= 50) {
		scale := 2.0
		if wide < 10 {
			scale = 3
		}
		r = min(r/scale, 2.5)
	}
	return r
}

func dist(a, b rough.Point) float64 { return math.Hypot(b[0]-a[0], b[1]-a[1]) }

func paintPaths(paint *strings.Builder, paths []rough.PathInfo, opacity float64, dashes string) {
	for i, p := range paths {
		if p.Pattern != nil {
			// The dependency represents a filled curved shape as a clip
			// and a separate sketch. The sketch spans this element's own
			// surface; serializing only p.D produces a black shape.
			id := paint.Len()
			fmt.Fprintf(paint, `<defs><clipPath id="sketch%d"><path d="%s"/></clipPath></defs><g clip-path="url(#sketch%d)">`,
				id, p.D, id)
			paintPaths(paint, []rough.PathInfo{p.Pattern.Path}, opacity, "")
			paint.WriteString("</g>")
			continue
		}
		dash := ""
		if i == len(paths)-1 {
			dash = dashes
		}
		fmt.Fprintf(paint, `<path d="%s" fill="%s" stroke="%s" stroke-width="%g" stroke-linecap="round" stroke-linejoin="round" opacity="%g"%s/>`,
			p.D, p.Fill, p.Stroke, p.StrokeWidth, opacity/100, dash)
	}
}
