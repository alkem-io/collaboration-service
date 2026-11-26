package whiteboardrender

import (
	"fmt"
	"html"
	"math"
	"strings"

	rough "github.com/d2lang/rough-go"
)

const (
	frameRadius     = 8
	frameNameOffset = 3
	frameNameSize   = 14
	frameNameHeight = 1.25 * frameNameSize
)

func structuralElements(items []savedElement) (map[string]savedElement, map[string]savedElement) {
	frames, labels := map[string]savedElement{}, map[string]savedElement{}
	for _, e := range items {
		if isFrame(e) {
			frames[e.ID] = e
		}
		if e.Type == "text" && e.ContainerID != "" {
			labels[e.ContainerID] = e
		}
	}
	return frames, labels
}

func isFrame(e savedElement) bool { return e.Type == "frame" || e.Type == "magicframe" }

func beginFrameClip(paint *strings.Builder, i int, e savedElement, frames map[string]savedElement) bool {
	f, ok := frames[e.FrameID]
	if !ok {
		return false
	}
	if f.Angle != 0 {
		panic("frame clipping outside this probe")
	}
	if len(e.GroupIds) == 0 {
		x0, y0, x1, y1 := elementBounds(e)
		if x0 >= f.X+f.Width || y0 >= f.Y+f.Height || x1 <= f.X || y1 <= f.Y {
			return false
		}
	}
	fmt.Fprintf(paint, `<defs><clipPath id="clip%d" clipPathUnits="userSpaceOnUse"><rect x="%g" y="%g" width="%g" height="%g" rx="%d"/></clipPath></defs><g clip-path="url(#clip%d)">`,
		i, f.X, f.Y, f.Width, f.Height, frameRadius, i)
	return true
}

func paintFrame(paint *strings.Builder, e savedElement) {
	if e.Angle != 0 {
		panic("frame title/geometry outside this probe")
	}
	// A short title (no truncation) with the editor's declared Helvetica
	// metrics. Use Liberation Sans rather than a host-installed face.
	y := e.Y - frameNameOffset - frameNameHeight +
		(frameNameHeight+frameNameSize*(1577.0-471)/2048)/2
	stroke := "#bbb"
	if e.Type == "magicframe" {
		stroke = "#7affd7"
	}
	name := e.Name
	if name == "" {
		name = "Frame"
		if e.Type == "magicframe" {
			name = "AI Frame"
		}
	}
	name = clipFrameTitle(name, frameNameSize, e.Width)
	fmt.Fprintf(paint, `<text x="%g" y="%g" font-family="Liberation Sans" font-size="%d" fill="#999999">%s</text><rect x="%g" y="%g" width="%g" height="%g" rx="%d" fill="none" stroke="%s" stroke-width="2" opacity="%g"/>`,
		e.X, y, frameNameSize, html.EscapeString(name),
		e.X, e.Y, e.Width, e.Height, frameRadius, stroke, e.Opacity/100)
}

func arrowLabel(e, label savedElement) savedElement {
	if len(e.Points) < 2 || label.Angle != 0 {
		panic("line transform outside this probe")
	}
	n := len(e.Points)
	center := e.Points[n/2]
	if n%2 == 0 {
		center = mix(e.Points[n/2-1], center, .5)
		if e.Roundness != nil && !e.Elbowed {
			center = roundedSegmentMidpoint(e, n/2)
		}
	}
	at := lineCoordinates(e)(center)
	label.X = at[0] - label.Width/2
	label.Y = at[1] - label.Height/2
	return label
}

// The editor anchors an even-point rounded arrow's label halfway along the
// central collision curve, not halfway between the segment's endpoints.
func roundedSegmentMidpoint(e savedElement, segment int) rough.Point {
	opts := &rough.Options{
		Seed: rough.Float64(e.Seed), Roughness: rough.Float64(0),
		DisableMultiStroke: rough.Bool(true), PreserveVertices: rough.Bool(true),
	}
	ops := curveOps(rough.NewGenerator(nil).Curve(e.Points, opts))
	prev, op := ops[segment-1].Data, ops[segment].Data
	a, b, c, d := rough.Point{prev[len(prev)-2], prev[len(prev)-1]}, rough.Point{op[0], op[1]}, rough.Point{op[2], op[3]}, rough.Point{op[4], op[5]}
	const steps = 64
	var points [steps + 1]rough.Point
	var lengths [steps + 1]float64
	points[0] = a
	for i := 1; i <= steps; i++ {
		t := float64(i) / steps
		points[i] = mix(mix(mix(a, b, t), mix(b, c, t), t), mix(mix(b, c, t), mix(c, d, t), t), t)
		lengths[i] = lengths[i-1] + dist(points[i-1], points[i])
	}
	half := lengths[steps] / 2
	for i := 1; i <= steps; i++ {
		if half < lengths[i] {
			return mix(points[i-1], points[i], (half-lengths[i-1])/(lengths[i]-lengths[i-1]))
		}
	}
	return a // The saved segment has zero arc length.
}

func paintLinear(paint *strings.Builder, i int, e, label savedElement, background string) {
	if len(e.Points) < 2 {
		panic("line geometry outside this probe")
	}
	options, dashes := roughStyle(e)
	g := rough.NewGenerator(nil)
	d := linearShaft(g, options, e)
	if label.ID != "" {
		label = arrowLabel(e, label)
		// Equivalent to clearing the label rectangle on the arrow's own
		// temporary layer. A transparent hole must reveal earlier elements,
		// not the global board background.
		l, t, r, b := linearBounds(e)
		x, y := min(l, label.X)-20, min(t, label.Y)-20
		w, h := max(r, label.X+label.Width)-x+20, max(b, label.Y+label.Height)-y+20
		fmt.Fprintf(paint, `<defs><mask id="label%d" maskUnits="userSpaceOnUse" x="%g" y="%g" width="%g" height="%g"><rect x="%g" y="%g" width="%g" height="%g" fill="white"/><rect x="%g" y="%g" width="%g" height="%g" fill="black"/></mask></defs><g mask="url(#label%d)">`,
			i, x, y, w, h, x, y, w, h, label.X, label.Y, label.Width, label.Height, i)
	}
	cx, cy := shaftCenter(d)
	fmt.Fprintf(paint, `<g transform="translate(%g %g) rotate(%g %g %g)">`,
		e.X, e.Y, e.Angle*180/math.Pi, cx, cy)
	paintPaths(paint, g.ToPaths(d), e.Opacity, dashes)
	paintHead(paint, g, d, options, e, background, e.StartArrowhead, true)
	paintHead(paint, g, d, options, e, background, e.EndArrowhead, false)
	paint.WriteString("</g>")
	if label.ID != "" {
		paint.WriteString("</g>")
		paintText(paint, label)
	}
}

func linearShaft(g *rough.Generator, opts *rough.Options, e savedElement) rough.Drawable {
	switch {
	case e.Elbowed:
		elbowOpts := *opts
		elbowOpts.PreserveVertices = rough.Bool(true)
		return g.Path(elbowPath(e.Points), &elbowOpts)
	case e.Roundness != nil:
		return g.Curve(e.Points, opts)
	case opts.Fill != nil:
		return g.Polygon(e.Points, opts)
	default:
		return g.LinearPath(e.Points, opts)
	}
}

func shaftCenter(d rough.Drawable) (float64, float64) {
	l, t, r, b := shaftBounds(d, func(p rough.Point) rough.Point { return p })
	return (l + r) / 2, (t + b) / 2
}

func curveOps(d rough.Drawable) []rough.Op {
	for _, set := range d.Sets {
		if set.Type == rough.OpSetPath {
			return set.Ops
		}
	}
	return nil
}

// Canvas chooses a pivot from the unrotated shaft's true cubic extrema.
// Bounds also cover the rotated cubic, rather than its saved control polygon.
func shaftBounds(d rough.Drawable, transform func(rough.Point) rough.Point) (float64, float64, float64, float64) {
	x0, y0, x1, y1 := math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)
	add := func(p rough.Point) {
		x0, y0, x1, y1 = min(x0, p[0]), min(y0, p[1]), max(x1, p[0]), max(y1, p[1])
	}
	var cursor rough.Point
	for _, op := range curveOps(d) {
		v := op.Data
		switch op.Op {
		case rough.OpMove:
			cursor = transform(rough.Point{v[0], v[1]})
		case rough.OpLineTo:
			add(cursor)
			cursor = transform(rough.Point{v[0], v[1]})
			add(cursor)
		case rough.OpBCurveTo:
			c := [4]rough.Point{cursor, transform(rough.Point{v[0], v[1]}), transform(rough.Point{v[2], v[3]}), transform(rough.Point{v[4], v[5]})}
			add(c[0])
			add(c[3])
			for axis := range 2 {
				p, q, r, s := c[0][axis], c[1][axis], c[2][axis], c[3][axis]
				a, b, d := 3*(s-3*r+3*q-p), 6*(r-2*q+p), 3*(q-p)
				for _, t := range realRoots(a, b, d) {
					if t > 0 && t < 1 {
						z := 1 - t
						add(rough.Point{z*z*z*c[0][0] + 3*t*z*z*c[1][0] + 3*t*t*z*c[2][0] + t*t*t*c[3][0],
							z*z*z*c[0][1] + 3*t*z*z*c[1][1] + 3*t*t*z*c[2][1] + t*t*t*c[3][1]})
					}
				}
			}
			cursor = c[3]
		}
	}
	return x0, y0, x1, y1
}

func realRoots(a, b, c float64) []float64 {
	if a == 0 {
		if b == 0 {
			return nil
		}
		return []float64{-c / b}
	}
	d := b*b - 4*a*c
	if d < 0 {
		return nil
	}
	rd := math.Sqrt(d)
	return []float64{(-b - rd) / (2 * a), (-b + rd) / (2 * a)}
}

func lineCoordinates(e savedElement) func(rough.Point) rough.Point {
	s, c := math.Sincos(e.Angle)
	var cx, cy float64
	if e.Angle != 0 {
		opts, _ := roughStyle(e)
		cx, cy = shaftCenter(linearShaft(rough.NewGenerator(nil), opts, e))
	}
	return func(p rough.Point) rough.Point {
		x, y := p[0]-cx, p[1]-cy
		return rough.Point{e.X + cx + x*c - y*s, e.Y + cy + x*s + y*c}
	}
}

func linearBounds(e savedElement) (float64, float64, float64, float64) {
	opts, _ := roughStyle(e)
	return shaftBounds(linearShaft(rough.NewGenerator(nil), opts, e), lineCoordinates(e))
}

// shape.ts uses a quadratic corner limited by the two saved leg lengths.
// Do not recalculate leg routing: it is part of the persisted scene.
func elbowPath(points []rough.Point) string {
	var path strings.Builder
	fmt.Fprintf(&path, "M %g %g", points[0][0], points[0][1])
	for i, p := range points[1 : len(points)-1] {
		a, b := points[i], points[i+2]
		da, db := math.Hypot(a[0]-p[0], a[1]-p[1]), math.Hypot(b[0]-p[0], b[1]-p[1])
		r := min(16, da/2, db/2)
		if r == 0 {
			continue
		}
		fmt.Fprintf(&path, " L %g %g Q %g %g %g %g", p[0]+(a[0]-p[0])*r/da, p[1]+(a[1]-p[1])*r/da,
			p[0], p[1], p[0]+(b[0]-p[0])*r/db, p[1]+(b[1]-p[1])*r/db)
	}
	end := points[len(points)-1]
	fmt.Fprintf(&path, " L %g %g", end[0], end[1])
	return path.String()
}

//nolint:gocyclo // The cases are a direct mapping of the editor's supported arrowhead enum.
func paintHead(paint *strings.Builder, g *rough.Generator, shaft rough.Drawable, options *rough.Options, e savedElement, background, kind string, start bool) {
	if kind == "" {
		return
	}
	tip, back, leg := arrowDirection(e, shaft, start)
	opts := *options
	opts.Roughness = rough.Float64(min(1, *options.Roughness))
	opts.FillStyle = rough.String("solid")
	draw := func(d rough.Drawable, dash string) { paintPaths(paint, g.ToPaths(d), e.Opacity, dash) }
	drawLine := func(a, b rough.Point, dash string) { draw(g.Line(a[0], a[1], b[0], b[1], &opts), dash) }
	point := func(p rough.Point, distance, angle float64) rough.Point {
		s, c := math.Sincos(angle * math.Pi / 180)
		return rough.Point{p[0] + distance*(back[0]*c-back[1]*s), p[1] + distance*(back[0]*s+back[1]*c)}
	}
	var lineDash string
	if e.StrokeStyle == "dotted" {
		lineDash = fmt.Sprintf(` stroke-dasharray="1.5,%g"`, 4+e.StrokeWidth)
	}
	marker := func(offset float64) {
		size := min(20, leg/2)
		at := point(tip, size*offset, 0)
		drawLine(point(at, size, -25), point(at, size, 25), lineDash)
	}
	circle := func(fill string, offset, scale float64) {
		size := min(15, leg/2)
		at := point(tip, offset*size, 0)
		circleOpts := opts
		circleOpts.Fill = rough.String(fill)
		circleOpts.Roughness = rough.Float64(min(.5, *options.Roughness))
		draw(g.Circle(at[0], at[1], max(0, size+e.StrokeWidth-2)*scale, &circleOpts), "")
	}
	crowfoot := func() {
		size := min(15, leg/2)
		at := point(tip, size, 0)
		drawLine(point(at, -size, -25), at, lineDash)
		drawLine(point(at, -size, 25), at, lineDash)
	}
	switch kind {
	case "arrow", "bar":
		size, angle := min(25, leg/2), 20.0
		if kind == "bar" {
			size, angle = min(15, leg/2), 90
		}
		drawLine(point(tip, size, -angle), tip, lineDash)
		drawLine(point(tip, size, angle), tip, lineDash)
	case "triangle", "triangle_outline", "diamond", "diamond_outline":
		opts.Fill = rough.String(e.StrokeColor)
		if strings.HasSuffix(kind, "_outline") {
			opts.Fill = rough.String(background)
		}
		size := min(15, leg/2)
		diamond := strings.HasPrefix(kind, "diamond")
		if diamond {
			size = min(12, leg/4)
		}
		vertices := []rough.Point{tip, point(tip, size, -25)}
		if diamond {
			vertices = append(vertices, point(tip, 2*size, 0))
		}
		vertices = append(vertices, point(tip, size, 25), tip)
		draw(g.Polygon(vertices, &opts), "")
	case "circle":
		circle(e.StrokeColor, 0, 1)
	case "circle_outline":
		circle(background, 0, 1)
	case "cardinality_one":
		marker(0)
	case "cardinality_many":
		crowfoot()
	case "cardinality_one_or_many":
		crowfoot()
		marker(-.25)
	case "cardinality_exactly_one":
		marker(-.5)
		marker(0)
	case "cardinality_zero_or_one":
		circle(background, 1.5, .8)
		marker(-.5)
	case "cardinality_zero_or_many":
		crowfoot()
		circle(background, 1.5, .8)
	default:
		panic("unknown arrowhead: " + kind)
	}
}

func arrowDirection(e savedElement, shaft rough.Drawable, start bool) (rough.Point, rough.Point, float64) {
	ops := curveOps(shaft)
	i := len(ops) - 1
	p, q := e.Points[len(e.Points)-2], e.Points[len(e.Points)-1]
	if start {
		i = 1
		p, q = e.Points[1], e.Points[0]
	}
	if i < 1 || i >= len(ops) || ops[i].Op != rough.OpBCurveTo {
		panic("arrow curve does not end in a cubic")
	}
	last, prev := ops[i].Data, ops[i-1].Data
	tip := rough.Point{last[4], last[5]}
	if start {
		tip = rough.Point{prev[len(prev)-2], prev[len(prev)-1]}
	}
	const t = .3
	direction := rough.Point{}
	for i := range 2 {
		u := 1 - t
		near := u*u*u*last[4+i] + 3*t*u*u*last[2+i] +
			3*t*t*(1-t)*last[i] + t*t*t*prev[len(prev)-2+i]
		direction[i] = near - tip[i]
	}
	d := math.Hypot(direction[0], direction[1])
	if d == 0 {
		panic("degenerate arrow tangent")
	}
	return tip, rough.Point{direction[0] / d, direction[1] / d}, math.Hypot(q[0]-p[0], q[1]-p[1])
}
