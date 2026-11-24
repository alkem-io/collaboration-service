package whiteboardrender

// Algorithm derived from perfect-freehand 1.2.0, Stephen Ruiz Ltd, MIT.
// See notices/Perfect-Freehand.txt. This specializes the stroke to Excalidraw's
// saved-mode options (finished, round caps, no taper, ease-out sine).

import (
	"fmt"
	"math"
	"slices"
	"strings"

	rough "github.com/d2lang/rough-go"
)

type vec = rough.Point

type strokePoint struct {
	p, v        vec
	dist, total float64
	pressure    float64
}

func freehandPoints(e savedElement, size float64) []strokePoint {
	input := e.Points
	pressure := func(i int, fallback float64) float64 {
		if !e.SimulatePressure && i < len(e.Pressures) && e.Pressures[i] >= 0 {
			return e.Pressures[i]
		}
		return fallback
	}
	if len(input) == 0 {
		if e.SimulatePressure {
			return nil
		}
		input, e.Pressures = []vec{{0, 0}}, []float64{.5}
	}
	if len(input) == 2 {
		a, b := input[0], input[1]
		input = []vec{a, mix(a, b, .25), mix(a, b, .5), mix(a, b, .75), b}
		// The interpolants in the reference carry only x and y.
		e.Pressures = []float64{pressure(0, .25)}
	} else if len(input) == 1 {
		input = []vec{input[0], add(input[0], vec{1, 1})}
		e.Pressures = []float64{pressure(0, .25), pressure(0, .5)}
	}
	adjusted := []strokePoint{{p: input[0], pressure: pressure(0, .25)}}
	prev := adjusted[0]
	var total float64
	for i, current := range input[1:] {
		i++
		last := i == len(input)-1
		if !last {
			current = mix(prev.p, current, .575)
		}
		delta := sub(prev.p, current)
		d := length(delta)
		if d == 0 {
			continue
		}
		total += d
		if !last && len(adjusted) == 1 && total < size {
			continue
		}
		prev = strokePoint{p: current, v: scale(delta, 1/d), dist: d, total: total, pressure: pressure(i, .5)}
		adjusted = append(adjusted, prev)
	}
	if len(adjusted) > 1 {
		adjusted[0].v = adjusted[1].v
	}
	return adjusted
}

//nolint:gocyclo // Kept in the pinned algorithm's original order for direct point-for-point comparison.
func freehandOutline(e savedElement) []vec {
	size := e.StrokeWidth * 4.25
	points := freehandPoints(e, size)
	if len(points) == 0 || size <= 0 {
		return nil
	}
	getPressure := func(p strokePoint, previous float64) float64 {
		if !e.SimulatePressure {
			return p.pressure
		}
		speed := min(1, p.dist/size)
		return min(1, previous+(1-speed-previous)*speed*.275)
	}
	oldPressure := points[0].pressure
	for _, p := range points[:min(10, len(points))] {
		oldPressure = (oldPressure + getPressure(p, oldPressure)) / 2
	}
	prev := points[0].v
	pl, pr := points[0].p, points[0].p
	wasSharp := false
	first, last := points[0].p, points[len(points)-1].p
	const pi = math.Pi + .0001
	var radius float64
	var left, right []vec
	for i, p := range points {
		isLast := i == len(points)-1
		if !isLast && points[len(points)-1].total-p.total < 3 {
			continue
		}
		pressure := getPressure(p, oldPressure)
		radius = max(.01, size*math.Sin((.5-.6*(.5-pressure))*math.Pi/2))
		if len(points) == 1 {
			d := vec{-1, -1}
			return sweep(first, add(first, scale(perp(d), -radius/length(d))), 2*pi, 13, false, true)
		}
		next, turning := p.v, 1.0
		if !isLast {
			next = points[i+1].v
			turning = dot(p.v, next)
		}
		if dot(p.v, prev) < 0 && !wasSharp || turning < 0 {
			offset := scale(perp(prev), radius)
			left = append(left, sweep(p.p, sub(p.p, offset), pi, 13, true, true)...)
			right = append(right, sweep(p.p, add(p.p, offset), -pi, 13, true, true)...)
			pl, pr = left[len(left)-1], right[len(right)-1]
			wasSharp = wasSharp || turning < 0
			continue
		}
		wasSharp = false
		offset := scale(perp(mix(next, p.v, turning)), radius)
		l, r := sub(p.p, offset), add(p.p, offset)
		if isLast || i <= 1 || dot(sub(l, pl), sub(l, pl)) > size*size/4 {
			left, pl = append(left, l), l
		}
		if isLast || i <= 1 || dot(sub(r, pr), sub(r, pr)) > size*size/4 {
			right, pr = append(right, r), r
		}
		oldPressure, prev = pressure, p.v
	}
	start := sweep(first, right[0], pi, 13, false, true)
	end := sweep(last, add(last, scale(perp(points[len(points)-1].v), -radius)), pi*3, 29, false, false)
	slices.Reverse(right)
	return slices.Concat(left, end, right, start)
}

func sweep(center, p vec, angle float64, divisions int, includeFirst, includeLast bool) []vec {
	d := sub(p, center)
	step := 1 / float64(divisions)
	t := step
	if includeFirst {
		t = 0
	}
	out := make([]vec, 0, divisions+1)
	for ; t < 1 || includeLast && t == 1; t += step {
		s, c := math.Sincos(angle * t)
		out = append(out, add(center, vec{d[0]*c - d[1]*s, d[0]*s + d[1]*c}))
	}
	return out
}

func paintFreehand(paint *strings.Builder, e savedElement) {
	out := freehandOutline(e)
	if len(out) == 0 {
		panic("invalid saved freehand outline")
	}

	l, t, r, b := pointBounds(e.Points)
	fmt.Fprintf(paint, `<path transform="translate(%g %g) rotate(%g %g %g)" fill="%s" opacity="%g" d="M %g %g Q`,
		e.X, e.Y, e.Angle*180/math.Pi, (l+r)/2, (t+b)/2, e.StrokeColor, e.Opacity/100, out[0][0], out[0][1])
	for i, p := range out {
		m := mix(p, out[(i+1)%len(out)], .5)
		fmt.Fprintf(paint, " %g %g %g %g", p[0], p[1], m[0], m[1])
	}
	fmt.Fprintf(paint, " L %g %g Z\"/>", out[0][0], out[0][1])
}

func add(a, b vec) vec            { return vec{a[0] + b[0], a[1] + b[1]} }
func sub(a, b vec) vec            { return vec{a[0] - b[0], a[1] - b[1]} }
func scale(a vec, s float64) vec  { return vec{a[0] * s, a[1] * s} }
func mix(a, b vec, t float64) vec { return add(a, scale(sub(b, a), t)) }
func perp(a vec) vec              { return vec{a[1], -a[0]} }
func dot(a, b vec) float64        { return a[0]*b[0] + a[1]*b[1] }
func length(a vec) float64        { return math.Hypot(a[0], a[1]) }

func pointBounds(points []vec) (l, t, r, b float64) {
	l, t, r, b = math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)
	for _, p := range points {
		l, t, r, b = min(l, p[0]), min(t, p[1]), max(r, p[0]), max(b, p[1])
	}
	return
}
