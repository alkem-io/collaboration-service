package whiteboardrender

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
)

var paintColor = regexp.MustCompile(`^(#[0-9a-fA-F]{3,8}|[a-zA-Z]{1,24}|rgba?\([0-9.,% /]+\)|hsla?\([0-9.,% /]+\))$`)

// XML parsing is intentionally stricter than browser input recovery.
// Unsupported asset paint fails visibly before the renderer is called.
func checkAssetSVG(src string) error {
	if len(src) > 512<<10 {
		return errors.New("asset SVG exceeds input bound")
	}
	decoder := xml.NewDecoder(strings.NewReader(src))
	nodes, depth := 0, 0
	for {
		raw, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch t := raw.(type) {
		case xml.StartElement:
			nodes++
			depth++
			if depth > 32 || nodes > 4000 || len(t.Attr) > 64 {
				return errors.New("asset SVG exceeds tree bounds")
			}
			if err := checkSVGElement(t, depth == 1, nodes == 1); err != nil {
				return err
			}
		case xml.EndElement:
			depth--
		default:
			if err := checkSVGOther(t, depth, nodes); err != nil {
				return err
			}
		}
	}
	if nodes == 0 || depth != 0 {
		return errors.New("asset SVG has no complete root")
	}
	return nil
}

func checkSVGOther(raw xml.Token, depth, nodes int) error {
	switch t := raw.(type) {
	case xml.Directive:
		return errors.New("asset SVG XML directives are disabled")
	case xml.ProcInst:
		if t.Target != "xml" || nodes != 0 {
			return errors.New("asset SVG processing instructions are disabled")
		}
	case xml.CharData:
		if depth == 0 && len(strings.TrimSpace(string(t))) != 0 {
			return errors.New("unexpected content outside SVG")
		}
	}
	return nil
}

func checkSVGElement(t xml.StartElement, root, first bool) error {
	const svgNS = "http://www.w3.org/2000/svg"
	if t.Name.Space != "" && t.Name.Space != svgNS || root && (!first || t.Name.Local != "svg") {
		return errors.New("asset is not one SVG tree")
	}
	switch t.Name.Local {
	case "svg", "title", "desc", "g", "defs", "symbol", "use", "path", "rect", "circle",
		"ellipse", "line", "polyline", "polygon", "clipPath", "linearGradient", "radialGradient",
		"stop", "mask", "pattern", "marker":
	default:
		return fmt.Errorf("unsupported asset SVG node %q", t.Name.Local)
	}
	for _, a := range t.Attr {
		if err := checkSVGAttr(a); err != nil {
			return err
		}
	}
	return nil
}

func checkSVGAttr(a xml.Attr) error {
	name := strings.ToLower(a.Name.Local)
	value := strings.TrimSpace(a.Value)
	switch {
	case strings.HasPrefix(name, "on"), a.Name.Space == "http://www.w3.org/XML/1998/namespace" && name == "base":
		return errors.New("unsafe asset SVG attribute")
	case name == "href" && (!strings.HasPrefix(value, "#") || len(value) < 2):
		return errors.New("external asset SVG reference")
	case name == "style" && strings.ContainsAny(value, `\@`):
		return errors.New("unsupported inline asset style")
	case !localPaint(value):
		return errors.New("external asset SVG paint")
	default:
		return nil
	}
}

func localPaint(value string) bool {
	rest := strings.ToLower(value)
	for {
		_, ref, found := strings.Cut(rest, "url(")
		if !found {
			return true
		}
		ref, tail, ok := strings.Cut(ref, ")")
		ref = strings.Trim(strings.TrimSpace(ref), "`'\"")
		if !ok || !strings.HasPrefix(ref, "#") || len(ref) < 2 {
			return false
		}
		rest = tail
	}
}

func validPlane(e savedElement) bool {
	// Coordinates, rough parameters and freehand points are independent of the
	// raster dimensions. Validate them before calling geometry implementations.
	for _, v := range []float64{e.X, e.Y, e.Width, e.Height, e.Angle, e.StrokeWidth} {
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > 1e6 {
			return false
		}
	}
	if !validPaint(e) {
		return false
	}
	for _, p := range e.Points {
		if math.Abs(p[0]) > 1e6 || math.Abs(p[1]) > 1e6 {
			return false
		}
	}
	return true
}

func validPaint(e savedElement) bool {
	return e.Width >= 0 && e.Height >= 0 && e.StrokeWidth >= 0 && e.StrokeWidth <= 128 &&
		e.Roughness >= 0 && e.Roughness <= 2.5 && e.Opacity >= 0 && e.Opacity <= 100 &&
		paintColor.MatchString(e.StrokeColor) && paintColor.MatchString(e.BackgroundColor)
}

func roughWorkEstimate(e savedElement) float64 {
	if isEmbed(e) {
		e = embedShape(e)
	}
	if style := roughFillStyle(e); style == "" || style == "solid" {
		return 0
	}
	// A conservative count of hatch traversals independent of output scale;
	// do not let enormous scene coordinates allocate unbounded rough paths.
	gap := max(.1, e.StrokeWidth*4)
	sides := max(4, len(e.Points))
	return math.Ceil(math.Hypot(e.Width, e.Height)/gap) * float64(sides)
}
