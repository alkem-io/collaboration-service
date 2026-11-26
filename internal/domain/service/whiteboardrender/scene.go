package whiteboardrender

import (
	"cmp"
	"encoding/json"
	"slices"

	yjs "github.com/antst/go-yjs/crdt"
	rough "github.com/d2lang/rough-go"
)

type savedElement struct {
	ID, Index, Type, FileID, StrokeColor, BackgroundColor, StrokeStyle, FillStyle string
	X, Y, Width, Height, Angle, Seed, StrokeWidth, Roughness, Opacity             float64
	Text, TextAlign, Link                                                         string
	FrameID, ContainerID, Name, EndArrowhead, StartArrowhead                      string
	Points                                                                        []rough.Point
	Pressures                                                                     []float64
	GroupIds                                                                      []string
	Scale                                                                         [2]float64
	Crop                                                                          *imageCrop
	fontCSS                                                                       string
	SimulatePressure, Elbowed                                                     bool
	FontFamily                                                                    int
	FontSize, LineHeight                                                          float64
	IsDeleted                                                                     bool
	Roundness                                                                     *roundness
}

type roundness struct {
	Type  int
	Value *float64
}

// Browser-family numbers and hhea metrics from bundledFontFaces.ts and
// font-metadata.ts. Helvetica is local-only in the oracle; previews use the
// bundled deterministic equivalent, Liberation Sans.
type face struct {
	name, file string
	vertical   float64
}

var fontFaces = map[int]face{
	1:  {"Virgil", "Virgil/Virgil-Regular.woff2", .512},
	2:  {"Liberation Sans", "", 1106.0 / 2048},
	3:  {"Cascadia Code", "Cascadia/CascadiaCode-Regular.woff2", 1420.0 / 2048},
	5:  {"Excalifont", "Excalifont/Excalifont-Regular-a88b72a24fb54c9f94e3b5fdaa7481c9.woff2", .512},
	6:  {"Nunito", "Nunito/Nunito-Regular-XRXI3I6Li01BKofiOc5wtlZ2di8HDIkhdTQ3j6zbXWjgeg.woff2", .658},
	7:  {"Lilita One", "Lilita/Lilita-Regular-i7dPIFZ9Zz-WBtRtedDbYEF8RXi4EwQ.woff2", .703},
	8:  {"Comic Shanns", "ComicShanns/ComicShanns-Regular-279a7b317d12eb88de06167bd672b4b4.woff2", .5},
	9:  {"Liberation Sans", "", 1420.0 / 2048},
	10: {"Assistant", "Assistant/Assistant-Regular.woff2", 734.0 / 2048},
}

func (r *roundness) radius(size float64) float64 {
	switch r.Type {
	case 1, 2:
		return size / 4
	case 3:
		if r.Value != nil {
			return min(size/4, *r.Value)
		}
		return min(size/4, 32)
	default:
		panic("unknown saved roundness")
	}
}

type imageCrop struct {
	X, Y, Width, Height, NaturalWidth, NaturalHeight float64
}

func savedElements(doc *yjs.Doc) []savedElement {
	var out []savedElement
	scene := doc.GetMap("elements")
	if scene.GetSize() > 2600 {
		panic("saved scene exceeds the 2,600-element bound")
	}
	pointCount := 0
	roughWork := 0.0
	for _, val := range scene.Entries() {
		node, ok := val.(*yjs.YMap)
		if !ok {
			panic("expected nested saved element map")
		}
		jsonNode, err := json.Marshal(node.ToJSON())
		must(err)
		var e savedElement
		must(json.Unmarshal(jsonNode, &e))
		if !e.IsDeleted {
			if e.Index == "" || !validPlane(e) {
				panic("invalid saved geometry or index")
			}
			pointCount += len(e.Points) + len(e.Pressures)
			if pointCount > 160000 {
				panic("saved scene exceeds the points/pressure bound")
			}
			roughWork += roughWorkEstimate(e)
			if roughWork > 600000 {
				panic("saved scene exceeds the pattern work bound")
			}
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b savedElement) int {
		if order := cmp.Compare(a.Index, b.Index); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return out
}
