package whiteboardrender

import (
	"math"
	"strings"
	"sync"

	"golang.org/x/image/font"
	"golang.org/x/image/font/sfnt"
	"golang.org/x/image/math/fixed"
)

var captionFontOnce sync.Once
var previewCaptionFont *sfnt.Font

type captionMeasure struct {
	buf  sfnt.Buffer
	ppem fixed.Int26_6
}

func newCaptionMeasure(size float64) *captionMeasure {
	captionFontOnce.Do(func() {
		data, err := fontAssets.readFile("fonts/Liberation/LiberationSans-Regular.ttf")
		must(err)
		previewCaptionFont, err = sfnt.Parse(data)
		must(err)
	})
	return &captionMeasure{ppem: fixed.Int26_6(math.Round(size * 64))}
}

func (m *captionMeasure) glyph(r rune) sfnt.GlyphIndex {
	g, err := previewCaptionFont.GlyphIndex(&m.buf, r)
	must(err)
	return g
}

func (m *captionMeasure) kern(previous, glyph sfnt.GlyphIndex) int64 {
	if previous != 0 {
		if k, err := previewCaptionFont.Kern(&m.buf, previous, glyph, m.ppem, font.HintingNone); err == nil {
			return int64(k)
		}
	}
	return 0
}

func (m *captionMeasure) advance(previous, glyph sfnt.GlyphIndex) int64 {
	a, err := previewCaptionFont.GlyphAdvance(&m.buf, glyph, m.ppem, font.HintingNone)
	must(err)
	return int64(a) + m.kern(previous, glyph)
}

func (m *captionMeasure) width(s string) int64 {
	var previous sfnt.GlyphIndex
	var width int64
	for _, r := range s {
		glyph := m.glyph(r)
		width += m.advance(previous, glyph)
		previous = glyph
	}
	return width
}

// Only exporter-owned captions are measured here, never saved whiteboard text.
func captionWidth(s string, size float64) float64 {
	return float64(newCaptionMeasure(size).width(s)) / 64
}

func clipFrameTitle(s string, size, maxWidth float64) string {
	const suffix = "..."
	m := newCaptionMeasure(size)
	limit := maxWidth * 64
	ellipsis, dot := m.width(suffix), m.glyph('.')
	var previous sfnt.GlyphIndex
	var width int64
	end := 0
	for i, r := range s {
		if i > 0 && float64(width+m.kern(previous, dot)+ellipsis) <= limit {
			end = i
		}
		glyph := m.glyph(r)
		width += m.advance(previous, glyph)
		previous = glyph
	}
	if float64(width) <= limit {
		return s
	}
	if end > 0 || float64(ellipsis) <= limit {
		return s[:end] + suffix
	}
	return ""
}

func wrapEmbedCaption(s string, size, maxWidth float64) string {
	var out strings.Builder
	var line []rune
	for _, r := range s {
		line = append(line, r)
		if len(line) < 2 || captionWidth(string(line), size) <= maxWidth {
			continue
		}
		lastBreak := len(line) - 1
		for i := lastBreak - 1; i > 0; i-- {
			if strings.ContainsRune(" -/", line[i]) {
				lastBreak = i + 1
				break
			}
		}
		out.WriteString(string(line[:lastBreak]))
		out.WriteByte('\n')
		line = line[lastBreak:]
	}
	out.WriteString(string(line))
	return out.String()
}
