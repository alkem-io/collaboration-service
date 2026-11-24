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

// This is explicitly for exporter-owned captions. Saved whiteboard text is
// never wrapped or truncated by this renderer.
func captionWidth(s string, size float64) float64 {
	captionFontOnce.Do(func() {
		data, err := fontAssets.readFile("fonts/Liberation/LiberationSans-Regular.ttf")
		must(err)
		previewCaptionFont, err = sfnt.Parse(data)
		must(err)
	})
	var buf sfnt.Buffer
	var previous sfnt.GlyphIndex
	var width fixed.Int26_6
	ppem := fixed.Int26_6(math.Round(size * 64))
	for _, r := range s {
		glyph, err := previewCaptionFont.GlyphIndex(&buf, r)
		must(err)
		advance, err := previewCaptionFont.GlyphAdvance(&buf, glyph, ppem, font.HintingNone)
		must(err)
		if previous != 0 {
			if kern, err := previewCaptionFont.Kern(&buf, previous, glyph, ppem, font.HintingNone); err == nil {
				width += kern
			}
		}
		width += advance
		previous = glyph
	}
	return float64(width) / 64
}

func clipFrameTitle(s string, size, maxWidth float64) string {
	if captionWidth(s, size) <= maxWidth {
		return s
	}
	rs := []rune(s)
	for i := len(rs) - 1; i > 0; i-- {
		candidate := string(rs[:i]) + "..."
		if captionWidth(candidate, size) <= maxWidth {
			return candidate
		}
	}
	return s
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
