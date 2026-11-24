package whiteboardrender

import (
	"archive/zip"
	"bytes"
	_ "embed"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/mgilbir/aster"
)

//go:embed fonts.zip
var fontZIP []byte

//go:embed xiaolai.tsv
var xiaolaiManifest string

var openFontPack = sync.OnceValues(func() (*zip.Reader, error) {
	return zip.NewReader(bytes.NewReader(fontZIP), int64(len(fontZIP)))
})

type packedFonts struct{}

var fontAssets packedFonts

func (packedFonts) readFile(name string) ([]byte, error) {
	pack, err := openFontPack()
	if err != nil {
		return nil, err
	}
	return fs.ReadFile(pack, name)
}

func fontOptions(items []savedElement) []aster.Option {
	pack, err := openFontPack()
	must(err)
	var opts []aster.Option
	used := make(map[int]string)
	chars := make(map[rune]bool)
	register := func(alias, name string) {
		b, err := fontAssets.readFile("fonts/" + name)
		must(err)
		opts = append(opts, aster.WithFont(alias, b))
	}
	for i := range items {
		e := &items[i]
		caption := e.Text
		switch {
		case e.Type == "text":
			if css, ok := used[e.FontFamily]; ok {
				e.fontCSS = css
			} else {
				face, ok := fontFaces[e.FontFamily]
				if !ok {
					panic("unknown saved font family")
				}
				css := faceCSS(pack, face, register)
				e.fontCSS, used[e.FontFamily] = css, css
			}
		case isFrame(*e):
			caption = e.Name
		default:
			continue
		}
		for _, r := range caption {
			if unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul) {
				chars[r] = true
			}
		}
	}
	var fallback []string
	if len(chars) != 0 {
		for i, row := range strings.Split(strings.TrimSpace(xiaolaiManifest), "\n") {
			name, intervals, ok := strings.Cut(row, "\t")
			if !ok {
				panic("invalid pinned font manifest")
			}
			if coversAny(intervals, chars) {
				if len(fallback) >= 40 {
					panic("saved scene font work exceeds preview bound")
				}
				alias := fmt.Sprintf("Xiaolai %d", i)
				register(alias, name)
				fallback = append(fallback, alias)
			}
		}
		register("Noto Sans CJK JP", "Noto/NotoSansCJKjp-Regular.otf")
		fallback = append(fallback, "Noto Sans CJK JP")
	}
	if len(fallback) > 0 {
		fallback = slices.Concat(fallback, []string{"Liberation Sans", "Noto Emoji"})
		opts = append(opts, aster.WithFontFallback(fallback...))
	}
	return opts
}

func faceCSS(pack *zip.Reader, face face, register func(string, string)) string {
	css := face.name
	if face.file == "" {
		return css
	}
	register(face.name, face.file)
	known, err := fs.Glob(pack, "fonts/"+path.Dir(face.file)+"/*.woff2")
	must(err)
	for i, entry := range known {
		name := strings.TrimPrefix(entry, "fonts/")
		if name == face.file || strings.Contains(name, "Assistant-") {
			continue
		}
		alias := fmt.Sprintf("%s subset %d", face.name, i)
		register(alias, name)
		css += ", " + alias
	}
	return css
}

func coversAny(intervals string, chars map[rune]bool) bool {
	for _, span := range strings.Split(intervals, ",") {
		lo, hi, multiple := strings.Cut(strings.TrimSpace(span), "-")
		a, err := strconv.ParseInt(strings.TrimPrefix(lo, "U+"), 16, 32)
		must(err)
		b := a
		if multiple {
			b, err = strconv.ParseInt(hi, 16, 32)
			must(err)
		}
		for ch := range chars {
			if int64(ch) >= a && int64(ch) <= b {
				return true
			}
		}
	}
	return false
}
