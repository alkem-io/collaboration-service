//go:build ignore

// Test-only: compare the exact same browser-exported SVG using three backends.
// This does not project a saved Go Y.Doc or qualify a production renderer.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/davidbyttow/govips/v2/vips"
	"github.com/mgilbir/aster"
)

type renderedFixture struct {
	Name, SVG   string
	PNG, SVGPng []byte
}

func main() {
	body, err := os.ReadFile(os.Args[1])
	must(err)
	var fixtures []renderedFixture
	must(json.Unmarshal(body, &fixtures))
	must(vips.Startup(&vips.Config{ConcurrencyLevel: 1, MaxCacheSize: 0, MaxCacheMem: 0, MaxCacheFiles: 0}))
	defer vips.Shutdown()
	engine, err := aster.New(aster.WithTimeout(3*time.Second), aster.WithMemoryLimit(64<<20))
	must(err)
	defer engine.Close()

	for _, f := range fixtures {
		fmt.Println(f.Name, "SVG bytes:", len(f.SVG))
		ref := decode(f.PNG)
		chrome := decode(f.SVGPng)
		diff("Chrome SVG vs editor Canvas", chrome, ref)
		a, err := engine.SVGToPNG(f.SVG)
		if err == nil {
			diff("Aster vs Chrome SVG", decode(a), chrome)
		} else {
			fmt.Println("Aster:", err)
		}
		v, err := vipsPNG([]byte(f.SVG))
		if err == nil {
			diff("govips vs Chrome SVG", decode(v), chrome)
		} else {
			fmt.Println("govips:", err)
		}
	}
}

func vipsPNG(svg []byte) ([]byte, error) {
	img, err := vips.NewImageFromBuffer(svg)
	if err != nil {
		return nil, err
	}
	defer img.Close()
	body, _, err := img.ExportPng(nil)
	return body, err
}
