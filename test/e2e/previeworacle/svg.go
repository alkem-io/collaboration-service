//go:build ignore

// This is a qualification probe, not service code. Run it in the scratch
// module containing the pinned canvas candidate, leaving production go.mod alone.
package main

import (
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"os"
	"strings"

	"github.com/srwiley/oksvg"
	"github.com/srwiley/rasterx"
	"github.com/tdewolff/canvas"
	"github.com/tdewolff/canvas/renderers/rasterizer"
)

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var fixtures []struct {
		Name, Paint string
		Center      [4]uint8
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		panic(err)
	}
	for _, f := range fixtures {
		source := `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="48" viewBox="0 0 64 48">` + f.Paint + "</svg>"
		want := color.RGBA{R: f.Center[0], G: f.Center[1], B: f.Center[2], A: f.Center[3]}
		compare("canvas", f.Name, source, want, canvasImage)
		compare("oksvg", f.Name, source, want, oksvgImage)
	}
}

func canvasImage(source string) (image.Image, error) {
	c, err := canvas.ParseSVG(strings.NewReader(source))
	if err != nil {
		return nil, err
	}
	return rasterizer.Draw(c, canvas.DPMM(1), canvas.DefaultColorSpace), nil
}

func oksvgImage(source string) (image.Image, error) {
	icon, err := oksvg.ReadIconStream(strings.NewReader(source), oksvg.StrictErrorMode)
	if err != nil {
		return nil, err
	}
	im := image.NewRGBA(image.Rect(0, 0, 64, 48))
	icon.SetTarget(0, 0, 64, 48)
	scanner := rasterx.NewScannerGV(64, 48, im, im.Bounds())
	icon.Draw(rasterx.NewDasher(64, 48, scanner), 1)
	return im, nil
}

func compare(engine, name, source string, want color.RGBA, render func(string) (image.Image, error)) {
	im, err := render(source)
	if err != nil {
		fmt.Printf("%s %s: decoder refused asset: %v\n", engine, name, err)
		return
	}
	center := im.At(im.Bounds().Dx()/2, im.Bounds().Dy()/2)
	fmt.Printf("%s %s: %v center %v, matches browser: %t\n", engine, name, im.Bounds(), center, center == want)
}
