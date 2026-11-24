//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"image"
	"image/png"
)

func decode(body []byte) image.Image {
	pixels, err := png.Decode(bytes.NewReader(body))
	must(err)
	return pixels
}

func diff(name string, got, want image.Image) (fraction, mean float64) {
	rect := want.Bounds()
	if got.Bounds() != rect {
		panic(fmt.Sprintf("%s: bounds %v, want %v", name, got.Bounds(), rect))
	}
	var painted, pixels, total int
	for y := rect.Min.Y; y < rect.Max.Y; y++ {
		for x := rect.Min.X; x < rect.Max.X; x++ {
			gr, gg, gb, ga := got.At(x, y).RGBA()
			wr, wg, wb, wa := want.At(x, y).RGBA()
			if max(ga, wa) == 0 {
				continue
			}
			painted++
			r, g, b, a := delta(gr, wr), delta(gg, wg), delta(gb, wb), delta(ga, wa)
			total += r + g + b + a
			if max(r, g, b, a) >= 32 {
				pixels++
			}
		}
	}
	if painted == 0 {
		fmt.Printf("  %s: both images are transparent\n", name)
		return 0, 0
	}
	fraction, mean = float64(pixels)/float64(painted), float64(total)/float64(4*painted)
	fmt.Printf("  %s: %d/%d painted pixels differ by >=32; mean RGBA delta %.2f\n",
		name, pixels, painted, mean)
	return fraction, mean
}

func delta(a, b uint32) int {
	return int((max(a, b) - min(a, b)) >> 8)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
