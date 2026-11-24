//go:build ignore

// Test-only probe for a newer Go candidate. Run in an isolated module: the
// collaboration-service toolchain and dependency graph are not changed.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"

	"github.com/mgilbir/aster"
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
	converter, err := aster.New(aster.WithTimeout(2*time.Second), aster.WithMemoryLimit(32<<20))
	if err != nil {
		panic(err)
	}
	defer converter.Close()
	for _, f := range fixtures {
		source := `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="48" viewBox="0 0 64 48">` + f.Paint + "</svg>"
		data, err := converter.SVGToPNG(source)
		if err != nil {
			panic(err)
		}
		im, err := png.Decode(bytes.NewReader(data))
		if err != nil {
			panic(err)
		}
		point := color.NRGBAModel.Convert(im.At(im.Bounds().Dx()/2, im.Bounds().Dy()/2))
		want := color.NRGBA{R: f.Center[0], G: f.Center[1], B: f.Center[2], A: f.Center[3]}
		if point != want {
			panic(fmt.Sprintf("%s: center %v, want %v", f.Name, point, want))
		}
		fmt.Printf("%s: %v center %v, matches browser\n", f.Name, im.Bounds(), point)
	}
	checkRestrictedReads(converter)
}

func checkRestrictedReads(converter *aster.Converter) {
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()
	external := `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="48"><image width="64" height="48" href="` + upstream.URL + `/image.png"/></svg>`
	if result, err := converter.SVGToPNG(external); err == nil || len(result) != 0 || requests.Load() != 0 {
		panic(fmt.Sprintf("external SVG image did not fail closed: bytes=%d err=%v requests=%d", len(result), err, requests.Load()))
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	blank := `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="48"/>`
	if result, err := converter.SVGToPNG(blank, aster.WithContext(cancelled)); !errors.Is(err, context.Canceled) || len(result) != 0 {
		panic(fmt.Sprintf("canceled render returned bytes=%d err=%v", len(result), err))
	}
	fmt.Println("external image denied without a network request; canceled render returned no image")
}
