# Whiteboard renderer qualification probe

This probe is test-only and intentionally outside the service's dependency
graph. It starts with saved assets and expands to actual saved scenes. It checks a
center pixel against the exact published fork 0.5.6 in headless Chrome. The four
fixtures are local, inert SVG; Chrome is not allowed to request another origin.
The same test does not attempt to establish visual tolerances for shape edges.

In a disposable scratch directory, with `oracle` set to this directory:

```sh
npm init -y
npm install --no-save --package-lock=false @excalidraw-yjs/excalidraw@0.5.6 \
  @excalidraw-yjs/element@0.5.6 yjs@13.6.27 react@19 react-dom@19 \
  playwright-core@1.56.1 vite@7.3.1 perfect-freehand@1.2.0
CHROME_BIN=/path/to/chrome node "$oracle/image.cjs"

go mod init example.invalid/collab-preview-svg-probe
go get github.com/tdewolff/canvas@v0.0.0-20261118111642-891200a8cf2b \
  github.com/srwiley/oksvg@v0.0.0-20221011165216-be6e8873101c
CGO_ENABLED=0 go run "$oracle/svg.go" "$oracle/images.json"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o svg-proof-linux "$oracle/svg.go"
```

Expected with these candidate pins: both native decoders pass the plain control,
but both incorrectly paint the local-referenced green pattern black and paint
through the local mask. Canvas also renders the `<use>` case as fully transparent.
A clean process exit is not a parity verdict: inspect each labeled comparison.

In a separate disposable Go module, the Aster converter checks the same fixtures,
external-image refusal (loopback only), and cancellation. It runs under the
service's Go 1.26.10 with CGO off:

```sh
go mod init example.invalid/collab-preview-aster
go get github.com/mgilbir/aster@v0.2.0
CGO_ENABLED=0 go run "$oracle/aster.go" "$oracle/images.json"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o aster-proof-linux "$oracle/aster.go"
```

That slice passed. None of these probes modifies the root Go module or
establishes full whiteboard parity.

## Render an actual saved Yjs fixture

From the same disposable npm directory and with unused paths under it:

```sh
COLLAB_PREVIEW_SAVED_ORACLE="$PWD/shapes.json" \
COLLAB_PREVIEW_SAVED_TEXT_ORACLE="$PWD/text.json" \
COLLAB_PREVIEW_SAVED_GRID_ORACLE="$PWD/grid.json" \
COLLAB_PREVIEW_SAVED_FRAME_ORACLE="$PWD/frame.json" \
COLLAB_PREVIEW_SAVED_ARROW_ORACLE="$PWD/arrow.json" \
COLLAB_PREVIEW_SAVED_CURVES_ORACLE="$PWD/curves.json" \
COLLAB_PREVIEW_SAVED_FRAME_GROUP_ORACLE="$PWD/frame-group.json" \
COLLAB_PREVIEW_SAVED_HEADS_ORACLE="$PWD/heads.json" \
COLLAB_PREVIEW_SAVED_EMPTY_ORACLE="$PWD/empty.json" \
COLLAB_PREVIEW_SAVED_PRESSURE_ORACLE="$PWD/pressure.json" \
COLLAB_PREVIEW_SAVED_SCRIPTS_ORACLE="$PWD/scripts.json" \
COLLAB_PREVIEW_SAVED_FONTS_ORACLE="$PWD/fonts.json" \
COLLAB_PREVIEW_SAVED_CJK_ORACLE="$PWD/cjk.json" \
COLLAB_PREVIEW_SAVED_TIED_ORACLE="$PWD/tied.json" \
COLLAB_PREVIEW_SAVED_MISSING_INDEX_ORACLE="$PWD/missing-index.json" \
COLLAB_PREVIEW_SAVED_CROP_ORACLE="$PWD/crop.json" \
COLLAB_PREVIEW_SAVED_CODECS_ORACLE="$PWD/codecs.json" \
COLLAB_PREVIEW_SAVED_ROUNDED_ORACLE="$PWD/rounded.json" \
COLLAB_PREVIEW_SAVED_EMBEDS_ORACLE="$PWD/embeds.json" \
COLLAB_PREVIEW_SAVED_CAPTIONS_ORACLE="$PWD/captions.json" \
COLLAB_PREVIEW_SAVED_ASSET_BURST_ORACLE="$PWD/asset-burst.json" \
COLLAB_PREVIEW_SAVED_SHAPE_BURST_ORACLE="$PWD/shape-burst.json" \
COLLAB_PREVIEW_FONTS_DIR="$editor_src/packages/excalidraw/fonts" \
COLLAB_PREVIEW_CODEC_BMP="$bmp_fixture" \
COLLAB_PREVIEW_CODEC_AVIF="$avif_fixture" \
CHROME_BIN=/path/to/chrome node "$oracle/image.cjs"
```

Set `editor_src` to the pinned 0.5.6 source checkout from research R1: the
Xiaolai coverage ranges are in that checkout, not the published asset directory.
For the two local codec inputs use opaque 64×48 solid RGB(32,192,64) fixtures
in BMP and AVIF. Omit those two environment variables to run the base codec
matrix without those formats.

From collaboration-service, run the production Go renderer against those
fresh outputs. Point `fixture_dir` to a directory containing only the saved
scene JSONs produced above; do not include individual SVG or mixed-render
probes. With no environment override, the test uses the checked-in compressed
synthetic corpus (22 cases, excluding the two large stress cases).

```sh
COLLAB_PREVIEW_ORACLE_DIR="$fixture_dir" CGO_ENABLED=0 \
  go test ./internal/domain/service/whiteboardrender -run '^TestPackagedRendererMatchesSavedBrowserCorpus$' -v
```

The optional Noto fallback in the CJK proof accepts only the SHA-256-pinned
Japanese Regular OTF from upstream [Sans2.004](https://github.com/notofonts/noto-cjk/releases/tag/Sans2.004).
Without it the common characters render, but the rare U+20BB7 sample is a
missing glyph. Do not use an arbitrary host font as a replacement.

The production comparison uses the 1×-then-resampled Canvas oracle for
ordinary cases and the direct target-scale bitmap for dense scenes. Neither
pixel count alone is a semantic verdict. The qualification additionally
checked frame/clipping and image crop interior colors. The packaged pressure
fixture pins each outline point to the browser's perfect-freehand output.

## Compare SVG rasterizers without attributing editor differences to them

Set `COLLAB_PREVIEW_MIXED_ORACLE="$PWD/mixed.json"` on the Chromium invocation to
capture the editor's separately generated SVG and Canvas images, plus Chromium's
rasterization of the exact SVG. In a disposable Go module with libvips and its
`pkg-config` headers installed:

```sh
go get github.com/mgilbir/aster@v0.2.0 github.com/davidbyttow/govips/v2/vips@v2.19.0
CGO_ENABLED=1 go run "$oracle/mixed.go" "$oracle/mixed_pixels.go" "$fixture_dir/mixed.json"
```

This last comparison intentionally uses CGO. It is not the production path.
