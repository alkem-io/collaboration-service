# Whiteboard preview third-party notices

The unchanged browser-fork font files in the Go renderer are from
`alkem-io/excalidraw-yjs` 0.5.6 (source
`90b57f27faa35b570844c2f277e277cf450a7e73`). Aster's embedded fallback fonts
and separately bundled Noto Sans CJK JP retain their own licenses.
The notices here ship in the runtime image. Dependencies are pinned in
`go.mod`; dependency selection and font archive provenance are in
`specs/004-native-collab-previews/research.md`.

The renderer's pressure-stroke projection includes a port of portions of
Perfect Freehand's MIT-licensed geometry. See `Perfect-Freehand.txt`.
