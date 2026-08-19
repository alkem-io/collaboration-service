package model

import "errors"

// ErrNotFound is returned by MetadataStore.Load and CheckpointStore.LoadCheckpoint when no row
// or blob exists for the given id/pointer. It is part of the port contract:
// adapters MUST wrap their backend's not-found error with this sentinel so the
// domain can branch on errors.Is(err, model.ErrNotFound) without knowing the
// backend.
var ErrNotFound = errors.New("not found")
