module github.com/alkem-io/collaboration-service

go 1.26

require (
	github.com/coder/websocket v1.8.15
	github.com/go-chi/chi/v5 v5.3.0
	github.com/google/uuid v1.6.0
	github.com/prometheus/client_golang v1.23.2
	github.com/skyterra/y-crdt v0.0.0-20260618095206-a2c966d82c1a
	go.uber.org/zap v1.28.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/sys v0.35.0 // indirect
	google.golang.org/protobuf v1.36.8 // indirect
)

// The CRDT core is the Alkemio fork of skyterra/y-crdt. The fork keeps the
// upstream module path (github.com/skyterra/y-crdt) so this replace is valid;
// it pins the v2-encoding-and-sync-protocol branch at commit a2c966d (the
// commit whose cross-impl fuzz gate is green — WS-A of 003-unify-collab-yjs).
replace github.com/skyterra/y-crdt => github.com/antst/y-crdt v0.0.0-20260618095206-a2c966d82c1a
