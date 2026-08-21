.PHONY: build docker test e2e integration coverage-gate lint vet openapi setup-hooks run clean

BINARY := collaboration-service
GO := go
GOFLAGS := -race

build:
	mkdir -p bin/
	$(GO) build -o bin/$(BINARY) ./cmd/server/

docker:
	docker build -t alkemio/collaboration-service:latest .

test:
	$(GO) test $(GOFLAGS) -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

# Build-tagged e2e suite (full stack through internal/app over real WebSockets +
# the yjs/y-protocols JS-interop harness). Hermetic — no external backends (the
# two-pod test uses in-process miniredis). Requires the harness deps:
#   ( cd test/e2e/jsinterop && npm ci )
e2e:
	$(GO) test $(GOFLAGS) -tags e2e ./test/e2e/...

# Build-tagged integration suites (redis/rabbitmq + the app.New durable wiring)
# against live backends — set RABBITMQ_TEST_URL (unset ⇒ those tests skip).
integration:
	$(GO) test $(GOFLAGS) -tags integration ./...

# Combined ≥95% coverage gate (unit + integration + e2e, merged + scoped), as CI
# runs it. Pass a different threshold as the first arg if needed.
coverage-gate:
	./.scripts/coverage-gate.sh

lint:
	golangci-lint run

vet:
	$(GO) vet ./...

# Regenerate the OpenAPI spec from the chi router + handler Render methods. The
# standalone create endpoint (POST /collab/{documentId}) and /healthz are the
# documented surface; the live collaboration protocol is the WebSocket contract
# (specs/.../ws-protocol.md), out of scope for OpenAPI.
openapi:
	apispec --dir . --output openapi.yaml --config apispec.yaml

setup-hooks:
	git config core.hooksPath .githooks
	@echo "Git hooks configured"

run:
	$(GO) run ./cmd/server/

clean:
	rm -rf bin/ coverage.out
