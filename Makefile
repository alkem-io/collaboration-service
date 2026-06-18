.PHONY: build docker test lint vet openapi setup-hooks run clean

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

lint:
	golangci-lint run

vet:
	$(GO) vet ./...

# Regenerate the OpenAPI spec from the chi router + handler Render methods. The
# standalone create/delete REST API (POST/DELETE /collab/{documentId}, T016) and
# /healthz are the documented surface; the live collaboration protocol is the
# WebSocket contract (specs/.../ws-protocol.md), out of scope for OpenAPI.
openapi:
	apispec --dir . --output openapi.yaml --config apispec.yaml

setup-hooks:
	git config core.hooksPath .githooks
	@echo "Git hooks configured"

run:
	$(GO) run ./cmd/server/

clean:
	rm -rf bin/ coverage.out
