# syntax=docker/dockerfile:1.24

# Build Stage — static CGO-free build (no native image libs; pure Go CRDT core)
FROM golang:1.26.10-alpine@sha256:27f2d3b83cfc5803c8078f8e730af0f7c431bf067afaec8f60a36fb763187cba AS builder

WORKDIR /app

RUN apk add --no-cache git

# Download modules first for layer caching
COPY go.mod go.sum ./
RUN go mod download

# Build
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /bin/collaboration-service ./cmd/server/

# Runtime Stage — distroless-equivalent minimal Alpine
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

RUN apk add --no-cache ca-certificates

# Non-root user matching K8s securityContext (fsGroup: 65532)
RUN addgroup -g 65532 -S nonroot && adduser -u 65532 -S -G nonroot nonroot

WORKDIR /app

COPY --from=builder /bin/collaboration-service /bin/collaboration-service

USER nonroot:nonroot

EXPOSE 4006

ENTRYPOINT ["/bin/collaboration-service"]
