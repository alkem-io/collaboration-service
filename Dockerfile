# syntax=docker/dockerfile:1.24

ARG GO_VERSION=1.26
ARG ALPINE_VERSION=3.24

# Build Stage — static CGO-free build (no native image libs; pure Go CRDT core)
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder

WORKDIR /app

RUN apk add --no-cache git

# Download modules first for layer caching
COPY go.mod go.sum ./
RUN go mod download

# Build
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /bin/collaboration-service ./cmd/server/

# Runtime Stage — distroless-equivalent minimal Alpine
FROM alpine:${ALPINE_VERSION}

RUN apk add --no-cache ca-certificates

# Non-root user matching K8s securityContext (fsGroup: 65532)
RUN addgroup -g 65532 -S nonroot && adduser -u 65532 -S -G nonroot nonroot

WORKDIR /app

COPY --from=builder /bin/collaboration-service /bin/collaboration-service

USER nonroot:nonroot

EXPOSE 4006

ENTRYPOINT ["/bin/collaboration-service"]
