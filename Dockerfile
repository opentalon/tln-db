# syntax=docker/dockerfile:1

# Build the talondb-server binary. bbolt and all deps are pure Go, so a
# static CGO-free build works and lets us ship a distroless image.
FROM golang:1.25-alpine AS builder
WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/talondb-server ./cmd/talondb-server

# Minimal, non-root runtime.
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/talondb-server /talondb-server
# gRPC (9899), HTTP/JSON (8080), Prometheus metrics (9090).
EXPOSE 9899 8080 9090
USER 65532:65532
ENTRYPOINT ["/talondb-server"]
