FROM golang:1.26 AS builder

ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/Yasser-Ameur/pulse/internal/server.Version=${VERSION}" -o /out/pulse-server ./cmd/pulse-server
# The runtime image runs as nonroot (uid 65532) and cannot create /data itself,
# so the directory is created here and copied in with the right owner.
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/pulse-server /app/pulse-server
COPY examples/codespaces.yaml /app/config.yaml
COPY --from=builder --chown=65532:65532 /out/data /data

EXPOSE 9090
EXPOSE 9091
VOLUME ["/data"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/pulse-server", "healthcheck", "--config", "/app/config.yaml"]

ENTRYPOINT ["/app/pulse-server", "--config", "/app/config.yaml"]

