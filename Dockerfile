FROM golang:1.26 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pulse-server ./cmd/pulse-server

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/pulse-server /app/pulse-server
COPY examples/codespaces.yaml /app/config.yaml

EXPOSE 9090
VOLUME ["/data"]

ENTRYPOINT ["/app/pulse-server", "--config", "/app/config.yaml"]

