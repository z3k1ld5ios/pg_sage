FROM golang:1.23-alpine AS builder
RUN apk add --no-cache git
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o sage-sidecar ./cmd/pg_sage_sidecar/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /build/sage-sidecar /usr/local/bin/sage-sidecar
EXPOSE 5433 9187
ENTRYPOINT ["sage-sidecar"]
