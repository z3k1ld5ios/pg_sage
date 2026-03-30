FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go build -o webhook-mock .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/webhook-mock .
EXPOSE 9999
CMD ["./webhook-mock"]
