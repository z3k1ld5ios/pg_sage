FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go build -o llm-mock .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/llm-mock .
EXPOSE 11434
CMD ["./llm-mock"]
