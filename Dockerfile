FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /broker ./cmd/broker
RUN CGO_ENABLED=0 go build -o /manager ./cmd/manager
RUN CGO_ENABLED=0 go build -o /publisher ./cmd/publisher
RUN CGO_ENABLED=0 go build -o /subscriber ./cmd/subscriber
RUN CGO_ENABLED=0 go build -o /demo ./cmd/demo

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /broker /manager /publisher /subscriber /demo /app/
RUN mkdir -p /data/broker /data/manager
EXPOSE 8080 8081
CMD ["sh"]
