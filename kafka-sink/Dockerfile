FROM rust:alpine AS builder
WORKDIR /app
RUN apk add --no-cache build-base cmake pkgconf openssl-dev cyrus-sasl-dev zlib-dev zlib-static
COPY . .
RUN cargo install --path .

FROM alpine:3.18
COPY --from=builder /usr/local/cargo/bin/kafka-sink /usr/local/bin/kafka-sink
CMD ["kafka-sink"]
