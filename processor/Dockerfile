FROM rust:alpine AS builder
WORKDIR /app
RUN apk add --no-cache build-base cmake pkgconf openssl-dev cyrus-sasl-dev zlib-dev zlib-static
COPY . .
RUN cd processor && cargo install --path .

FROM alpine:3.18
COPY --from=builder /usr/local/cargo/bin/processor /usr/local/bin/processor
CMD ["processor"]
