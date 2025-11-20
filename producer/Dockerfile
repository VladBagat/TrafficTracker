FROM rust:alpine AS builder
WORKDIR /producer
# Install C build toolchain and library dependencies
#RUN apk add --no-cache build-base cmake pkgconfig openssl-dev cyrus-sasl-dev zlib-dev
RUN apk add --no-cache build-base cmake pkgconf openssl-dev cyrus-sasl-dev zlib-dev zlib-static
COPY . .
RUN cargo install --path .

FROM alpine:3.18
COPY --from=builder /usr/local/cargo/bin/producer /usr/local/bin/producer
CMD ["producer"]