FROM rust:1.91-alpine as builder
WORKDIR /producer
# Install C build toolchain and library dependencies
RUN apk add --no-cache build-base cmake pkgconfig openssl-dev cyrus-sasl-dev zlib-dev
COPY . .
RUN cargo install --path .

FROM alpine:3.18
#RUN apt-get update && apt-get install -y extra-runtime-dependencies && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/producer /usr/local/bin/producer
CMD ["producer"]