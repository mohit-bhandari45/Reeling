FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /reeling ./cmd/server

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates xz-utils curl \
    && curl --retry 5 --retry-delay 10 --retry-all-errors -L \
       https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz -o /tmp/ffmpeg.tar.xz \
    && tar -xf /tmp/ffmpeg.tar.xz -C /tmp \
    && mv /tmp/ffmpeg-*-amd64-static/ffmpeg /usr/local/bin/ffmpeg \
    && mv /tmp/ffmpeg-*-amd64-static/ffprobe /usr/local/bin/ffprobe \
    && rm -rf /tmp/ffmpeg* \
    && apt-get remove -y xz-utils curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY --from=builder /reeling /reeling

EXPOSE 8080

CMD ["/reeling"]