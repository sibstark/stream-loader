# syntax=docker/dockerfile:1

ARG GO_IMAGE=golang:1.27.0-alpine3.24
ARG PYTHON_IMAGE=python:3.14.7-slim-trixie

FROM ${GO_IMAGE} AS builder

WORKDIR /src

COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal

RUN go test ./...
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/streamrec ./cmd/streamrec

FROM ${PYTHON_IMAGE} AS runtime

ARG STREAMLINK_VERSION=8.6.0
ARG FFMPEG_VERSION=7:7.1.5-0+deb13u1

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates "ffmpeg=${FFMPEG_VERSION}" \
    && python -m pip install --no-cache-dir "streamlink==${STREAMLINK_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /app/recordings

COPY --from=builder /out/streamrec /usr/local/bin/streamrec

STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/streamrec"]
