# syntax=docker/dockerfile:1.7
ARG BUILD_VERSION=dev
ARG BUILD_REVISION=unknown
FROM golang:1.24.2-bookworm@sha256:79390b5e5af9ee6e7b1173ee3eac7fadf6751a545297672916b59bfa0ecf6f71 AS build
ARG BUILD_VERSION
ARG BUILD_REVISION
WORKDIR /work/sipbridge
ENV GOFLAGS=-mod=readonly
COPY third_party/sipgo ./third_party/sipgo
COPY dependencies ./dependencies
RUN sha256sum --strict --check dependencies/sipgo.sha256 > /dev/null \
    && test "$(find third_party/sipgo -type f | wc -l)" -eq "$(wc -l < dependencies/sipgo.sha256)"
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY scripts/collect-licenses.sh ./scripts/collect-licenses.sh
RUN --mount=type=cache,target=/go/pkg/mod sh scripts/collect-licenses.sh /licenses
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X sipbridge/internal/buildinfo.Version=${BUILD_VERSION} -X sipbridge/internal/buildinfo.Revision=${BUILD_REVISION}" \
    -o /sipbridge ./cmd/sipbridge
FROM scratch
ARG BUILD_VERSION
ARG BUILD_REVISION
LABEL org.opencontainers.image.source="https://github.com/zyno-io/sipbridge" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.revision="${BUILD_REVISION}"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /sipbridge /sipbridge
COPY --from=build /licenses /licenses
COPY dependencies/control-schema.json /usr/share/sipbridge/control-schema.json
USER 65532:65532
EXPOSE 5060/udp 5060 5061 8443 8800
ENTRYPOINT ["/sipbridge"]
