FROM golang:1.24.2-bookworm@sha256:79390b5e5af9ee6e7b1173ee3eac7fadf6751a545297672916b59bfa0ecf6f71 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY third_party/sipgo ./third_party/sipgo
RUN go mod download
COPY internal/controlpb ./internal/controlpb
COPY test/performance/controller ./test/performance/controller
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -trimpath -o /controller ./test/performance/controller
FROM scratch
COPY --from=build /controller /controller
USER 65532:65532
ENTRYPOINT ["/controller"]
