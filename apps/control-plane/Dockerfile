FROM golang:1.25-bookworm AS build

WORKDIR /src

COPY apps/control-plane/go.mod apps/control-plane/go.sum ./
RUN go mod download

COPY apps/control-plane/ ./

RUN go build -o /out/server ./cmd/server
RUN go build -o /out/temporal-worker ./cmd/temporal-worker

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /out/server /usr/local/bin/server
COPY --from=build /out/temporal-worker /usr/local/bin/temporal-worker
COPY docs/api/openapi.yaml /app/docs/api/openapi.yaml

EXPOSE 8080

CMD ["/usr/local/bin/server"]
