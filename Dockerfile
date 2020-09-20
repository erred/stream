FROM golang:alpine AS build

WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /bin/stream-server ./cmd/stream-server

FROM scratch

COPY --from=build /bin/stream-server /bin/

ENTRYPOINT ["/bin/stream-server"]
