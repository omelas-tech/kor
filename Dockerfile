FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kord ./cmd/kord \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kor ./cmd/kor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kord /usr/local/bin/kord
COPY --from=build /out/kor /usr/local/bin/kor
# Inside a container the default loopback bind is useless; compose/k8s set
# KORD_LISTEN=0.0.0.0:6565 explicitly.
EXPOSE 6565
ENTRYPOINT ["kord"]
