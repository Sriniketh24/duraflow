# Stage 1: Build
FROM golang:1.26 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/duraflow ./cmd/duraflow

# Stage 2: Runtime
FROM gcr.io/distroless/static-debian12

COPY --from=build /out/duraflow /duraflow

EXPOSE 8080
ENTRYPOINT ["/duraflow"]
