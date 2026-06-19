.PHONY: run build test vet fmt bench pg pg-stop up

run:
	go run ./cmd/duraflow

build:
	go build -o bin/duraflow ./cmd/duraflow

test:
	go test ./... -race -count=1

vet:
	go vet ./...

fmt:
	gofmt -w .

bench:
	go run ./cmd/bench

pg:
	docker run -d --rm --name df-pg -e POSTGRES_PASSWORD=duraflow -e POSTGRES_USER=duraflow -e POSTGRES_DB=duraflow -p 55432:5432 postgres:16

pg-stop:
	docker rm -f df-pg

up:
	docker compose up --build
