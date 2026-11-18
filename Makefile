.PHONY: web relay-web build test clean

web:
	cd web && npm run build

relay-web:
	cd relay-web && npm run build

test: web relay-web
	go test -race ./...

build: web relay-web
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o dist/gens-daemon.exe ./cmd/gens-daemon
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o dist/gens-daemon-linux-amd64 ./cmd/gens-daemon
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o dist/relay-server-linux-amd64 ./cmd/relay-server
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o dist/relay-server.exe ./cmd/relay-server

clean:
	rm -rf dist web/dist relay-web/dist
