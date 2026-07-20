.PHONY: test race bench lint fmt vet cover tidy check

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -run='^$$' -bench=. -benchmem ./...

lint:
	golangci-lint run

fmt:
	gofmt -w .

vet:
	go vet ./...

cover:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out | tail -1

tidy:
	go mod tidy

# Run everything CI runs.
check: fmt vet race lint
