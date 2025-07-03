.PHONY: build run test clean deps lint help

# Build the application
build:
	go build -o bin/binance-data-fetcher main.go

# Run the application
run:
	go run main.go

# Install dependencies
deps:
	go mod tidy
	go mod download

# Run tests
test:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f coverage.out coverage.html

# Run linter
lint:
	golangci-lint run

# Install linter (if not installed)
install-lint:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Format code
fmt:
	go fmt ./...

# Vet code
vet:
	go vet ./...

# Create binary for different platforms
build-linux:
	GOOS=linux GOARCH=amd64 go build -o bin/binance-data-fetcher-linux main.go

build-windows:
	GOOS=windows GOARCH=amd64 go build -o bin/binance-data-fetcher-windows.exe main.go

build-darwin:
	GOOS=darwin GOARCH=amd64 go build -o bin/binance-data-fetcher-darwin main.go

# Build all platforms
build-all: build-linux build-windows build-darwin

# Show help
help:
	@echo "Available commands:"
	@echo "  build        - Build the application"
	@echo "  run          - Run the application"
	@echo "  deps         - Install dependencies"
	@echo "  test         - Run tests"
	@echo "  test-coverage- Run tests with coverage"
	@echo "  clean        - Clean build artifacts"
	@echo "  lint         - Run linter"
	@echo "  install-lint - Install linter"
	@echo "  fmt          - Format code"
	@echo "  vet          - Vet code"
	@echo "  build-linux  - Build for Linux"
	@echo "  build-windows- Build for Windows"
	@echo "  build-darwin - Build for macOS"
	@echo "  build-all    - Build for all platforms"
	@echo "  help         - Show this help" 