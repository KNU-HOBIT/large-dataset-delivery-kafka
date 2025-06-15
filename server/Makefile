# 변수 정의
BINARY_NAME=large-dataset-server
DOCKER_IMAGE=large-dataset-server
DOCKER_TAG=latest
BUILD_DIR=build
CONFIG_PATH=configs/development.yaml

# Go 관련 변수
GOPATH=$(shell go env GOPATH)
GOOS=$(shell go env GOOS)
GOARCH=$(shell go env GOARCH)

# 컬러 출력을 위한 변수
GREEN=\033[0;32m
RED=\033[0;31m
YELLOW=\033[1;33m
NC=\033[0m # No Color

.PHONY: help build clean test lint run docker-build docker-run docker-push k8s-deploy dev-setup

# 기본 타겟
all: clean test build

# 도움말
help:
	@echo "$(GREEN)Available targets:$(NC)"
	@echo "  $(YELLOW)build$(NC)         - Build the application"
	@echo "  $(YELLOW)clean$(NC)         - Clean build artifacts"
	@echo "  $(YELLOW)test$(NC)          - Run tests"
	@echo "  $(YELLOW)test-integration$(NC) - Run integration tests"
	@echo "  $(YELLOW)lint$(NC)          - Run linter"
	@echo "  $(YELLOW)run$(NC)           - Run the application locally"
	@echo "  $(YELLOW)docker-build$(NC)  - Build Docker image"
	@echo "  $(YELLOW)docker-run$(NC)    - Run Docker container"
	@echo "  $(YELLOW)docker-push$(NC)   - Push Docker image"
	@echo "  $(YELLOW)k8s-deploy$(NC)    - Deploy to Kubernetes"
	@echo "  $(YELLOW)dev-setup$(NC)     - Setup development environment"

# 개발 환경 설정
dev-setup:
	@echo "$(GREEN)Setting up development environment...$(NC)"
	go mod download
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/swaggo/swag/cmd/swag@latest
	@echo "$(GREEN)Development environment setup complete!$(NC)"

# 빌드
build:
	@echo "$(GREEN)Building $(BINARY_NAME)...$(NC)"
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build \
		-ldflags="-w -s -X main.version=$(shell git describe --tags --always --dirty)" \
		-o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/server
	@echo "$(GREEN)Build complete: $(BUILD_DIR)/$(BINARY_NAME)$(NC)"

# 정리
clean:
	@echo "$(YELLOW)Cleaning build artifacts...$(NC)"
	rm -rf $(BUILD_DIR)
	go clean -cache
	docker system prune -f
	@echo "$(GREEN)Clean complete!$(NC)"

# 테스트
test:
	@echo "$(GREEN)Running unit tests...$(NC)"
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)Test complete! Coverage report: coverage.html$(NC)"

# 통합 테스트
test-integration:
	@echo "$(GREEN)Running integration tests...$(NC)"
	go test -v -tags=integration ./tests/integration/...
	@echo "$(GREEN)Integration tests complete!$(NC)"

# 린트
lint:
	@echo "$(GREEN)Running linter...$(NC)"
	golangci-lint run ./...
	@echo "$(GREEN)Lint complete!$(NC)"

# 로컬 실행
run:
	@echo "$(GREEN)Starting application locally...$(NC)"
	CONFIG_PATH=$(CONFIG_PATH) go run ./cmd/server serve

# Docker 빌드
docker-build:
	@echo "$(GREEN)Building Docker image...$(NC)"
	docker build -f deployments/docker/Dockerfile -t $(DOCKER_IMAGE):$(DOCKER_TAG) .
	@echo "$(GREEN)Docker image built: $(DOCKER_IMAGE):$(DOCKER_TAG)$(NC)"

# Docker 실행
docker-run: docker-build
	@echo "$(GREEN)Running Docker container...$(NC)"
	docker compose -f deployments/docker/docker-compose.yml up -d
	@echo "$(GREEN)Docker container started!$(NC)"

# Docker 중지
docker-stop:
	@echo "$(YELLOW)Stopping Docker containers...$(NC)"
	docker compose -f deployments/docker/docker-compose.yml down
	@echo "$(GREEN)Docker containers stopped!$(NC)"

# Docker 이미지 푸시
docker-push: docker-build
	@echo "$(GREEN)Pushing Docker image...$(NC)"
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	@echo "$(GREEN)Docker image pushed!$(NC)"

# Kubernetes 배포
k8s-deploy:
	@echo "$(GREEN)Deploying to Kubernetes...$(NC)"
	kubectl apply -f deployments/kubernetes/
	@echo "$(GREEN)Kubernetes deployment complete!$(NC)"

# Kubernetes 삭제
k8s-delete:
	@echo "$(YELLOW)Deleting from Kubernetes...$(NC)"
	kubectl delete -f deployments/kubernetes/
	@echo "$(GREEN)Kubernetes resources deleted!$(NC)"

# 벤치마크
benchmark:
	@echo "$(GREEN)Running benchmarks...$(NC)"
	go test -bench=. -benchmem ./...
	@echo "$(GREEN)Benchmark complete!$(NC)"

# API 문서 생성
docs:
	@echo "$(GREEN)Generating API documentation...$(NC)"
	swag init -g cmd/server/main.go -o docs/
	@echo "$(GREEN)API documentation generated in docs/$(NC)"

# 보안 체크
security:
	@echo "$(GREEN)Running security checks...$(NC)"
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
	else \
		echo "$(YELLOW)Installing gosec...$(NC)"; \
		go install github.com/securego/gosec/v2/cmd/gosec@latest; \
		gosec ./...; \
	fi
	@echo "$(GREEN)Security check complete!$(NC)"

# 의존성 업데이트
deps-update:
	@echo "$(GREEN)Updating dependencies...$(NC)"
	go get -u ./...
	go mod tidy
	@echo "$(GREEN)Dependencies updated!$(NC)"

# 로그 확인 (Docker)
logs:
	@echo "$(GREEN)Showing application logs...$(NC)"
	docker-compose -f deployments/docker/docker-compose.yml logs -f app

# 상태 확인
status:
	@echo "$(GREEN)Checking application status...$(NC)"
	curl -f http://localhost:3001/health || echo "$(RED)Application is not running$(NC)" 