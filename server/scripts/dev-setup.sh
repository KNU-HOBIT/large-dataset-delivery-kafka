#!/bin/bash

# 개발 환경 설정 스크립트
# 크리티컬하지 않은 부분은 SKIP하고 계속 진행

# 색상 설정
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Setting up development environment...${NC}"

# Go 버전 확인 (필수)
echo -e "${YELLOW}Checking Go version...${NC}"
if ! command -v go &> /dev/null; then
    echo -e "${RED}❌ Go is not installed. Please install Go 1.19 or later.${NC}"
    exit 1
fi

GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
MIN_VERSION="1.19"

if [ "$(printf '%s\n' "$MIN_VERSION" "$GO_VERSION" | sort -V | head -n1)" != "$MIN_VERSION" ]; then
    echo -e "${RED}❌ Go version $GO_VERSION is too old. Please upgrade to Go 1.19 or later.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Go version $GO_VERSION is compatible${NC}"

# Docker 확인 (선택적)
echo -e "${YELLOW}Checking Docker...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${BLUE}⏭️  Docker not found, skipping Docker checks${NC}"
    echo -e "${YELLOW}💡 Install Docker later if you need containerization${NC}"
else
    echo -e "${GREEN}✅ Docker is available${NC}"
    
    # Docker Compose 확인 (선택적)
    echo -e "${YELLOW}Checking Docker Compose...${NC}"
    if ! docker compose version &> /dev/null && ! command -v docker-compose &> /dev/null; then
        echo -e "${BLUE}⏭️  Docker Compose not found, skipping${NC}"
        echo -e "${YELLOW}💡 Install Docker Compose later if needed${NC}"
    else
        # Docker Compose 버전 표시
        if docker compose version &> /dev/null; then
            COMPOSE_VERSION=$(docker compose version --short)
            echo -e "${GREEN}✅ Docker Compose v2 ($COMPOSE_VERSION) is available${NC}"
        elif command -v docker-compose &> /dev/null; then
            COMPOSE_VERSION=$(docker-compose --version | grep -o 'version [0-9.]*' | cut -d' ' -f2)
            echo -e "${GREEN}✅ Docker Compose v1 ($COMPOSE_VERSION) is available${NC}"
        fi
    fi
fi

# Go 모듈 다운로드 (필수)
echo -e "${YELLOW}Downloading Go modules...${NC}"
# server 디렉토리로 이동 (스크립트가 server/scripts/에서 실행되므로)
cd "$(dirname "$0")/.."
if go mod download && go mod verify; then
    echo -e "${GREEN}✅ Go modules downloaded${NC}"
else
    echo -e "${RED}❌ Failed to download Go modules${NC}"
    exit 1
fi

# 개발 도구 설치 (선택적)
echo -e "${YELLOW}Installing development tools...${NC}"

# Linter
if ! command -v golangci-lint &> /dev/null; then
    echo "Installing golangci-lint..."
    if go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; then
        echo -e "${GREEN}✅ golangci-lint installed${NC}"
    else
        echo -e "${BLUE}⏭️  Failed to install golangci-lint, skipping${NC}"
    fi
else
    echo -e "${GREEN}✅ golangci-lint already installed${NC}"
fi

# Security scanner
if ! command -v gosec &> /dev/null; then
    echo "Installing gosec..."
    if go install github.com/securego/gosec/v2/cmd/gosec@latest; then
        echo -e "${GREEN}✅ gosec installed${NC}"
    else
        echo -e "${BLUE}⏭️  Failed to install gosec, skipping${NC}"
    fi
else
    echo -e "${GREEN}✅ gosec already installed${NC}"
fi

# API documentation
if ! command -v swag &> /dev/null; then
    echo "Installing swag..."
    if go install github.com/swaggo/swag/cmd/swag@latest; then
        echo -e "${GREEN}✅ swag installed${NC}"
    else
        echo -e "${BLUE}⏭️  Failed to install swag, skipping${NC}"
    fi
else
    echo -e "${GREEN}✅ swag already installed${NC}"
fi

# Hot reload
if ! command -v air &> /dev/null; then
    echo "Installing air..."
    if go install github.com/air-verse/air@latest; then
        echo -e "${GREEN}✅ air installed${NC}"
    else
        echo -e "${BLUE}⏭️  Failed to install air, skipping${NC}"
    fi
else
    echo -e "${GREEN}✅ air already installed${NC}"
fi

echo -e "${GREEN}✅ Development tools setup complete${NC}"

# 환경변수 파일 설정 (선택적)
echo -e "${YELLOW}Setting up environment files...${NC}"
if [ ! -f ".env" ]; then
    if [ -f "env.example" ]; then
        cp env.example .env
        echo -e "${GREEN}✅ Created .env file from template${NC}"
        echo -e "${YELLOW}⚠️  Please update .env file with your configuration${NC}"
    else
        echo -e "${BLUE}⏭️  env.example not found, skipping .env creation${NC}"
        echo -e "${YELLOW}💡 Configuration is handled by config.json in this project${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  .env file already exists, skipping...${NC}"
fi

# Git hooks 설정 (선택적)
echo -e "${YELLOW}Setting up Git hooks...${NC}"
if [ -d ".git" ]; then
    mkdir -p .git/hooks

    # Pre-commit hook
    cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
echo "Running pre-commit checks..."

# Format check
if [ "$(gofmt -s -l . | wc -l)" -gt 0 ]; then
    echo "❌ Code is not formatted. Please run 'gofmt -s -w .'"
    gofmt -s -l .
    exit 1
fi

# Lint check (if available)
if command -v golangci-lint &> /dev/null; then
    golangci-lint run ./...
    if [ $? -ne 0 ]; then
        echo "❌ Linting failed"
        exit 1
    fi
else
    echo "⏭️  golangci-lint not available, skipping lint check"
fi

# Tests
go test -short ./...
if [ $? -ne 0 ]; then
    echo "❌ Tests failed"
    exit 1
fi

echo "✅ Pre-commit checks passed"
EOF

    chmod +x .git/hooks/pre-commit
    echo -e "${GREEN}✅ Git hooks configured${NC}"
else
    echo -e "${BLUE}⏭️  Not a git repository, skipping Git hooks setup${NC}"
fi

# 디렉토리 생성 (필수)
echo -e "${YELLOW}Creating necessary directories...${NC}"
mkdir -p logs tmp build
echo -e "${GREEN}✅ Directories created${NC}"

# Air 설정 파일 생성 (선택적)
if [ ! -f ".air.toml" ]; then
    echo -e "${YELLOW}Creating Air configuration...${NC}"
    cat > .air.toml << 'EOF'
root = "."
testdata_dir = "testdata"
tmp_dir = "tmp"

[build]
  args_bin = []
  bin = "./tmp/main"
  cmd = "go build -o ./tmp/main ."
  delay = 1000
  exclude_dir = ["assets", "tmp", "vendor", "testdata", "build", "deployments", "scripts", "configs"]
  exclude_file = []
  exclude_regex = ["_test.go"]
  exclude_unchanged = false
  follow_symlink = false
  full_bin = ""
  include_dir = []
  include_ext = ["go", "tpl", "tmpl", "html", "yaml", "yml", "json"]
  kill_delay = "0s"
  log = "build-errors.log"
  send_interrupt = false
  stop_on_root = false

[color]
  app = ""
  build = "yellow"
  main = "magenta"
  runner = "green"
  watcher = "cyan"

[log]
  time = false

[misc]
  clean_on_exit = false

[screen]
  clear_on_rebuild = false
EOF
    echo -e "${GREEN}✅ Air configuration created${NC}"
else
    echo -e "${YELLOW}⚠️  .air.toml already exists, skipping...${NC}"
fi

echo -e "${GREEN}🎉 Development environment setup complete!${NC}"
echo -e "${YELLOW}Next steps:${NC}"
echo -e "  1. Configuration is managed via config.json"
echo -e "  2. Run '${GREEN}make dev-setup${NC}' to verify everything works"
echo -e "  3. Run '${GREEN}make run${NC}' to start the development server"
echo -e "  4. Run '${GREEN}air${NC}' for hot reload during development (if installed)"
echo -e "  5. Run '${GREEN}make docker-run${NC}' to test with Docker (if available)" 