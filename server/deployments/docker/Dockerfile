# Build stage
FROM golang:1.22 AS builder

# 보안을 위한 필수 패키지 설치 (librdkafka는 confluent-kafka-go에 필요)
RUN apt-get update && apt-get install -y \
    git \
    ca-certificates \
    tzdata \
    gcc \
    libc6-dev \
    librdkafka-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# Go 모듈 다운로드 (레이어 캐싱 최적화)
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# 소스 코드 복사
COPY . .

# 바이너리 빌드 (CGO 활성화, confluent-kafka-go 지원)
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
    -buildvcs=false \
    -ldflags='-w -s' \
    -o server .

# Final stage
FROM debian:12-slim

# 런타임 의존성 설치
RUN apt-get update && apt-get install -y \
    ca-certificates \
    tzdata \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Non-root 사용자 생성
RUN groupadd -g 1001 appgroup && \
    useradd -u 1001 -g appgroup -s /bin/bash -m appuser

# 애플리케이션 바이너리 복사
COPY --from=builder /app/server /server

# 설정 파일 복사
COPY --from=builder /app/config.json /config.json

# Non-root 사용자로 실행
USER appuser:appgroup

# 헬스체크 설정 (간단한 프로세스 체크로 변경)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep server || exit 1

# 포트 노출
EXPOSE 3001

# 기본 환경 변수
ENV GIN_MODE=release
ENV CONFIG_PATH=/config.json

# 애플리케이션 실행
ENTRYPOINT ["/server"] 