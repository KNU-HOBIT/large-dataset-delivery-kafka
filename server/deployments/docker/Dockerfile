# Build stage
FROM golang:1.21-alpine AS builder

# 보안을 위한 필수 패키지 설치
RUN apk add --no-cache git ca-certificates tzdata

# 작업 디렉토리 설정
WORKDIR /app

# Go 모듈 다운로드 (레이어 캐싱 최적화)
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# 소스 코드 복사
COPY . .

# 바이너리 빌드 (정적 링킹, 디버그 정보 제거)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o server ./cmd/server

# Final stage
FROM scratch

# 인증서와 타임존 정보 복사
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Non-root 사용자 생성을 위한 파일들
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

# 애플리케이션 바이너리 복사
COPY --from=builder /app/server /server

# 설정 파일 복사
COPY --from=builder /app/configs /configs

# Non-root 사용자로 실행
USER nobody:nobody

# 헬스체크 설정
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["/server", "healthcheck"]

# 포트 노출
EXPOSE 3001

# 기본 환경 변수
ENV GIN_MODE=release
ENV CONFIG_PATH=/configs/production.yaml

# 애플리케이션 실행
ENTRYPOINT ["/server"]
CMD ["serve"] 