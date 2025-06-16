package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantErr  bool
		setup    func() (string, func()) // 테스트 파일 생성 및 cleanup 함수
	}{
		{
			name:     "유효한 config.json 파일",
			filename: "config.json", // 실제 config.json 사용
			wantErr:  false,
		},
		{
			name:     "존재하지 않는 파일",
			filename: "nonexistent_config.json",
			wantErr:  true,
		},
		{
			name:     "빈 파일명",
			filename: "",
			wantErr:  true,
		},
		{
			name:    "잘못된 JSON 형식",
			wantErr: true,
			setup: func() (string, func()) {
				tmpFile, err := os.CreateTemp("", "invalid_config_*.json")
				require.NoError(t, err)

				// 잘못된 JSON 작성
				_, err = tmpFile.WriteString(`{"server": {"port": ":3001",}`) // 잘못된 JSON
				require.NoError(t, err)
				tmpFile.Close()

				return tmpFile.Name(), func() { os.Remove(tmpFile.Name()) }
			},
		},
		{
			name:    "빈 JSON 파일",
			wantErr: false,
			setup: func() (string, func()) {
				tmpFile, err := os.CreateTemp("", "empty_config_*.json")
				require.NoError(t, err)

				_, err = tmpFile.WriteString(`{}`)
				require.NoError(t, err)
				tmpFile.Close()

				return tmpFile.Name(), func() { os.Remove(tmpFile.Name()) }
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := tt.filename
			var cleanup func()

			// 동적으로 테스트 파일 생성이 필요한 경우
			if tt.setup != nil {
				filename, cleanup = tt.setup()
				defer cleanup()
			}

			config, err := LoadConfig(filename)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			// 실제 config.json을 사용한 경우 구체적인 값 검증
			if tt.filename == "config.json" {
				assert.Equal(t, ":3001", config.Server.Port)
				assert.Equal(t, 5, config.Server.ShutdownTimeoutSeconds)
				assert.Equal(t, "all", config.Kafka.Acks)
				assert.Equal(t, 6, config.Jobs.WorkerNum)
				assert.Equal(t, 100, config.Database.MongoDB.MaxPoolSize)
				assert.Equal(t, "ms", config.Database.InfluxDB.PrecisionUnit)
				assert.Contains(t, config.API.SystemDatabases, "admin")
			}
		})
	}
}

func TestConfig_Structure(t *testing.T) {
	t.Run("Config 구조체 필드 검증", func(t *testing.T) {
		// 최소한의 유효한 설정으로 Config 구조체 생성
		validConfigJSON := `{
			"server": {
				"port": ":8080",
				"shutdownTimeoutSeconds": 10
			},
			"kafka": {
				"bootstrapServers": "localhost:9092",
				"acks": "1",
				"enableIdempotence": "false",
				"compressionType": "none",
				"metadataTimeoutMs": 3000,
				"watermarkTimeoutMs": 500,
				"flushTimeoutMs": 10000
			},
			"jobs": {
				"workerNum": 4,
				"jobQueueCapacity": 50,
				"dividedJobs": 24,
				"jobsPerPartition": 2
			}
		}`

		var config Config
		err := json.Unmarshal([]byte(validConfigJSON), &config)
		require.NoError(t, err)

		// 구조체 필드 값 검증
		assert.Equal(t, ":8080", config.Server.Port)
		assert.Equal(t, 10, config.Server.ShutdownTimeoutSeconds)
		assert.Equal(t, "localhost:9092", config.Kafka.BootstrapServers)
		assert.Equal(t, "1", config.Kafka.Acks)
		assert.Equal(t, 4, config.Jobs.WorkerNum)
	})

	t.Run("빈 Config 구조체 기본값", func(t *testing.T) {
		var config Config

		// 빈 구조체의 기본값 확인
		assert.Empty(t, config.Server.Port)
		assert.Equal(t, 0, config.Server.ShutdownTimeoutSeconds)
		assert.Empty(t, config.Kafka.BootstrapServers)
		assert.Equal(t, 0, config.Jobs.WorkerNum)
	})
}

func TestLoadConfig_Integration(t *testing.T) {
	t.Run("실제 config.json과 구조체 일치성 검증", func(t *testing.T) {
		// 현재 디렉토리에서 config.json 로드
		config, err := LoadConfig("config.json")
		require.NoError(t, err)

		// 중요한 설정 값들이 제대로 로드되었는지 검증
		assert.NotEmpty(t, config.Server.Port, "서버 포트는 비어있으면 안됨")
		assert.Greater(t, config.Server.ShutdownTimeoutSeconds, 0, "셧다운 타임아웃은 0보다 커야 함")

		assert.NotEmpty(t, config.Kafka.BootstrapServers, "Kafka 브로커 주소는 필수")
		assert.NotEmpty(t, config.Kafka.Acks, "Kafka Acks 설정은 필수")

		assert.Greater(t, config.Jobs.WorkerNum, 0, "워커 수는 0보다 커야 함")
		assert.Greater(t, config.Jobs.JobQueueCapacity, 0, "작업 큐 용량은 0보다 커야 함")

		// 데이터베이스 설정 검증
		assert.Greater(t, config.Database.MongoDB.MaxPoolSize, 0, "MongoDB 풀 사이즈는 0보다 커야 함")
		assert.Greater(t, config.Database.InfluxDB.HTTPRequestTimeoutSeconds, 0, "InfluxDB 타임아웃은 0보다 커야 함")

		// 형식 설정 검증
		assert.NotEmpty(t, config.Formats.DateTimeFormat, "날짜 형식은 비어있으면 안됨")
		assert.NotEmpty(t, config.Formats.RFC3339NanoFormat, "RFC3339 형식은 비어있으면 안됨")

		// API 설정 검증
		assert.NotEmpty(t, config.API.DefaultSort.MongoDB, "MongoDB 기본 정렬은 비어있으면 안됨")
		assert.NotEmpty(t, config.API.DefaultSort.InfluxDB, "InfluxDB 기본 정렬은 비어있으면 안됨")
		assert.NotEmpty(t, config.API.SystemDatabases, "시스템 데이터베이스 목록은 비어있으면 안됨")
	})
}

func TestLoadConfig_EdgeCases(t *testing.T) {
	t.Run("읽기 전용 파일", func(t *testing.T) {
		// 임시 설정 파일 생성
		tmpFile, err := os.CreateTemp("", "readonly_config_*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// 유효한 JSON 작성
		validJSON := `{"server": {"port": ":3001"}}`
		_, err = tmpFile.WriteString(validJSON)
		require.NoError(t, err)
		tmpFile.Close()

		// 파일을 읽기 전용으로 변경
		err = os.Chmod(tmpFile.Name(), 0444)
		require.NoError(t, err)

		// 읽기는 성공해야 함
		config, err := LoadConfig(tmpFile.Name())
		assert.NoError(t, err)
		assert.Equal(t, ":3001", config.Server.Port)
	})

	t.Run("매우 큰 설정 파일", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "large_config_*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// 큰 설정 파일 생성 (많은 시스템 데이터베이스)
		largeConfig := Config{}
		largeConfig.Server.Port = ":3001"
		for i := 0; i < 1000; i++ {
			largeConfig.API.SystemDatabases = append(largeConfig.API.SystemDatabases, "db"+string(rune(i)))
		}

		data, err := json.Marshal(largeConfig)
		require.NoError(t, err)

		_, err = tmpFile.Write(data)
		require.NoError(t, err)
		tmpFile.Close()

		// 큰 파일도 정상적으로 로드되어야 함
		config, err := LoadConfig(tmpFile.Name())
		assert.NoError(t, err)
		assert.Equal(t, ":3001", config.Server.Port)
		assert.Len(t, config.API.SystemDatabases, 1000)
	})
}
