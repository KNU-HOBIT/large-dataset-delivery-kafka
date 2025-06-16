package main

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetParam(t *testing.T) {
	tests := []struct {
		name      string
		queryStr  string
		preferred string
		fallback  string
		expected  string
	}{
		{
			name:      "선호하는 파라미터가 있는 경우",
			queryStr:  "database=mydb&bucket=oldbucket",
			preferred: "database",
			fallback:  "bucket",
			expected:  "mydb",
		},
		{
			name:      "선호하는 파라미터가 없고 fallback이 있는 경우",
			queryStr:  "bucket=oldbucket",
			preferred: "database",
			fallback:  "bucket",
			expected:  "oldbucket",
		},
		{
			name:      "둘 다 없는 경우",
			queryStr:  "other=value",
			preferred: "database",
			fallback:  "bucket",
			expected:  "",
		},
		{
			name:      "선호하는 파라미터가 빈 값인 경우",
			queryStr:  "database=&bucket=oldbucket",
			preferred: "database",
			fallback:  "bucket",
			expected:  "oldbucket",
		},
		{
			name:      "두 파라미터 모두 있지만 선호하는 것이 빈 값",
			queryStr:  "database=&bucket=oldbucket",
			preferred: "database",
			fallback:  "bucket",
			expected:  "oldbucket",
		},
		{
			name:      "선호하는 파라미터에 공백이 있는 경우",
			queryStr:  "database=%20mydb%20&bucket=oldbucket",
			preferred: "database",
			fallback:  "bucket",
			expected:  " mydb ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// HTTP 요청 생성
			req := httptest.NewRequest("GET", "/?"+tt.queryStr, nil)

			result := getParam(req, tt.preferred, tt.fallback)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseTimeRangeParams(t *testing.T) {
	tests := []struct {
		name     string
		queryStr string
		want     *TimeRangeParams
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "유효한 모든 파라미터",
			queryStr: "connection_id=test-conn&database=mydb&table=mytable&tag_key=sensor&tag_value=01&mongo_database=mongodb&collection=data&time_field=timestamp",
			want: &TimeRangeParams{
				ConnectionID:  "test-conn",
				Database:      "mydb",
				Table:         "mytable",
				TagKey:        "sensor",
				TagValue:      "01",
				MongoDatabase: "mongodb",
				Collection:    "data",
				TimeField:     "timestamp",
			},
			wantErr: false,
		},
		{
			name:     "필수 파라미터만 있는 경우",
			queryStr: "connection_id=test-conn",
			want: &TimeRangeParams{
				ConnectionID:  "test-conn",
				Database:      "",
				Table:         "",
				TagKey:        "",
				TagValue:      "",
				MongoDatabase: "",
				Collection:    "",
				TimeField:     "",
			},
			wantErr: false,
		},
		{
			name:     "레거시 파라미터 사용 (bucket, measurement)",
			queryStr: "connection_id=test-conn&bucket=legacy-bucket&measurement=legacy-table",
			want: &TimeRangeParams{
				ConnectionID:  "test-conn",
				Database:      "legacy-bucket",
				Table:         "legacy-table",
				TagKey:        "",
				TagValue:      "",
				MongoDatabase: "",
				Collection:    "",
				TimeField:     "",
			},
			wantErr: false,
		},
		{
			name:     "새 파라미터가 레거시보다 우선",
			queryStr: "connection_id=test-conn&database=new-db&bucket=old-bucket&table=new-table&measurement=old-table",
			want: &TimeRangeParams{
				ConnectionID:  "test-conn",
				Database:      "new-db",
				Table:         "new-table",
				TagKey:        "",
				TagValue:      "",
				MongoDatabase: "",
				Collection:    "",
				TimeField:     "",
			},
			wantErr: false,
		},
		{
			name:     "connection_id 누락",
			queryStr: "database=mydb&table=mytable",
			want:     nil,
			wantErr:  true,
			errMsg:   "connection_id is required",
		},
		{
			name:     "빈 connection_id",
			queryStr: "connection_id=&database=mydb",
			want:     nil,
			wantErr:  true,
			errMsg:   "connection_id is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/?"+tt.queryStr, nil)

			result, err := parseTimeRangeParams(req)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestParseDatasetDetailParams(t *testing.T) {
	tests := []struct {
		name     string
		queryStr string
		want     *DatasetDetailParams
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "유효한 모든 파라미터",
			queryStr: "connection_id=test-conn&database_name=mydb&table_name=mytable&tag_key=sensor&tag_value=01&mongo_database=mongodb&collection=data&time_field=timestamp",
			want: &DatasetDetailParams{
				ConnectionID:  "test-conn",
				Database:      "mydb",
				Table:         "mytable",
				TagKey:        "sensor",
				TagValue:      "01",
				MongoDatabase: "mongodb",
				Collection:    "data",
				TimeField:     "timestamp",
			},
			wantErr: false,
		},
		{
			name:     "레거시 파라미터 사용 (bucket_name, measurement)",
			queryStr: "connection_id=test-conn&bucket_name=legacy-bucket&measurement=legacy-table",
			want: &DatasetDetailParams{
				ConnectionID:  "test-conn",
				Database:      "legacy-bucket",
				Table:         "legacy-table",
				TagKey:        "",
				TagValue:      "",
				MongoDatabase: "",
				Collection:    "",
				TimeField:     "",
			},
			wantErr: false,
		},
		{
			name:     "connection_id 누락",
			queryStr: "database_name=mydb&table_name=mytable",
			want:     nil,
			wantErr:  true,
			errMsg:   "connection_id is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/?"+tt.queryStr, nil)

			result, err := parseDatasetDetailParams(req)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestParseDataExportParams(t *testing.T) {
	tests := []struct {
		name     string
		queryStr string
		want     *DataExportParams
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "유효한 모든 파라미터",
			queryStr: "connection_id=test-conn&database=mydb&table=mytable&tag_key=sensor&tag_value=01&kafka_brokers=localhost:9092&send_topic=test-topic&drop_last=true&mongo_database=mongodb&collection=data&time_field=timestamp",
			want: &DataExportParams{
				ConnectionID:  "test-conn",
				Database:      "mydb",
				Table:         "mytable",
				TagKey:        "sensor",
				TagValue:      "01",
				KafkaBrokers:  "localhost:9092",
				SendTopic:     "test-topic",
				DropLast:      true,
				MongoDatabase: "mongodb",
				Collection:    "data",
				TimeField:     "timestamp",
			},
			wantErr: false,
		},
		{
			name:     "drop_last false 명시",
			queryStr: "connection_id=test-conn&kafka_brokers=localhost:9092&send_topic=test-topic&drop_last=false",
			want: &DataExportParams{
				ConnectionID: "test-conn",
				KafkaBrokers: "localhost:9092",
				SendTopic:    "test-topic",
				DropLast:     false,
			},
			wantErr: false,
		},
		{
			name:     "drop_last F로 설정",
			queryStr: "connection_id=test-conn&kafka_brokers=localhost:9092&send_topic=test-topic&drop_last=F",
			want: &DataExportParams{
				ConnectionID: "test-conn",
				KafkaBrokers: "localhost:9092",
				SendTopic:    "test-topic",
				DropLast:     false,
			},
			wantErr: false,
		},
		{
			name:     "drop_last 기본값 (true)",
			queryStr: "connection_id=test-conn&kafka_brokers=localhost:9092&send_topic=test-topic",
			want: &DataExportParams{
				ConnectionID: "test-conn",
				KafkaBrokers: "localhost:9092",
				SendTopic:    "test-topic",
				DropLast:     true,
			},
			wantErr: false,
		},
		{
			name:     "connection_id 누락",
			queryStr: "kafka_brokers=localhost:9092&send_topic=test-topic",
			want:     nil,
			wantErr:  true,
			errMsg:   "connection_id, kafka_brokers, and send_topic are required",
		},
		{
			name:     "kafka_brokers 누락",
			queryStr: "connection_id=test-conn&send_topic=test-topic",
			want:     nil,
			wantErr:  true,
			errMsg:   "connection_id, kafka_brokers, and send_topic are required",
		},
		{
			name:     "send_topic 누락",
			queryStr: "connection_id=test-conn&kafka_brokers=localhost:9092",
			want:     nil,
			wantErr:  true,
			errMsg:   "connection_id, kafka_brokers, and send_topic are required",
		},
		{
			name:     "레거시 파라미터 사용",
			queryStr: "connection_id=test-conn&bucket=legacy-bucket&measurement=legacy-table&kafka_brokers=localhost:9092&send_topic=test-topic",
			want: &DataExportParams{
				ConnectionID: "test-conn",
				Database:     "legacy-bucket",
				Table:        "legacy-table",
				KafkaBrokers: "localhost:9092",
				SendTopic:    "test-topic",
				DropLast:     true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/?"+tt.queryStr, nil)

			result, err := parseDataExportParams(req)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestParseParams_SpecialCharacters(t *testing.T) {
	t.Run("URL 인코딩된 파라미터", func(t *testing.T) {
		queryStr := "connection_id=test-conn&database=my%20database&table=my%2Btable&tag_key=sensor%40home"
		req := httptest.NewRequest("GET", "/?"+queryStr, nil)

		result, err := parseTimeRangeParams(req)
		require.NoError(t, err)

		assert.Equal(t, "test-conn", result.ConnectionID)
		assert.Equal(t, "my database", result.Database)
		assert.Equal(t, "my+table", result.Table)
		assert.Equal(t, "sensor@home", result.TagKey)
	})

	t.Run("특수 문자가 포함된 값들", func(t *testing.T) {
		// URL 값들을 먼저 인코딩
		params := url.Values{}
		params.Set("connection_id", "test-conn")
		params.Set("kafka_brokers", "broker1:9092,broker2:9092")
		params.Set("send_topic", "topic-with-dashes_and_underscores")
		params.Set("tag_value", "value/with/slashes")

		req := httptest.NewRequest("GET", "/?"+params.Encode(), nil)

		result, err := parseDataExportParams(req)
		require.NoError(t, err)

		assert.Equal(t, "test-conn", result.ConnectionID)
		assert.Equal(t, "broker1:9092,broker2:9092", result.KafkaBrokers)
		assert.Equal(t, "topic-with-dashes_and_underscores", result.SendTopic)
		assert.Equal(t, "value/with/slashes", result.TagValue)
	})
}

func TestParseParams_EdgeCases(t *testing.T) {
	t.Run("매우 긴 파라미터 값", func(t *testing.T) {
		longValue := string(make([]byte, 1000))
		for i := range longValue {
			longValue = longValue[:i] + "a" + longValue[i+1:]
		}

		params := url.Values{}
		params.Set("connection_id", "test-conn")
		params.Set("database", longValue)

		req := httptest.NewRequest("GET", "/?"+params.Encode(), nil)

		result, err := parseTimeRangeParams(req)
		require.NoError(t, err)
		assert.Equal(t, longValue, result.Database)
	})

	t.Run("중복된 파라미터", func(t *testing.T) {
		// Go의 http 패키지는 중복된 파라미터 중 첫 번째 값을 사용
		queryStr := "connection_id=first&connection_id=second&database=mydb"
		req := httptest.NewRequest("GET", "/?"+queryStr, nil)

		result, err := parseTimeRangeParams(req)
		require.NoError(t, err)
		assert.Equal(t, "first", result.ConnectionID)
	})

	t.Run("빈 쿼리 스트링", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)

		_, err := parseTimeRangeParams(req)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection_id is required")
	})
}
