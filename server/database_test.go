package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateInfluxQueryParams(t *testing.T) {
	tests := []struct {
		name         string
		connectionID string
		bucket       string
		measurement  string
		tagKey       string
		tagValue     string
	}{
		{
			name:         "모든 파라미터가 있는 경우",
			connectionID: "influx-conn-1",
			bucket:       "test-bucket",
			measurement:  "temperature",
			tagKey:       "sensor_id",
			tagValue:     "sensor001",
		},
		{
			name:         "빈 문자열 파라미터들",
			connectionID: "",
			bucket:       "",
			measurement:  "",
			tagKey:       "",
			tagValue:     "",
		},
		{
			name:         "특수문자가 포함된 파라미터들",
			connectionID: "influx-conn-special",
			bucket:       "test/bucket-with-special_chars",
			measurement:  "temp@measurement",
			tagKey:       "sensor#key",
			tagValue:     "sensor$value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateInfluxQueryParams(tt.connectionID, tt.bucket, tt.measurement, tt.tagKey, tt.tagValue)

			assert.Equal(t, tt.connectionID, result.ConnectionID)
			assert.Equal(t, "influx", result.DBType)
			assert.NotNil(t, result.Influx)
			assert.Equal(t, tt.bucket, result.Influx.Bucket)
			assert.Equal(t, tt.measurement, result.Influx.Measurement)
			assert.Equal(t, tt.tagKey, result.Influx.TagKey)
			assert.Equal(t, tt.tagValue, result.Influx.TagValue)
		})
	}
}

func TestCreateMongoQueryParams(t *testing.T) {
	tests := []struct {
		name         string
		connectionID string
		database     string
		collection   string
		timeField    string
		tagKey       string
		tagValue     string
	}{
		{
			name:         "모든 파라미터가 있는 경우",
			connectionID: "mongo-conn-1",
			database:     "test-db",
			collection:   "sensors",
			timeField:    "timestamp",
			tagKey:       "device_id",
			tagValue:     "device001",
		},
		{
			name:         "빈 문자열 파라미터들",
			connectionID: "",
			database:     "",
			collection:   "",
			timeField:    "",
			tagKey:       "",
			tagValue:     "",
		},
		{
			name:         "MongoDB 명명 규칙 테스트",
			connectionID: "mongo-conn-naming",
			database:     "my_database",
			collection:   "my-collection",
			timeField:    "created_at",
			tagKey:       "region",
			tagValue:     "us-west-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateMongoQueryParams(tt.connectionID, tt.database, tt.collection, tt.timeField, tt.tagKey, tt.tagValue)

			assert.Equal(t, tt.connectionID, result.ConnectionID)
			assert.Equal(t, "mongo", result.DBType)
			assert.NotNil(t, result.Mongo)
			assert.Equal(t, tt.database, result.Mongo.Database)
			assert.Equal(t, tt.collection, result.Mongo.Collection)
			assert.Equal(t, tt.timeField, result.Mongo.TimeField)
			assert.Equal(t, tt.tagKey, result.Mongo.TagKey)
			assert.Equal(t, tt.tagValue, result.Mongo.TagValue)
		})
	}
}

func TestQueryParams_GetInfluxParams(t *testing.T) {
	t.Run("Influx 파라미터가 있는 경우", func(t *testing.T) {
		params := QueryParams{
			ConnectionID: "test-conn",
			DBType:       "influx",
			Influx: &InfluxQueryParams{
				Bucket:      "test-bucket",
				Measurement: "test-measurement",
				TagKey:      "test-key",
				TagValue:    "test-value",
			},
		}

		result := params.GetInfluxParams()

		assert.NotNil(t, result)
		assert.Equal(t, "test-bucket", result.Bucket)
		assert.Equal(t, "test-measurement", result.Measurement)
		assert.Equal(t, "test-key", result.TagKey)
		assert.Equal(t, "test-value", result.TagValue)
	})

	t.Run("Influx 파라미터가 nil인 경우", func(t *testing.T) {
		params := QueryParams{
			ConnectionID: "test-conn",
			DBType:       "influx",
			Influx:       nil,
		}

		result := params.GetInfluxParams()

		assert.NotNil(t, result)
		assert.Equal(t, "", result.Bucket)
		assert.Equal(t, "", result.Measurement)
		assert.Equal(t, "", result.TagKey)
		assert.Equal(t, "", result.TagValue)
	})
}

func TestQueryParams_GetMongoParams(t *testing.T) {
	t.Run("Mongo 파라미터가 있는 경우", func(t *testing.T) {
		params := QueryParams{
			ConnectionID: "test-conn",
			DBType:       "mongo",
			Mongo: &MongoQueryParams{
				Database:   "test-db",
				Collection: "test-collection",
				TimeField:  "timestamp",
				TagKey:     "test-key",
				TagValue:   "test-value",
			},
		}

		result := params.GetMongoParams()

		assert.NotNil(t, result)
		assert.Equal(t, "test-db", result.Database)
		assert.Equal(t, "test-collection", result.Collection)
		assert.Equal(t, "timestamp", result.TimeField)
		assert.Equal(t, "test-key", result.TagKey)
		assert.Equal(t, "test-value", result.TagValue)
	})

	t.Run("Mongo 파라미터가 nil인 경우", func(t *testing.T) {
		params := QueryParams{
			ConnectionID: "test-conn",
			DBType:       "mongo",
			Mongo:        nil,
		}

		result := params.GetMongoParams()

		assert.NotNil(t, result)
		assert.Equal(t, "", result.Database)
		assert.Equal(t, "", result.Collection)
		assert.Equal(t, "", result.TimeField)
		assert.Equal(t, "", result.TagKey)
		assert.Equal(t, "", result.TagValue)
	})
}

func TestNewConnectionManager(t *testing.T) {
	t.Run("새 ConnectionManager 생성", func(t *testing.T) {
		cm := NewConnectionManager()

		assert.NotNil(t, cm)
		assert.NotNil(t, cm.connections)
		assert.NotNil(t, cm.connInfos)
		assert.Equal(t, 0, len(cm.connections))
		assert.Equal(t, 0, len(cm.connInfos))
	})
}
