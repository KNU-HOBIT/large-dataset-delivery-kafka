package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDBClient implements DatabaseClient for MongoDB
type MongoDBClient struct {
	client   *mongo.Client
	url      string
	database string
}

// NewMongoDBClient creates a new MongoDB client
func NewMongoDBClient(url, database string) (*MongoDBClient, error) {
	// Configure client options with connection pool settings
	clientOptions := options.Client().ApplyURI(url).
		SetMaxPoolSize(uint64(config.Database.MongoDB.MaxPoolSize)).                                                  // 최대 연결 수
		SetMaxConnIdleTime(time.Duration(config.Database.MongoDB.MaxConnIdleTimeSeconds) * time.Second).              // 유휴 연결 타임아웃
		SetConnectTimeout(time.Duration(config.Database.MongoDB.ConnectTimeoutSeconds) * time.Second).                // 연결 타임아웃
		SetSocketTimeout(time.Duration(config.Database.MongoDB.SocketTimeoutSeconds) * time.Second).                  // 소켓 타임아웃
		SetServerSelectionTimeout(time.Duration(config.Database.MongoDB.ServerSelectionTimeoutSeconds) * time.Second) // 서버 선택 타임아웃

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Database.MongoDB.QueryTimeoutSeconds)*time.Second)
	defer cancel()
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &MongoDBClient{
		client:   client,
		url:      url,
		database: database,
	}, nil
}

// CheckStartEndRange implements DatabaseClient interface for MongoDB
func (m *MongoDBClient) CheckStartEndRange(params QueryParams) (TimeRangeStr, error) {
	mongoParams := params.GetMongoParams()
	collection := m.client.Database(mongoParams.Database).Collection(mongoParams.Collection)

	// Build filter
	filter := bson.M{}
	if mongoParams.TagKey != "" && mongoParams.TagValue != "" {
		filter[mongoParams.TagKey] = mongoParams.TagValue
	}

	// If no TimeField is provided, return empty TimeRangeStr (record-count based processing)
	if mongoParams.TimeField == "" {
		return TimeRangeStr{
			startStr: "",
			endStr:   "",
		}, nil
	}

	// Find first record
	opts := options.FindOne().SetSort(bson.M{mongoParams.TimeField: 1})
	var firstResult bson.M
	err := collection.FindOne(context.Background(), filter, opts).Decode(&firstResult)
	if err != nil {
		return TimeRangeStr{}, err
	}

	// Find last record
	opts = options.FindOne().SetSort(bson.M{mongoParams.TimeField: -1})
	var lastResult bson.M
	err = collection.FindOne(context.Background(), filter, opts).Decode(&lastResult)
	if err != nil {
		return TimeRangeStr{}, err
	}

	// Extract timestamps
	var startTime, endTime time.Time
	if t, ok := firstResult[mongoParams.TimeField].(primitive.DateTime); ok {
		startTime = t.Time()
	} else if t, ok := firstResult[mongoParams.TimeField].(time.Time); ok {
		startTime = t
	}

	if t, ok := lastResult[mongoParams.TimeField].(primitive.DateTime); ok {
		endTime = t.Time()
	} else if t, ok := lastResult[mongoParams.TimeField].(time.Time); ok {
		endTime = t
	}

	return TimeRangeStr{
		startStr: startTime.Format(config.Formats.DateTimeFormat),
		endStr:   endTime.Format(config.Formats.DateTimeFormat),
	}, nil
}

// GetTotalRecordCount implements DatabaseClient interface for MongoDB
func (m *MongoDBClient) GetTotalRecordCount(timeRange TimeRangeStr, params QueryParams) (int64, error) {
	mongoParams := params.GetMongoParams()
	collection := m.client.Database(mongoParams.Database).Collection(mongoParams.Collection)

	// Build filter - only using tag filters, no time range for MongoDB
	filter := bson.M{}
	if mongoParams.TagKey != "" && mongoParams.TagValue != "" {
		filter[mongoParams.TagKey] = mongoParams.TagValue
	}

	count, err := collection.CountDocuments(context.Background(), filter)
	return count, err
}

// CalculateEndTimes implements DatabaseClient interface for MongoDB
func (m *MongoDBClient) CalculateEndTimes(startStr string, recordsPerJob int64, params QueryParams) ([]string, error) {
	mongoParams := params.GetMongoParams()
	collection := m.client.Database(mongoParams.Database).Collection(mongoParams.Collection)

	// If no TimeField provided, return empty array since we use record-count based processing
	if mongoParams.TimeField == "" {
		return []string{}, nil
	}

	startTime, err := time.Parse(config.Formats.DateTimeFormat, startStr)
	if err != nil {
		return nil, err
	}

	// Build filter
	filter := bson.M{
		mongoParams.TimeField: bson.M{"$gte": startTime},
	}
	if mongoParams.TagKey != "" && mongoParams.TagValue != "" {
		filter[mongoParams.TagKey] = mongoParams.TagValue
	}

	// Find documents and calculate end times
	opts := options.Find().SetSort(bson.M{mongoParams.TimeField: 1})
	cursor, err := collection.Find(context.Background(), filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	var endTimes []string
	var count int64
	for cursor.Next(context.Background()) {
		count++
		if count%recordsPerJob == 0 {
			var result bson.M
			if err := cursor.Decode(&result); err != nil {
				continue
			}

			var t time.Time
			if timeVal, ok := result[mongoParams.TimeField].(primitive.DateTime); ok {
				t = timeVal.Time()
			} else if timeVal, ok := result[mongoParams.TimeField].(time.Time); ok {
				t = timeVal
			}

			endTimes = append(endTimes, increaseOneNanosecond(t).Format(time.RFC3339Nano))
		}
	}

	return endTimes, nil
}

// GetDatasets implements DatabaseClient interface for MongoDB
func (m *MongoDBClient) GetDatasets() ([]Dataset, error) {
	// List databases
	databases, err := m.client.ListDatabaseNames(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}

	var datasets []Dataset
	for _, dbName := range databases {
		// Skip system databases
		isSystemDB := false
		for _, systemDB := range config.API.SystemDatabases {
			if dbName == systemDB {
				isSystemDB = true
				break
			}
		}
		if isSystemDB {
			continue
		}

		// List collections in database
		collections, err := m.client.Database(dbName).ListCollectionNames(context.Background(), bson.M{})
		if err != nil {
			continue
		}

		for _, collName := range collections {
			datasets = append(datasets, Dataset{
				DatabaseName:       dbName,   // MongoDB database
				TableName:          collName, // MongoDB collection
				TagKeyStr:          "",
				TagValueStr:        "",
				DatasetName:        fmt.Sprintf("%s:%s", dbName, collName),
				DatasetDescription: fmt.Sprintf("Collection '%s' in database '%s'", collName, dbName),
				DatasetType:        "MongoDB",
				DatasetParams: map[string]interface{}{
					"database":   dbName,
					"collection": collName,
				},
			})
		}
	}

	return datasets, nil
}

// QueryDatabase implements DatabaseClient interface for MongoDB
func (m *MongoDBClient) QueryDatabase(params QueryParams) (map[string]interface{}, error) {
	mongoParams := params.GetMongoParams()
	collection := m.client.Database(mongoParams.Database).Collection(mongoParams.Collection)

	// Build filter
	filter := bson.M{}
	if mongoParams.TagKey != "" && mongoParams.TagValue != "" {
		filter[mongoParams.TagKey] = mongoParams.TagValue
	}

	// Find first and last records
	var sortField string = config.API.DefaultSort.MongoDB // Default to _id for consistent ordering
	if mongoParams.TimeField != "" {
		sortField = mongoParams.TimeField
	}

	firstOpts := options.FindOne().SetSort(bson.M{sortField: 1})
	lastOpts := options.FindOne().SetSort(bson.M{sortField: -1})

	var firstResult, lastResult bson.M
	err := collection.FindOne(context.Background(), filter, firstOpts).Decode(&firstResult)
	if err != nil {
		return nil, err
	}

	err = collection.FindOne(context.Background(), filter, lastOpts).Decode(&lastResult)
	if err != nil {
		return nil, err
	}

	// Count documents
	count, err := collection.CountDocuments(context.Background(), filter)
	if err != nil {
		return nil, err
	}

	// Extract columns and data
	var columns []string
	for key := range firstResult {
		if key != "_id" { // Skip MongoDB's internal _id field
			columns = append(columns, key)
		}
	}
	sort.Strings(columns)

	// Prepare data array
	data := [][]interface{}{}
	for _, result := range []bson.M{firstResult, lastResult} {
		values := make([]interface{}, len(columns))
		for i, column := range columns {
			values[i] = result[column]
		}
		data = append(data, values)
	}

	// Extract timestamps (if TimeField is provided)
	var startTime, endTime time.Time
	var startStr, endStr string

	if mongoParams.TimeField != "" {
		if t, ok := firstResult[mongoParams.TimeField].(primitive.DateTime); ok {
			startTime = t.Time()
		} else if t, ok := firstResult[mongoParams.TimeField].(time.Time); ok {
			startTime = t
		}

		if t, ok := lastResult[mongoParams.TimeField].(primitive.DateTime); ok {
			endTime = t.Time()
		} else if t, ok := lastResult[mongoParams.TimeField].(time.Time); ok {
			endTime = t
		}

		startStr = startTime.Format(time.RFC3339Nano)
		endStr = endTime.Format(time.RFC3339Nano)
	} else {
		// No time field provided, use placeholder values
		startStr = ""
		endStr = ""
	}

	responseData := map[string]interface{}{
		"start":       startStr,
		"end":         endStr,
		"bucket":      mongoParams.Database,
		"measurement": mongoParams.Collection,
		"count":       count,
		"columns":     columns,
		"data":        data,
	}

	return responseData, nil
}

// ReadDataAndSend implements DatabaseClient interface for MongoDB
func (m *MongoDBClient) ReadDataAndSend(params QueryParams, execInfo JobExecutionInfo, producer *kafka.Producer, topic string, partitionCount int) (int, error) {
	mongoParams := params.GetMongoParams()
	collection := m.client.Database(mongoParams.Database).Collection(mongoParams.Collection)

	// MongoDB는 오프셋 기반 분할 사용
	recordsToProcess := execInfo.EndOffset - execInfo.StartOffset

	// 기본 필터 (시간 범위가 있다면 적용)
	filter := bson.M{}

	// // 시간 필터 적용 (선택사항 - 전체 범위에서 오프셋으로 분할)
	// if mongoParams.TimeField != "" {
	// 	// 전체 시간 범위를 고려한 필터를 적용할 수도 있음
	// 	// 여기서는 오프셋 분할에 집중
	// }

	// 태그 필터 적용
	if mongoParams.TagKey != "" && mongoParams.TagValue != "" {
		filter[mongoParams.TagKey] = mongoParams.TagValue
	}

	// Sort 옵션 (일관성을 위해 _id 기준 정렬)
	opts := options.Find().
		SetSkip(execInfo.StartOffset). // 시작 오프셋
		SetLimit(recordsToProcess).    // 처리할 레코드 수
		SetSort(bson.M{"_id": 1})      // 일관된 정렬 보장

	// 컨텍스트 타임아웃 설정
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Database.MongoDB.ContextTimeoutSeconds)*time.Second)
	defer cancel()

	// 데이터 조회
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return 0, fmt.Errorf("failed to find documents: %v", err)
	}
	defer cursor.Close(ctx)

	totalProcessed := 0

	// 커서를 통해 문서 처리
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			log.Printf("Error decoding document: %v", err)
			continue
		}

		// MongoDB ObjectID 제거 및 데이터 정리
		dataMap := make(map[string]interface{})
		for k, v := range result {
			if k != "_id" { // MongoDB의 내부 _id 필드 제외
				dataMap[k] = v
			}
		}

		// 메타데이터 추가 (선택사항)
		dataMap["_source"] = "mongodb"
		dataMap["_collection"] = mongoParams.Collection
		dataMap["_database"] = mongoParams.Database

		// JSON으로 변환
		jsonData, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(dataMap)
		if err != nil {
			log.Printf("Error marshaling document: %v", err)
			continue
		}

		// 파티션 결정
		partition := kafka.PartitionAny
		if execInfo.SendPartition >= 0 {
			partition = int32(execInfo.SendPartition)
		} else {
			partition = int32(totalProcessed % partitionCount)
		}

		// Kafka로 메시지 전송
		if err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: partition,
			},
			Value: jsonData,
		}, nil); err != nil {
			log.Printf("Error producing message to Kafka: %v", err)
			continue
		}
		totalProcessed++
	}

	// 커서 에러 확인
	if err := cursor.Err(); err != nil {
		return totalProcessed, fmt.Errorf("cursor error: %v", err)
	}

	return totalProcessed, nil
}

// Close closes the MongoDB client
func (m *MongoDBClient) Close() error {
	return m.client.Disconnect(context.Background())
}

func (m *MongoDBClient) CalculateJobExecutions(totalRecords int64, jobCount int, params QueryParams) ([]JobExecutionInfo, error) {
	// MongoDB는 오프셋 기반 분할 방식 사용
	recordsPerJob := totalRecords / int64(jobCount)
	remainder := totalRecords % int64(jobCount)

	var execInfos []JobExecutionInfo
	var currentOffset int64 = 0

	for i := 0; i < jobCount; i++ {
		endOffset := currentOffset + recordsPerJob
		if i < int(remainder) { // 나머지를 앞쪽 Job들에 분배
			endOffset++
		}

		execInfos = append(execInfos, JobExecutionInfo{
			StartOffset: currentOffset,
			EndOffset:   endOffset,
			// Time fields는 사용하지 않음 (빈 문자열)
		})

		currentOffset = endOffset
	}

	return execInfos, nil
}
