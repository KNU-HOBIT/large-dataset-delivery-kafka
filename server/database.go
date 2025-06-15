package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// TimeRangeStr holds start and end time strings
type TimeRangeStr struct {
	startStr string
	endStr   string
}

// Dataset represents a dataset structure
type Dataset struct {
	DatabaseName       string                 `json:"database_name"` // InfluxDB: bucket, MongoDB: database
	TableName          string                 `json:"table_name"`    // InfluxDB: measurement, MongoDB: collection
	TagKeyStr          string                 `json:"tagKeyStr"`
	TagValueStr        string                 `json:"tagValueStr"`
	DatasetName        string                 `json:"dataset_name"`
	DatasetDescription string                 `json:"dataset_description"`
	DatasetType        string                 `json:"dataset_type"`
	DatasetParams      map[string]interface{} `json:"dataset_params"`
}

// QueryParams holds unified parameters for database queries
type QueryParams struct {
	// Connection information
	ConnectionID string
	DBType       string

	// Database-specific parameters
	Influx *InfluxQueryParams
	Mongo  *MongoQueryParams
}

// InfluxQueryParams holds InfluxDB-specific parameters
type InfluxQueryParams struct {
	URL         string
	Token       string
	Org         string
	Bucket      string
	Measurement string
	TagKey      string
	TagValue    string
}

// MongoQueryParams holds MongoDB-specific parameters
type MongoQueryParams struct {
	Database   string
	Collection string
	TimeField  string
	TagKey     string
	TagValue   string
}

// GetInfluxParams returns InfluxDB parameters or creates them from legacy fields
func (q *QueryParams) GetInfluxParams() *InfluxQueryParams {
	if q.Influx != nil {
		return q.Influx
	}
	// For backward compatibility, return empty struct
	return &InfluxQueryParams{}
}

// GetMongoParams returns MongoDB parameters or creates them from legacy fields
func (q *QueryParams) GetMongoParams() *MongoQueryParams {
	if q.Mongo != nil {
		return q.Mongo
	}
	// For backward compatibility, return empty struct
	return &MongoQueryParams{}
}

// CreateInfluxQueryParams creates a QueryParams with InfluxDB parameters
func CreateInfluxQueryParams(connectionID, bucket, measurement, tagKey, tagValue string) QueryParams {
	return QueryParams{
		ConnectionID: connectionID,
		DBType:       "influx",
		Influx: &InfluxQueryParams{
			Bucket:      bucket,
			Measurement: measurement,
			TagKey:      tagKey,
			TagValue:    tagValue,
		},
	}
}

// CreateMongoQueryParams creates a QueryParams with MongoDB parameters
func CreateMongoQueryParams(connectionID, database, collection, timeField, tagKey, tagValue string) QueryParams {
	return QueryParams{
		ConnectionID: connectionID,
		DBType:       "mongo",
		Mongo: &MongoQueryParams{
			Database:   database,
			Collection: collection,
			TimeField:  timeField,
			TagKey:     tagKey,
			TagValue:   tagValue,
		},
	}
}

// DatabaseClient interface for database operations
type DatabaseClient interface {
	CheckStartEndRange(params QueryParams) (TimeRangeStr, error)
	GetTotalRecordCount(timeRange TimeRangeStr, params QueryParams) (int64, error)
	CalculateEndTimes(startStr string, recordsPerJob int64, params QueryParams) ([]string, error)
	CalculateJobExecutions(totalRecords int64, jobCount int, params QueryParams) ([]JobExecutionInfo, error)
	GetDatasets() ([]Dataset, error)
	QueryDatabase(params QueryParams) (map[string]interface{}, error)
	ReadDataAndSend(params QueryParams, execInfo JobExecutionInfo, producer *kafka.Producer, topic string, partitionCount int) (int, error)
	Close() error
}

// DatabaseConnectionInfo holds connection metadata
type DatabaseConnectionInfo struct {
	ConnectionID string
	DBType       string
	URL          string
	// InfluxDB specific
	Token string
	Org   string
	// MongoDB specific
	Database string
}

// DatabaseConnection holds connection information and client
type DatabaseConnection struct {
	ConnectionID string
	DBType       string
	Client       DatabaseClient
	Info         *DatabaseConnectionInfo // 연결 정보 추가
}

// ConnectionManager manages database connections
type ConnectionManager struct {
	connections map[string]*DatabaseConnection
	connInfos   map[string]*DatabaseConnectionInfo // 연결 정보만 저장
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: make(map[string]*DatabaseConnection),
		connInfos:   make(map[string]*DatabaseConnectionInfo),
	}
}

// RegisterConnection registers a new database connection
func (cm *ConnectionManager) RegisterConnection(connectionID, dbType string, client DatabaseClient, info *DatabaseConnectionInfo) {
	cm.connections[connectionID] = &DatabaseConnection{
		ConnectionID: connectionID,
		DBType:       dbType,
		Client:       client,
		Info:         info,
	}
	cm.connInfos[connectionID] = info
}

// GetConnection retrieves a database connection
func (cm *ConnectionManager) GetConnection(connectionID string) (*DatabaseConnection, error) {
	conn, exists := cm.connections[connectionID]
	if !exists {
		return nil, fmt.Errorf("connection with ID '%s' not found", connectionID)
	}
	return conn, nil
}

// ListConnections returns all registered connections
func (cm *ConnectionManager) ListConnections() []string {
	var connections []string
	for id := range cm.connections {
		connections = append(connections, id)
	}
	return connections
}

// RemoveConnection removes a database connection
func (cm *ConnectionManager) RemoveConnection(connectionID string) error {
	conn, exists := cm.connections[connectionID]
	if !exists {
		return fmt.Errorf("connection with ID '%s' not found", connectionID)
	}

	// Close the connection
	if err := conn.Client.Close(); err != nil {
		return fmt.Errorf("error closing connection: %v", err)
	}

	delete(cm.connections, connectionID)
	return nil
}

// GetConnectionInfo retrieves connection info for creating new clients
func (cm *ConnectionManager) GetConnectionInfo(connectionID string) (*DatabaseConnectionInfo, error) {
	info, exists := cm.connInfos[connectionID]
	if !exists {
		return nil, fmt.Errorf("connection info with ID '%s' not found", connectionID)
	}
	return info, nil
}

// Global connection manager instance
var connectionManager = NewConnectionManager()

// TODO: Implement SQLite persistence for connections
// This would replace the in-memory storage with persistent storage
