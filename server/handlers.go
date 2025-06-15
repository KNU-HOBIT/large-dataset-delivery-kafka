package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	jsoniter "github.com/json-iterator/go"
)

type ErrorResponse struct {
	Message string `json:"message"`
}

type KafkaEndPoint struct {
	bootstrapServers string
	topic            string
}

// ResponseData for API responses
type ResponseData struct {
	QStartStr       string   `json:"queryStartStr"`
	QEndStr         string   `json:"queryEndStr"`
	DatabaseStr     string   `json:"databaseStr"` // Changed from BucketStr
	TableStr        string   `json:"tableStr"`    // Changed from MeasurementStr
	TagKeyStr       string   `json:"tagKeyStr"`
	TagValueStr     string   `json:"tagValueStr"`
	KafkaBrokersStr string   `json:"kafkaBrokersStr"`
	SendTopicStr    string   `json:"sendTopicStr"`
	TotalMessages   int      `json:"totalMessages"`
	StartTime       int64    `json:"startTimeMillis"`
	EndTime         int64    `json:"endTimeMillis"`
	OffsetsData     []string `json:"offsetsData"`
}

// ConnectionRequest for registering database connections
type ConnectionRequest struct {
	ConnectionID string `json:"connection_id"`
	DBType       string `json:"db_type"`
	URL          string `json:"url"`
	Token        string `json:"token,omitempty"`    // For InfluxDB
	Org          string `json:"org,omitempty"`      // For InfluxDB
	Database     string `json:"database,omitempty"` // For MongoDB
}

// ConnectionResponse for listing connections
type ConnectionResponse struct {
	ConnectionID string `json:"connection_id"`
	DBType       string `json:"db_type"`
	Status       string `json:"status"`
}

func handleRequests() {
	// DB Connection Management APIs
	http.HandleFunc("/db-connections/register", handleDBConnectionRegister)
	http.HandleFunc("/db-connections/list", handleDBConnectionList)
	http.HandleFunc("/db-connections/remove", handleDBConnectionRemove)

	// General Database APIs (renamed from InfluxDB-centric names)
	http.HandleFunc("/data/query-by-time/", handleDataQueryByTime)    // was /data-by-time-range/
	http.HandleFunc("/data/export/", handleDataExport)                // was /all-of-data/
	http.HandleFunc("/datasets/list/", handleDatasetsList)            // was /bucket-list/
	http.HandleFunc("/datasets/detail/", handleDatasetsDetail)        // was /bucket-detail/
	http.HandleFunc("/datasets/time-range/", handleDatasetsTimeRange) // was /check-elapsed/
}

// POST /db-connections/register
func handleDBConnectionRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.ConnectionID == "" || req.DBType == "" || req.URL == "" {
		http.Error(w, "connection_id, db_type, and url are required", http.StatusBadRequest)
		return
	}

	// 연결 정보 구조체 생성
	connInfo := &DatabaseConnectionInfo{
		ConnectionID: req.ConnectionID,
		DBType:       req.DBType,
		URL:          req.URL,
		Token:        req.Token,
		Org:          req.Org,
		Database:     req.Database,
	}

	var client DatabaseClient
	var err error

	switch req.DBType {
	case "influx":
		if req.Token == "" || req.Org == "" {
			http.Error(w, "token and org are required for InfluxDB", http.StatusBadRequest)
			return
		}
		client, err = NewInfluxDBClient(req.URL, req.Token, req.Org)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to create InfluxDB client: %v", err), http.StatusInternalServerError)
			return
		}
	case "mongo":
		if req.Database == "" {
			http.Error(w, "database is required for MongoDB", http.StatusBadRequest)
			return
		}
		client, err = NewMongoDBClient(req.URL, req.Database)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to create MongoDB client: %v", err), http.StatusInternalServerError)
			return
		}
	default:
		http.Error(w, "Unsupported database type. Use 'influx' or 'mongo'", http.StatusBadRequest)
		return
	}

	// Register the connection
	connectionManager.RegisterConnection(req.ConnectionID, req.DBType, client, connInfo)

	response := map[string]interface{}{
		"message":       "Connection registered successfully",
		"connection_id": req.ConnectionID,
		"db_type":       req.DBType,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// GET /db-connections/list
func handleDBConnectionList(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	connections := []ConnectionResponse{}
	for connectionID := range connectionManager.connections {
		conn := connectionManager.connections[connectionID]
		connections = append(connections, ConnectionResponse{
			ConnectionID: connectionID,
			DBType:       conn.DBType,
			Status:       "active",
		})
	}

	response := map[string]interface{}{
		"connections": connections,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// DELETE /db-connections/remove
func handleDBConnectionRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	connectionID := r.URL.Query().Get("connection_id")
	if connectionID == "" {
		http.Error(w, "connection_id parameter is required", http.StatusBadRequest)
		return
	}

	err := connectionManager.RemoveConnection(connectionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	response := map[string]interface{}{
		"message":       "Connection removed successfully",
		"connection_id": connectionID,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// GET /datasets/time-range/ (was /check-elapsed/)
func handleDatasetsTimeRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		fmt.Fprintf(w, "unknown request")
		return
	}

	// Parse and validate parameters
	timeRangeParams, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get database connection
	conn, err := connectionManager.GetConnection(timeRangeParams.ConnectionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Build query parameters
	params, err := buildTimeRangeQueryParams(conn.DBType, timeRangeParams)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Printf("Connection ID: %s, DB Type: %s\n", timeRangeParams.ConnectionID, conn.DBType)

	// Process time range query asynchronously
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		processTimeRangeQuery(w, conn, params, timeRangeParams)
	}()
	wg.Wait()
}

type TimeRangeParams struct {
	ConnectionID  string
	Database      string
	Table         string
	TagKey        string
	TagValue      string
	MongoDatabase string
	Collection    string
	TimeField     string
}

func parseTimeRangeParams(r *http.Request) (*TimeRangeParams, error) {
	connectionID := r.URL.Query().Get("connection_id")
	if connectionID == "" {
		return nil, fmt.Errorf("connection_id is required")
	}

	// Support both old and new parameter names
	database := getParam(r, "database", "bucket")
	table := getParam(r, "table", "measurement")

	return &TimeRangeParams{
		ConnectionID:  connectionID,
		Database:      database,
		Table:         table,
		TagKey:        r.URL.Query().Get("tag_key"),
		TagValue:      r.URL.Query().Get("tag_value"),
		MongoDatabase: r.URL.Query().Get("mongo_database"),
		Collection:    r.URL.Query().Get("collection"),
		TimeField:     r.URL.Query().Get("time_field"),
	}, nil
}

func buildTimeRangeQueryParams(dbType string, params *TimeRangeParams) (QueryParams, error) {
	switch dbType {
	case "influx":
		if params.Database == "" || params.Table == "" {
			return QueryParams{}, fmt.Errorf("database and table are required for InfluxDB")
		}
		return CreateInfluxQueryParams(params.ConnectionID, params.Database, params.Table, params.TagKey, params.TagValue), nil
	case "mongo":
		mongoDb := params.MongoDatabase
		if mongoDb == "" {
			mongoDb = params.Database
		}
		if mongoDb == "" || params.Collection == "" {
			return QueryParams{}, fmt.Errorf("mongo_database and collection are required for MongoDB")
		}
		return CreateMongoQueryParams(params.ConnectionID, mongoDb, params.Collection, params.TimeField, params.TagKey, params.TagValue), nil
	default:
		return QueryParams{}, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

func processTimeRangeQuery(w http.ResponseWriter, conn *DatabaseConnection, params QueryParams, timeRangeParams *TimeRangeParams) {
	timeRange, err := conn.Client.CheckStartEndRange(params)
	if err != nil {
		log.Printf("Failed to get time range: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Printf("CheckStartEndRange: %s ~ %s\n", timeRange.startStr, timeRange.endStr)

	// Create response
	responseData := ResponseData{
		QStartStr:   timeRange.startStr,
		QEndStr:     timeRange.endStr,
		DatabaseStr: timeRangeParams.Database,
		TableStr:    timeRangeParams.Table,
		TagKeyStr:   timeRangeParams.TagKey,
		TagValueStr: timeRangeParams.TagValue,
	}

	responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(responseData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}

// GET /datasets/detail/ (was /bucket-detail/)
func handleDatasetsDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		fmt.Fprintf(w, "unknown request")
		return
	}

	// Parse and validate parameters
	detailParams, err := parseDatasetDetailParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get database connection
	conn, err := connectionManager.GetConnection(detailParams.ConnectionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Build query parameters
	params, err := buildDatasetDetailQueryParams(conn.DBType, detailParams)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Printf("Connection ID: %s, DB Type: %s\n", detailParams.ConnectionID, conn.DBType)

	// Query database
	responseData, err := conn.Client.QueryDatabase(params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(responseData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}

type DatasetDetailParams struct {
	ConnectionID  string
	Database      string
	Table         string
	TagKey        string
	TagValue      string
	MongoDatabase string
	Collection    string
	TimeField     string
}

func parseDatasetDetailParams(r *http.Request) (*DatasetDetailParams, error) {
	connectionID := r.URL.Query().Get("connection_id")
	if connectionID == "" {
		return nil, fmt.Errorf("connection_id is required")
	}

	// Support both old and new parameter names
	database := getParam(r, "database_name", "bucket_name")
	table := getParam(r, "table_name", "measurement")

	return &DatasetDetailParams{
		ConnectionID:  connectionID,
		Database:      database,
		Table:         table,
		TagKey:        r.URL.Query().Get("tag_key"),
		TagValue:      r.URL.Query().Get("tag_value"),
		MongoDatabase: r.URL.Query().Get("mongo_database"),
		Collection:    r.URL.Query().Get("collection"),
		TimeField:     r.URL.Query().Get("time_field"),
	}, nil
}

func buildDatasetDetailQueryParams(dbType string, params *DatasetDetailParams) (QueryParams, error) {
	switch dbType {
	case "influx":
		if params.Database == "" || params.Table == "" {
			return QueryParams{}, fmt.Errorf("database and table are required for InfluxDB")
		}
		return CreateInfluxQueryParams(params.ConnectionID, params.Database, params.Table, params.TagKey, params.TagValue), nil
	case "mongo":
		mongoDb := params.MongoDatabase
		if mongoDb == "" {
			mongoDb = params.Database
		}
		if mongoDb == "" || params.Collection == "" {
			return QueryParams{}, fmt.Errorf("mongo_database, collection are required for MongoDB")
		}
		return CreateMongoQueryParams(params.ConnectionID, mongoDb, params.Collection, params.TimeField, params.TagKey, params.TagValue), nil
	default:
		return QueryParams{}, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

// GET /datasets/list/ (was /bucket-list/)
func handleDatasetsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		fmt.Fprintf(w, "unknown request")
		return
	}

	connectionID := r.URL.Query().Get("connection_id")
	if connectionID == "" {
		http.Error(w, "connection_id is required", http.StatusBadRequest)
		return
	}

	// Get database connection
	conn, err := connectionManager.GetConnection(connectionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	datasets, err := conn.Client.GetDatasets()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"datasets": datasets,
	}

	responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}

// GET /data/export/ (was /all-of-data/)
func handleDataExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		fmt.Fprintf(w, "unknown request")
		return
	}

	// Parse and validate parameters
	exportParams, err := parseDataExportParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get database connection
	conn, err := connectionManager.GetConnection(exportParams.ConnectionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Build query parameters
	params, err := buildQueryParams(conn.DBType, exportParams)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Printf("Connection ID: %s, DB Type: %s\n", exportParams.ConnectionID, conn.DBType)

	// Process export asynchronously
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		processDataExport(w, conn, params, exportParams)
	}()
	wg.Wait()
}

type DataExportParams struct {
	ConnectionID  string
	Database      string
	Table         string
	TagKey        string
	TagValue      string
	KafkaBrokers  string
	SendTopic     string
	DropLast      bool
	MongoDatabase string
	Collection    string
	TimeField     string
}

func parseDataExportParams(r *http.Request) (*DataExportParams, error) {
	connectionID := r.URL.Query().Get("connection_id")
	kafkaBrokers := r.URL.Query().Get("kafka_brokers")
	sendTopic := r.URL.Query().Get("send_topic")

	if connectionID == "" || kafkaBrokers == "" || sendTopic == "" {
		return nil, fmt.Errorf("connection_id, kafka_brokers, and send_topic are required")
	}

	// Support both old and new parameter names
	database := getParam(r, "database", "bucket")
	table := getParam(r, "table", "measurement")
	dropLast := r.URL.Query().Get("drop_last") != "false" && r.URL.Query().Get("drop_last") != "F"

	return &DataExportParams{
		ConnectionID:  connectionID,
		Database:      database,
		Table:         table,
		TagKey:        r.URL.Query().Get("tag_key"),
		TagValue:      r.URL.Query().Get("tag_value"),
		KafkaBrokers:  kafkaBrokers,
		SendTopic:     sendTopic,
		DropLast:      dropLast,
		MongoDatabase: r.URL.Query().Get("mongo_database"),
		Collection:    r.URL.Query().Get("collection"),
		TimeField:     r.URL.Query().Get("time_field"),
	}, nil
}

func getParam(r *http.Request, preferred, fallback string) string {
	if value := r.URL.Query().Get(preferred); value != "" {
		return value
	}
	return r.URL.Query().Get(fallback)
}

func buildQueryParams(dbType string, params *DataExportParams) (QueryParams, error) {
	switch dbType {
	case "influx":
		if params.Database == "" || params.Table == "" {
			return QueryParams{}, fmt.Errorf("database and table are required for InfluxDB")
		}
		return CreateInfluxQueryParams(params.ConnectionID, params.Database, params.Table, params.TagKey, params.TagValue), nil
	case "mongo":
		mongoDb := params.MongoDatabase
		if mongoDb == "" {
			mongoDb = params.Database
		}
		if mongoDb == "" || params.Collection == "" {
			return QueryParams{}, fmt.Errorf("mongo_database and collection are required for MongoDB")
		}
		return CreateMongoQueryParams(params.ConnectionID, mongoDb, params.Collection, params.TimeField, params.TagKey, params.TagValue), nil
	default:
		return QueryParams{}, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

func processDataExport(w http.ResponseWriter, conn *DatabaseConnection, params QueryParams, exportParams *DataExportParams) {
	endpoint := KafkaEndPoint{
		bootstrapServers: exportParams.KafkaBrokers,
		topic:            exportParams.SendTopic,
	}

	var timeRange TimeRangeStr
	var err error

	// Get time range for InfluxDB only
	if conn.DBType == "influx" {
		timeRange, err = conn.Client.CheckStartEndRange(params)
		if err != nil {
			log.Printf("Failed to get time range: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Process jobs
	var totalMessages int
	var startTime, endTime int64
	var offsetsData []string

	// Job 기반 분할 (Job 수를 파티션에 맞춰 자동 조정하여 균등 분할)
	totalMessages, startTime, endTime, offsetsData, err = processJobs(conn.Client, timeRange, params, endpoint, exportParams.DropLast)

	if err != nil {
		log.Printf("Failed to process %s jobs: %v", conn.DBType, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create response
	responseData := ResponseData{
		QStartStr:       timeRange.startStr,
		QEndStr:         timeRange.endStr,
		DatabaseStr:     exportParams.Database,
		TableStr:        exportParams.Table,
		TagKeyStr:       exportParams.TagKey,
		TagValueStr:     exportParams.TagValue,
		KafkaBrokersStr: exportParams.KafkaBrokers,
		SendTopicStr:    exportParams.SendTopic,
		TotalMessages:   totalMessages,
		StartTime:       startTime,
		EndTime:         endTime,
		OffsetsData:     offsetsData,
	}

	responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(responseData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}

// GET /data/query-by-time/ (was /data-by-time-range/)
func handleDataQueryByTime(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		fmt.Fprintf(w, "unknown request")
		return
	}

	// Parse and validate parameters (reuse DataExportParams since they're identical)
	queryParams, err := parseDataExportParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get database connection
	conn, err := connectionManager.GetConnection(queryParams.ConnectionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Build query parameters
	params, err := buildQueryParams(conn.DBType, queryParams)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Printf("Connection ID: %s, DB Type: %s\n", queryParams.ConnectionID, conn.DBType)

	// Process query asynchronously
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		processDataQueryByTime(w, conn, params, queryParams)
	}()
	wg.Wait()
}

func processDataQueryByTime(w http.ResponseWriter, conn *DatabaseConnection, params QueryParams, queryParams *DataExportParams) {
	endpoint := KafkaEndPoint{
		bootstrapServers: queryParams.KafkaBrokers,
		topic:            queryParams.SendTopic,
	}

	// Get time range (required for both InfluxDB and MongoDB in this endpoint)
	timeRange, err := conn.Client.CheckStartEndRange(params)
	if err != nil {
		log.Printf("Failed to get time range: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Process jobs
	var totalMessages int
	var startTime, endTime int64
	var offsetsData []string

	// Job 기반 분할 (Job 수를 파티션에 맞춰 자동 조정하여 균등 분할)
	totalMessages, startTime, endTime, offsetsData, err = processJobs(conn.Client, timeRange, params, endpoint, queryParams.DropLast)

	if err != nil {
		log.Printf("Failed to process %s jobs: %v", conn.DBType, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create response
	responseData := ResponseData{
		QStartStr:       timeRange.startStr,
		QEndStr:         timeRange.endStr,
		DatabaseStr:     queryParams.Database,
		TableStr:        queryParams.Table,
		TagKeyStr:       queryParams.TagKey,
		TagValueStr:     queryParams.TagValue,
		KafkaBrokersStr: queryParams.KafkaBrokers,
		SendTopicStr:    queryParams.SendTopic,
		TotalMessages:   totalMessages,
		StartTime:       startTime,
		EndTime:         endTime,
		OffsetsData:     offsetsData,
	}

	responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(responseData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}
