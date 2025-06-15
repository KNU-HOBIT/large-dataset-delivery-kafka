# 🚀 Large Dataset Delivery with Kafka

[![Go Version](https://img.shields.io/badge/Go-1.19+-00ADD8?style=for-the-badge&logo=go)](https://golang.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka)](https://kafka.apache.org/)
[![InfluxDB](https://img.shields.io/badge/InfluxDB-22ADF6?style=for-the-badge&logo=influxdb)](https://www.influxdata.com/)
[![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb)](https://www.mongodb.com/)

A high-performance, scalable data streaming platform that efficiently delivers large datasets from multiple database sources (InfluxDB, MongoDB) to Apache Kafka with configurable parallelism and robust connection management.

## 🚀 Features

- **Multi-Database Support**: Seamlessly integrates with both InfluxDB and MongoDB
- **High-Performance Data Streaming**: Kafka-based message queuing with configurable parallelism
- **Dynamic Connection Management**: Runtime database connection registration and management
- **Scalable Worker Pool**: Configurable worker pools for optimal throughput
- **RESTful API**: Clean REST endpoints for data export and system management
- **Configurable Architecture**: Externalized configuration for easy deployment across environments

## 📋 Table of Contents

- [Architecture](#-architecture)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [API Endpoints](#-api-endpoints)
- [Usage Examples](#-usage-examples)
- [Development](#-development)
- [Contributing](#-contributing)

## 🏗 Architecture

The system consists of two main components:

### Server Component
- **Data Processing Engine**: Handles database queries and Kafka message production
- **Connection Manager**: Manages multiple database connections dynamically
- **Worker Pool**: Distributes workload across configurable number of workers
- **REST API**: Provides endpoints for data export and system management

### Client Component
- **Kafka Consumer**: Consumes messages from Kafka topics
- **HTTP Client**: Makes requests to external services
- **Signal Handling**: Graceful shutdown and signal management

## 📦 Installation

### Prerequisites

- Go 1.19 or higher
- Apache Kafka cluster
- InfluxDB and/or MongoDB instances

### Dependencies

```bash
go mod init github.com/noFlowWater/large-dataset-delivery-kafka
go get github.com/confluentinc/confluent-kafka-go/kafka
go get github.com/influxdata/influxdb-client-go/v2
go get github.com/json-iterator/go
go get go.mongodb.org/mongo-driver/mongo
```

### Build

```bash
# Server
cd server
go build -o large-dataset-server .

# Client
cd client
go build -o large-dataset-client .
```

## ⚙️ Configuration

### Server Configuration

Create a `config.json` file in the server directory:

```json
{
  "server": {
    "port": ":3001",
    "shutdownTimeoutSeconds": 5
  },
  "kafka": {
    "bootstrapServers": "broker1:9092,broker2:9092,broker3:9092",
    "acks": "all",
    "enableIdempotence": "true",
    "compressionType": "lz4",
    "metadataTimeoutMs": 5000,
    "watermarkTimeoutMs": 1000,
    "flushTimeoutMs": 15000
  },
  "jobs": {
    "workerNum": 6,
    "jobQueueCapacity": 100,
    "dividedJobs": 48,
    "jobsPerPartition": 4
  },
  "database": {
    "mongodb": {
      "maxPoolSize": 100,
      "maxConnIdleTimeSeconds": 30,
      "connectTimeoutSeconds": 10,
      "socketTimeoutSeconds": 30,
      "serverSelectionTimeoutSeconds": 5,
      "contextTimeoutSeconds": 30,
      "queryTimeoutSeconds": 5
    },
    "influxdb": {
      "httpRequestTimeoutSeconds": 900,
      "precisionUnit": "ms",
      "queryTimeoutSeconds": 5
    }
  },
  "formats": {
    "dateTimeFormat": "2006-01-02T15:04:05.000Z",
    "rfc3339NanoFormat": "2006-01-02T15:04:05.000000000Z07:00"
  },
  "api": {
    "defaultSort": {
      "mongodb": "_id",
      "influxdb": "_time"
    },
    "systemDatabases": ["admin", "local", "config"]
  }
}
```

#### Configuration Sections

| Section | Description |
|---------|-------------|
| `server` | HTTP server settings and shutdown timeout |
| `kafka` | Kafka producer configuration and timeouts |
| `jobs` | Worker pool and job processing settings |
| `database` | Database-specific connection and timeout settings |
| `formats` | Date/time format strings for data processing |
| `api` | API behavior defaults and system database exclusions |

### Client Configuration

Create a `config.yaml` file in the client directory:

```yaml
bootstrapServers: "broker1:9092,broker2:9092,broker3:9092"
consumerGroup: "data-processing-group"
kafkaTopic: "iot-data"
numWorkers: 4
startTimeStr: "2023-01-01T00:00:00Z"
endTimeStr: "2023-12-31T23:59:59Z"
equipmentID: "sensor-001"
httpRequestURLFmt: "http://analytics-service:8080/?start=%s&end=%s&eqp_id=%s"
maxMessageQueueSize: 25928778
brokerAddressFamily: "v4"
autoOffsetReset: "earliest"
```

## 🔌 API Endpoints

### Database Connection Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/db-connections/register` | Register a new database connection |
| `GET` | `/db-connections/list` | List all registered connections |
| `DELETE` | `/db-connections/remove` | Remove a database connection |

### Data Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/data/query-by-time/` | Query data within time range |
| `GET` | `/data/export/` | Export all data to Kafka |
| `GET` | `/datasets/list/` | List available datasets |
| `GET` | `/datasets/detail/` | Get dataset details |
| `GET` | `/datasets/time-range/` | Get dataset time range |

### Example: Register Database Connection

```bash
# Register InfluxDB connection
curl -X POST http://localhost:3001/db-connections/register \
  -H "Content-Type: application/json" \
  -d '{
    "connection_id": "influx-prod",
    "db_type": "influx",
    "url": "http://influxdb:8086",
    "token": "your-influx-token",
    "org": "your-org"
  }'

# Register MongoDB connection
curl -X POST http://localhost:3001/db-connections/register \
  -H "Content-Type: application/json" \
  -d '{
    "connection_id": "mongo-prod",
    "db_type": "mongo",
    "url": "mongodb://mongodb:27017",
    "database": "iot_data"
  }'
```

## 📊 Usage Examples

### Export Data to Kafka

```bash
curl "http://localhost:3001/data/export/?connection_id=influx-prod&database=telegraf&table=cpu&kafka_brokers=broker1:9092&send_topic=cpu-data"
```

### Query Time-Range Data

```bash
curl "http://localhost:3001/data/query-by-time/?connection_id=mongo-prod&mongo_database=sensors&collection=temperature&time_field=timestamp"
```

## 🛠 Development

### Project Structure

```
