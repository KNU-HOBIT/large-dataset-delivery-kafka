# large-dataset-delivery-kafka
Scalable IoT data processing system for high-volume data transfer, utilizing Go, Kafka, and InfluxDB for efficient real-time analytics.


```
go mod init github.com/noFlowWater/large-dataset-delivery-kafka
```
```
go get github.com/confluentinc/confluent-kafka-go/kafka
```
```
go get github.com/influxdata/influxdb-client-go/v2
```
```
go get github.com/json-iterator/go
```


## Configuration File 
The application requires a `config.json`, `config.yaml` file that contains all necessary configurations. Below is a template of what this file should look like. Replace the example values with your actual configuration details.

### Server
```
server/
├── config.go
├── ``config.json``
├── dispatcher.go
├── go.mod
├── go.sum
├── handlers.go
├── influx.go
├── jobs.go
├── main.go
├── utils.go
└── workers.go
```
Structure of `config.json`
```json
{
  "server": {
    "port": ":<port_number>"
  },
  "kafka": {
    "bootstrapServers": "<ip_address1>:<port1>,<ip_address2>:<port2>,<ip_address3>:<port3>",
    "acks": "all",
    "enableIdempotence": "true",
    "compressionType": "lz4"
  },
  "influxDB": {
    "url": "http://<ip_address>:<port>",
    "token": "<access_token>"
  },
  "jobs": {
    "workerNum": 16,
    "jobQueueCapacity": 100,
    "dividedJobs": 48
  },
  "topics": {
    "default": "<topic_name>"
  }
}
```

### Client
```
client/
├── config
│   └── config.go
├── ``config.yaml``
├── go.mod
├── go.sum
├── kafka
│   └── consumer.go
├── main.go
├── myhttp
│   └── client.go
├── shared
│   └── shared.go
└── util
    └── signal.go
```
Structure of `config.yaml`
```yaml
bootstrapServers: "<ip1>:<port1>,<ip2>:<port2>,<ip3>:<port3>"
consumerGroup: "<consumer_group_name>"
kafkaTopic: "<topic_name>"
numWorkers: <number_of_workers>
startTimeStr: "<start_time_iso8601>"
endTimeStr: "<end_time_iso8601>"
equipmentID: "<equipment_identifier>"
httpRequestURLFmt: "http://<ip>:<port>/?start=%s&end=%s&eqp_id=%s"
maxMessageQueueSize: 25928778
brokerAddressFamily: "v4"
autoOffsetReset: "earliest"
```
