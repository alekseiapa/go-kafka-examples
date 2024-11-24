# 📦 Kafka Examples in Golang

This repository contains a collection of Kafka examples implemented in Golang. This collection provides a variety of examples for using Kafka with Golang, ranging from basic streaming pipelines to advanced features like distributed tracing and monitoring.

# 🗂️ Project Structure

The repository is organized as follows:

```bash
go-kafka-examples/
│
├── 📄 README.md                        # Documentation for the repository
├── ⚙️ Makefile                         # Makefile to simplify running examples
│
├── 🟢 basic/                           # Basic Kafka examples
│   ├── 📂 producer_setup/
│   │   └── 📄 main.go                  # Set up a Kafka producer
│   ├── 📂 consumer_setup/
│   │   └── 📄 main.go                  # Set up a Kafka consumer
│   ├── 📂 send_string_message/
│   │   └── 📄 main.go                  # Send a string message to Kafka
│   ├── 📂 consume_single_message/
│   │   └── 📄 main.go                  # Consume a single message from Kafka
│   ├── 📂 producer_config/
│   │   └── 📄 main.go                  # Configure a Kafka producer
│   ├── 📂 consumer_group_config/
│   │   └── 📄 main.go                  # Configure a Kafka consumer group
│   ├── 📂 producer_error_handling/
│   │   └── 📄 main.go                  # Handle errors in Kafka producer
│   ├── 📂 consumer_error_handling/
│   │   └── 📄 main.go                  # Handle errors in Kafka consumer
│   ├── 📂 produce_json_messages/
│   │   └── 📄 main.go                  # Produce JSON messages to Kafka
│   └── 📂 consume_json_messages/
│       └── 📄 main.go                  # Consume JSON messages from Kafka
│
├── 🟡 intermediate/                    # Intermediate Kafka examples
│   ├── 📂 producer_retry_logic/
│   │   └── 📄 main.go                  # Retry logic for Kafka producers
│   ├── 📂 concurrent_message_processing/
│   │   └── 📄 main.go                  # Concurrent message processing
│   ├── 📂 kafka_headers/
│   │   └── 📄 main.go                  # Use Kafka headers for metadata
│   ├── 📂 message_partitioning/
│   │   └── 📄 main.go                  # Partition Kafka messages by key
│   ├── 📂 custom_partitioners/
│   │   └── 📄 main.go                  # Implement custom partitioners
│   ├── 📂 avro_producer/
│   │   └── 📄 main.go                  # Produce messages using Avro
│   ├── 📂 avro_consumer/
│   │   └── 📄 main.go                  # Consume messages using Avro
│   ├── 📂 protobuf_serialization/
│   │   └── 📄 main.go                  # Use Protocol Buffers for serialization
│   ├── 📂 exactly_once_transactions/
│   │   └── 📄 main.go                  # Implement exactly-once semantics
│   └── 📂 backoff_retries_consumer/
│       └── 📄 main.go                  # Backoff retry logic for consumers
│
├── 🔵 streaming/                       # Streaming Kafka examples
│   ├── 📂 stream_to_database/
│   │   └── 📄 main.go                  # Stream data to a database
│   ├── 📂 metrics_pipeline/
│   │   └── 📄 main.go                  # Real-time metrics pipeline
│   ├── 📂 message_transformation/
│   │   └── 📄 main.go                  # Transform Kafka messages
│   ├── 📂 topic_joining/
│   │   └── 📄 main.go                  # Join data from multiple topics
│   ├── 📂 message_filtering/
│   │   └── 📄 main.go                  # Filter Kafka messages
│   ├── 📂 real_time_aggregation/
│   │   └── 📄 main.go                  # Perform real-time aggregation
│   ├── 📂 log_processing_pipeline/
│   │   └── 📄 main.go                  # Process logs from Kafka
│   ├── 📂 real_time_chat/
│   │   └── 📄 main.go                  # Real-time chat application
│   ├── 📂 video_metadata_processing/
│   │   └── 📄 main.go                  # Process video metadata
│   └── 📂 message_to_rest_api/
│       └── 📄 main.go                  # Send Kafka messages to a REST API
│
├── 🔴 advanced/                        # Advanced Kafka examples
│   ├── 📂 schema_registry_integration/
│   │   └── 📄 main.go                  # Integrate with a schema registry
│   ├── 📂 dead_letter_queue/
│   │   └── 📄 main.go                  # Implement a dead-letter queue
│   ├── 📂 distributed_tracing/
│   │   └── 📄 main.go                  # Implement distributed tracing
│   ├── 📂 event_sourcing/
│   │   └── 📄 main.go                  # Build an event sourcing system
│   ├── 📂 large_message_handling/
│   │   └── 📄 main.go                  # Handle large messages in Kafka
│   ├── 📂 manual_offset_management/
│   │   └── 📄 main.go                  # Manage offsets manually
│   ├── 📂 fault_tolerant_consumer/
│   │   └── 📄 main.go                  # Build a fault-tolerant consumer
│   ├── 📂 kafka_streams_api/
│   │   └── 📄 main.go                  # Use Kafka Streams API
│   ├── 📂 windowed_aggregations/
│   │   └── 📄 main.go                  # Perform windowed aggregations
│   └── 📂 backpressure_handling/
│       └── 📄 main.go                  # Handle backpressure in consumers
│
├── 🟠 monitoring_testing/              # Monitoring and testing Kafka pipelines
│   ├── 📂 lag_monitoring/
│   │   └── 📄 main.go                  # Monitor Kafka consumer lag
│   ├── 📂 producer_mock_testing/
│   │   └── 📄 main.go                  # Test Kafka producers with mocks
│   ├── 📂 consumer_mock_testing/
│   │   └── 📄 main.go                  # Test Kafka consumers with mocks
│   ├── 📂 metrics_exporter/
│   │   └── 📄 main.go                  # Export Kafka metrics to Prometheus
│   ├── 📂 grafana_integration/
│   │   └── 📄 main.go                  # Integrate Kafka metrics with Grafana
│   ├── 📂 high_throughput_simulation/
│   │   └── 📄 main.go                  # Simulate high-throughput scenarios
│   ├── 📂 unit_tests_consumer_logic/
│   │   └── 📄 main.go                  # Unit test Kafka consumer logic
│   ├── 📂 integration_tests_pipelines/
│   │   └── 📄 main.go                  # Integration tests for pipelines
│   ├── 📂 producer_benchmarking/
│   │   └── 📄 main.go                  # Benchmark Kafka producer performance
│   └── 📂 consumer_benchmarking/
│       └── 📄 main.go                  # Benchmark Kafka consumer performance

```


# 📦 Kafka Examples in Golang

## 🟢 Basic Examples
1. **Producer Setup**  
   *[Path](basic/producer_setup/main.go)*  
   *Description*: Set up a Kafka producer in Golang to configure broker addresses, create a producer instance, and send a test message.

2. **Consumer Setup**  
   *[Path](basic/consumer_setup/main.go)*  
   *Description*: Set up a Kafka consumer to connect to a topic, read messages, and manage offsets effectively.

3. **Send String Message**  
   *[Path](basic/send_string_message/main.go)*  
   *Description*: Send a simple string message to a Kafka topic using basic producer functionality.

4. **Consume Single Message**  
   *[Path](basic/consume_single_message/main.go)*  
   *Description*: Consume a single message from a Kafka topic to demonstrate message consumption basics.

5. **Producer Config**  
   *[Path](basic/producer_config/main.go)*  
   *Description*: Configure a Kafka producer with advanced settings like batch size, compression, and retries for optimized performance.

6. **Consumer Group Config**  
   *[Path](basic/consumer_group_config/main.go)*  
   *Description*: Set up a Kafka consumer group with configurations such as group ID and auto-offset reset for multiple consumers.

7. **Producer Error Handling**  
   *[Path](basic/producer_error_handling/main.go)*  
   *Description*: Handle errors in Kafka producers by logging issues and retrying message delivery.

8. **Consumer Error Handling**  
   *[Path](basic/consumer_error_handling/main.go)*  
   *Description*: Gracefully handle errors in Kafka consumers, such as connection issues or deserialization failures.

9. **Produce JSON Messages**  
   *[Path](basic/produce_json_messages/main.go)*  
   *Description*: Send structured JSON messages to a Kafka topic by serializing data appropriately.

10. **Consume JSON Messages**  
    *[Path](basic/consume_json_messages/main.go)*  
    *Description*: Read and deserialize JSON messages from a Kafka topic to process structured data.

---

## 🟡 Intermediate Examples
1. **Producer Retry Logic**  
   *[Path](intermediate/producer_retry_logic/main.go)*  
   *Description*: Implement retry logic for Kafka producers to ensure reliable message delivery during transient failures.

2. **Concurrent Message Processing**  
   *[Path](intermediate/concurrent_message_processing/main.go)*  
   *Description*: Write a Kafka consumer to process messages concurrently, improving throughput for high-volume systems.

3. **Kafka Headers**  
   *[Path](intermediate/kafka_headers/main.go)*  
   *Description*: Use Kafka headers to attach metadata like tracing IDs, useful for downstream processing or filtering.

4. **Message Partitioning**  
   *[Path](intermediate/message_partitioning/main.go)*  
   *Description*: Partition Kafka messages by key to ensure related messages are sent to the same partition for ordered processing.

5. **Custom Partitioners**  
   *[Path](intermediate/custom_partitioners/main.go)*  
   *Description*: Implement custom partitioning logic to control message distribution across Kafka partitions.

6. **Avro Producer**  
   *[Path](intermediate/avro_producer/main.go)*  
   *Description*: Serialize messages using Avro schema for compact and structured message encoding.

7. **Avro Consumer**  
   *[Path](intermediate/avro_consumer/main.go)*  
   *Description*: Consume and deserialize Avro messages, integrating with a schema registry for validation.

8. **Protobuf Serialization**  
   *[Path](intermediate/protobuf_serialization/main.go)*  
   *Description*: Use Protocol Buffers (protobuf) for high-performance message serialization and deserialization.

9. **Exactly Once Transactions**  
   *[Path](intermediate/exactly_once_transactions/main.go)*  
   *Description*: Use Kafka transactions to achieve exactly-once message processing semantics.

10. **Backoff Retries Consumer**  
    *[Path](intermediate/backoff_retries_consumer/main.go)*  
    *Description*: Implement backoff retry logic for Kafka consumers to handle transient errors during processing.

---

## 🔵 Streaming Examples
1. **Stream to Database**  
   *[Path](streaming/stream_to_database/main.go)*  
   *Description*: Read streaming data from Kafka and save it to a database for real-time data integration.

2. **Metrics Pipeline**  
   *[Path](streaming/metrics_pipeline/main.go)*  
   *Description*: Build a real-time metrics pipeline to collect, process, and display metrics from Kafka messages.

3. **Message Transformation**  
   *[Path](streaming/message_transformation/main.go)*  
   *Description*: Modify the structure or content of messages in Kafka for downstream consumers.

4. **Topic Joining**  
   *[Path](streaming/topic_joining/main.go)*  
   *Description*: Join data from multiple Kafka topics to create enriched messages or combined datasets.

5. **Message Filtering**  
   *[Path](streaming/message_filtering/main.go)*  
   *Description*: Filter Kafka messages based on content, forwarding only relevant ones to a downstream topic.

6. **Real-Time Aggregation**  
   *[Path](streaming/real_time_aggregation/main.go)*  
   *Description*: Aggregate streaming data in real time, computing on-the-fly statistics or summaries.

7. **Log Processing Pipeline**  
   *[Path](streaming/log_processing_pipeline/main.go)*  
   *Description*: Process and analyze logs from Kafka topics, forwarding them to a storage or visualization system.

8. **Real-Time Chat**  
   *[Path](streaming/real_time_chat/main.go)*  
   *Description*: Implement a chat application using Kafka as a messaging backend to handle high-frequency communication.

9. **Video Metadata Processing**  
   *[Path](streaming/video_metadata_processing/main.go)*  
   *Description*: Process video metadata in real time to extract or transform video-related information.

10. **Message to REST API**  
    *[Path](streaming/message_to_rest_api/main.go)*  
    *Description*: Send Kafka messages to a REST API for further processing or storage.

---

## 🔴 Advanced Examples
1. **Schema Registry Integration**  
   *[Path](advanced/schema_registry_integration/main.go)*  
   *Description*: Integrate Kafka with a schema registry to validate and deserialize Avro or Protobuf messages.

2. **Dead Letter Queue**  
   *[Path](advanced/dead_letter_queue/main.go)*  
   *Description*: Handle undeliverable Kafka messages by routing them to a dead-letter queue for debugging and recovery.

3. **Distributed Tracing**  
   *[Path](advanced/distributed_tracing/main.go)*  
   *Description*: Implement distributed tracing to monitor and troubleshoot Kafka message flow across services.

4. **Event Sourcing**  
   *[Path](advanced/event_sourcing/main.go)*  
   *Description*: Build an event-sourcing system using Kafka to store and replay events for state reconstruction.

5. **Large Message Handling**  
   *[Path](advanced/large_message_handling/main.go)*  
   *Description*: Handle large messages in Kafka using compression and storage optimization techniques.

6. **Manual Offset Management**  
   *[Path](advanced/manual_offset_management/main.go)*  
   *Description*: Implement manual offset management to control message acknowledgment and processing.

7. **Fault-Tolerant Consumer**  
   *[Path](advanced/fault_tolerant_consumer/main.go)*  
   *Description*: Build a fault-tolerant Kafka consumer that recovers from failures without losing messages.

8. **Kafka Streams API**  
   *[Path](advanced/kafka_streams_api/main.go)*  
   *Description*: Use the Kafka Streams API to process and analyze streaming data with advanced stream transformations.

9. **Windowed Aggregations**  
   *[Path](advanced/windowed_aggregations/main.go)*  
   *Description*: Perform windowed aggregations on Kafka streams, such as time-based or sliding-window computations.

10. **Backpressure Handling**  
    *[Path](advanced/backpressure_handling/main.go)*  
    *Description*: Handle backpressure in Kafka consumers to prevent overwhelming downstream systems during high load.

---

## 🟠 Monitoring and Testing
1. **Lag Monitoring**  
   *[Path](monitoring_testing/lag_monitoring/main.go)*  
   *Description*: Monitor Kafka consumer lag to track unprocessed messages and ensure timely consumption.

2. **Producer Mock Testing**  
   *[Path](monitoring_testing/producer_mock_testing/main.go)*  
   *Description*: Test Kafka producer functionality using mock brokers to simulate real-world scenarios.

3. **Consumer Mock Testing**  
   *[Path](monitoring_testing/consumer_mock_testing/main.go)*  
   *Description*: Validate Kafka consumer behavior with mock brokers to test message processing logic.

4. **Metrics Exporter**  
   *[Path](monitoring_testing/metrics_exporter/main.go)*  
   *Description*: Build a custom Kafka metrics exporter to send consumer and producer statistics to Prometheus.

5. **Grafana Integration**  
   *[Path](monitoring_testing/grafana_integration/main.go)*  
   *Description*: Integrate Kafka metrics with Grafana for real-time visualization of performance metrics.

6. **High Throughput Simulation**  
   *[Path](monitoring_testing/high_throughput_simulation/main.go)*  
   *Description*: Simulate high-throughput scenarios to test producer


# 🚀 Running Examples

# 🛠️ Prerequisites

- Install Go.

- Clone the repo:
```
git clone https://github.com/alekseiapa/go-kafka-examples
cd go-kafka-examples
```

- Set up a Kafka broker. For example, you can use Docker:

```bash
docker run -d --name kafka -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=localhost -e KAFKA_ZOOKEEPER_CONNECT=localhost:2181 spotify/kafka
```

- Create the required Kafka topics. For example:

```bash
kafka-topics.sh --create --topic chat-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

- Use the provided Makefile to run examples:

```bash
make example=basic/consume_json_messages run
```

# ⚙️ Running Examples Using a Makefile
To simplify running examples, use the provided Makefile.

**Makefile:** 

```makefile
run:
@cd $(example) && go run main.go

.PHONY: run
```

## Run an Example
To run an example, use the make command with the example argument pointing to the example path:

```
make example=streaming/metrics_pipeline run
```