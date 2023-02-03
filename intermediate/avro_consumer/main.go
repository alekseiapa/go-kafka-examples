package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
)

// main is the entry point for the Kafka Avro consumer program.
// It demonstrates consuming Avro-serialized messages from a Kafka topic and decoding them.
func main() {
	// Kafka and Schema Registry connection configurations.
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "example-group",
		"auto.offset.reset": "earliest",
	}

	// Initialize a new consumer instance.
	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Subscribe to the Kafka topic from which messages need to be consumed.
	topic := "example-topic"
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %v", err)
	}

	// Set up signal handling to gracefully shut down the consumer.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Infinite loop to read messages from the Kafka topic.
	for {
		select {
		// Handle OS signals to gracefully terminate the consumer.
		case sig := <-sigChan:
			log.Printf("Caught signal %v: terminating", sig)
			return

		// Poll for a new message from the Kafka topic.
		default:
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				log.Printf("Consumer error: %v", err)
				continue
			}

			// Process the received message, which is serialized with Avro.
			processMessage(msg)
		}
	}
}

// processMessage deserializes the Avro message and logs the resulting data.
func processMessage(msg *kafka.Message) {
	// Define the Avro schema for decoding.
	schema := `{
		"type": "record",
		"name": "example_record",
		"fields": [
			{"name": "field1", "type": "string"},
			{"name": "field2", "type": "int"}
		]
	}`

	// Create a new Avro codec with the schema.
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Printf("Error creating Avro codec: %v", err)
		return
	}

	// Decode the Avro message.
	decoded, _, err := codec.NativeFromBinary(msg.Value)
	if err != nil {
		log.Printf("Failed to decode Avro message: %v", err)
		return
	}

	// Log the received message.
	log.Printf("Received message on %s: %v", msg.TopicPartition, decoded)
}
