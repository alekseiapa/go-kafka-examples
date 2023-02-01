package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
)

// main is the entry point of the program.
// It sets up a Kafka producer which serializes messages using Avro schema.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to send messages to
	topic := "example-avro-topic"

	// Initialize a new Kafka writer (producer)
	writer := kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Distributes messages to the partition with the least bytes
	}
	defer writer.Close()

	// Initialize Avro Codec
	codec, err := goavro.NewCodec(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "username", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`)
	if err != nil {
		log.Fatalf("Failed to create Avro codec: %v", err)
	}

	// Create Avro data to send
	native, _, err := codec.NativeFromTextual([]byte(`{"username":"john_doe","age":30}`))
	if err != nil {
		log.Fatalf("Failed to convert JSON to Avro: %v", err)
	}

	// Convert Native Go type to Binary Avro
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		log.Fatalf("Failed to encode Avro binary: %v", err)
	}

	// Create a test message to send
	message := kafka.Message{
		Key:   []byte("key-1"), // Key ensures the message always goes to the same partition
		Value: binary,
	}

	// Context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send the message
	log.Println("Sending Avro encoded message to Kafka...")
	err = writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatalf("Failed to write message to Kafka: %v", err)
	}

	log.Println("Message successfully sent to Kafka.")

	// Optional: Log completion
	fmt.Println("Kafka Avro producer setup complete. Exiting.")
}
