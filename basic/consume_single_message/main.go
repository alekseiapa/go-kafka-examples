package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// main is the entry point for the Kafka consumer program.
// It demonstrates consuming a single message from a Kafka topic.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to consume messages from
	topic := "example-topic"
	// Partition to consume from
	partition := 0

	// Initialize a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		Partition: partition,
		// Set the offset to read from the first offset.
		// This can be changed to kafka.LastOffset to read the most recent message.
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	// Context with cancellation to allow cancellation of the read operation
	ctx := context.Background()

	// Consume a single message from Kafka
	log.Println("Consuming a single message from Kafka...")
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		log.Fatalf("Failed to read message from Kafka: %v", err)
	}

	// Print the consumed message key and value
	fmt.Printf("Message consumed: key=%s value=%s\n", string(message.Key), string(message.Value))

	// Optional: Log completion
	fmt.Println("Kafka message consumption complete. Exiting.")
}
