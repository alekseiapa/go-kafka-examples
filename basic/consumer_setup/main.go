package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// main is the entry point for the Kafka consumer program.
// It demonstrates setting up a Kafka consumer, configuring it, and reading messages from a Kafka topic.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to read messages from
	topic := "example-topic"
	// Consumer group ID
	groupID := "example-group"

	// Initialize a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	// Context to manage consumer lifecycle and handle cancellation
	ctx := context.Background()

	log.Println("Starting to consume messages...")

	// Infinite loop to continuously read messages from the topic
	for {
		// Read messages from the Kafka topic
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("failed to read message: %v", err)
		}

		// Log the message details
		fmt.Printf("Received message at topic/partition/offset %v/%v/%v: %s = %s\n",
			message.Topic, message.Partition, message.Offset,
			string(message.Key), string(message.Value))
	}
}
