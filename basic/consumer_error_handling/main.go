package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main is the entry point for the Kafka consumer program.
// It demonstrates setting up a Kafka consumer, handling connection issues, and processing messages from a Kafka topic.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to consume messages from
	topic := "example-topic"
	// Consumer group
	groupID := "example-group"

	// Initialize a new Kafka reader (consumer)
	// The Dialer is optional and can be used to define more connection options if necessary.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		GroupID:   groupID,
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Fatalf("Failed to close reader: %v", err)
		}
	}()

	log.Println("Starting Kafka consumer...")

	// Run forever to continuously consume messages
	for {
		// Context with timeout for each read operation
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Read message from Kafka
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if err == context.DeadlineExceeded {
				log.Println("Read timeout - retrying...")
				continue
			}
			log.Printf("Error reading message: %v", err)
			log.Println("Retrying in 5 seconds...")
			time.Sleep(5 * time.Second)
			continue
		}

		// Process the message
		log.Printf("Message received: Key=%s Value=%s", string(m.Key), string(m.Value))
		// ACK the message consumption if using Kafka with commit-log features
	}
}

// runConsumer demonstrates how the consumer can be run, handling errors including read timeouts and retries.
// It encapsulates the core logic for connecting to Kafka and processing messages.
//
// Usage Example:
//  1. Start a Kafka broker and create a topic `example-topic`.
//  2. Run this consumer to start processing messages on the above-defined topic.
