
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main function acts as the entry point of our application.
// It configures a Kafka consumer to listen to messages and implements a dead-letter queue mechanism.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"

	// Topics
	sourceTopic := "main-topic"
	deadLetterTopic := "dead-letter-queue"

	// Initialize Kafka reader configuration
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     sourceTopic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	// Ensure reader cleanup on function exit
	defer r.Close()

	// Initialize Kafka writer for the dead-letter queue
	deadLetterWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   deadLetterTopic,
	})

	// Ensure writer cleanup on function exit
	defer deadLetterWriter.Close()

	// Context with a timeout to control the reading process
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		// Read messages
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("Failed to read message: %v", err)
		}

		// Simulate message processing
		if !processMessage(m) {
			// Message processing failed, send to dead-letter queue
			log.Printf("Failed to process message %s, sending to dead-letter queue", string(m.Value))
			dlErr := deadLetterWriter.WriteMessages(context.Background(), kafka.Message{
				Key:   m.Key,
				Value: m.Value,
			})
			if dlErr != nil {
				log.Printf("Failed to write message to dead-letter queue: %v", dlErr)
			} else {
				log.Printf("Message sent to dead-letter queue: %s", string(m.Value))
			}
		}
	}
}

// processMessage simulates processing a Kafka message.
// Returns false to indicate processing failure, true for success.
func processMessage(m kafka.Message) bool {
	fmt.Printf("Processing message: %s\n", string(m.Value))
	// Simulated processing logic
	return false // Assume failure for demonstration purposes
}
