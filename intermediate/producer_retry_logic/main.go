package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// maxRetries defines the number of retry attempts for message sending.
const maxRetries = 5

// retryInterval defines the duration to wait between retries.
const retryInterval = time.Second * 2

func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to send messages to
	topic := "example-topic"

	// Initialize a Kafka writer (producer) using the latest API
	writer := kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Balances the load by writing to the partition with the least data
	}
	defer writer.Close()

	// Create a test message to send
	message := kafka.Message{
		Key:   []byte("key-1"),
		Value: []byte("Hello, Kafka! Handling retries."),
		Time:  time.Now(),
	}

	// Use context with a timeout to prevent indefinite hangs
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// Try to send the message with retry logic
	log.Println("Sending message to Kafka with retry logic...")
	if err := writeMessageWithRetry(ctx, &writer, message); err != nil {
		log.Fatalf("Failed to send message even after retries: %v", err)
	}

	// Log success
	log.Println("Message successfully sent to Kafka.")
}

// writeMessageWithRetry attempts to send a message to Kafka with retry logic.
// If it encounters transient errors, it will retry sending the message up to maxRetries times.
func writeMessageWithRetry(ctx context.Context, writer *kafka.Writer, msg kafka.Message) error {
	var lastError error
	for attempts := 1; attempts <= maxRetries; attempts++ {
		if err := writer.WriteMessages(ctx, msg); err != nil {
			lastError = err
			// In case of an error, wait before retrying, unless the context is canceled
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("Attempt %d: Failed to write message. Retrying in %v...", attempts, retryInterval)
			select {
			case <-time.After(retryInterval):
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			// Message successfully sent
			return nil
		}
	}
	// Return the last encountered error if all retries fail
	return fmt.Errorf("all retry attempts failed: %w", lastError)
}
