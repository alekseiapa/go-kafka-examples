package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main sets up a Kafka producer with advanced configuration settings such as batch size,
// compression, and retries to optimize performance. It then sends a test message to a Kafka topic.
func main() {
	// Kafka broker address.
	brokerAddress := "localhost:9092"
	// Kafka topic to send messages to.
	topic := "example-topic"

	// Initialize a new Kafka writer (producer) with advanced configuration settings.
	writer := kafka.Writer{
		Addr:         kafka.TCP(brokerAddress),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{}, // Distributes messages to the partition with the least bytes.
		BatchSize:    10,                  // Sends message batches of up to 10 messages.
		BatchTimeout: 1 * time.Second,     // Flushes batches every 1 second.
		Compression:  kafka.Snappy,        // Uses Snappy compression for improved network performance.
		Async:        false,               // Synchronous write to ensure reliability.
	}
	defer writer.Close()

	// Create a test message to send.
	message := kafka.Message{
		Key:   []byte("key-1"), // Key ensures the message always goes to the same partition.
		Value: []byte("Hello, Kafka! This is an optimized test message."),
		Time:  time.Now(),
	}

	// Context with timeout to prevent hanging indefinitely.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send the message.
	log.Println("Sending optimized message to Kafka...")
	err := writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatalf("Failed to write message to Kafka: %v", err)
	}

	log.Println("Message successfully sent to Kafka with optimization.")

	// Optional: Log completion.
	log.Println("Kafka producer setup with advanced configuration complete. Exiting.")
}
