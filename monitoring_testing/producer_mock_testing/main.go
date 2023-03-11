package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main is the entry point of the program. It sets up a Kafka producer,
// sends a test message, and logs the result.
func main() {
	// Define the Kafka broker address for local testing.
	brokerAddress := "localhost:9092"

	// Define the Kafka topic for testing.
	topic := "test-topic"

	// Initialize a Kafka writer (producer).
	writer := kafka.Writer{
		Addr:      kafka.TCP(brokerAddress),
		Topic:     topic,
		Balancer:  &kafka.LeastBytes{}, // Balance messages across partitions
		BatchSize: 1,                   // Send messages as soon as possible
	}
	defer writer.Close()

	// Create a test message to send.
	message := kafka.Message{
		Key:   []byte("key-1"), // Key ensures the message always goes to the same partition
		Value: []byte("Test message to Kafka broker"),
		Time:  time.Now(),
	}

	// Set context with timeout for the producer to avoid blocking.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try to send the message to the Kafka broker.
	log.Println("Attempting to send message to Kafka broker...")
	err := writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatalf("Failed to write message to Kafka: %v", err)
	}

	log.Println("Message successfully sent to Kafka broker.")

	// This simulates testing the producer logic with a live Kafka instance.
}
