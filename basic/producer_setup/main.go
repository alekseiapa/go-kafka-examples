package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main is the entry point for the Kafka producer program.
// It demonstrates setting up a Kafka producer, configuring it, and sending a test message to a Kafka topic.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to send messages to
	topic := "example-topic"

	// Initialize a new Kafka writer (producer)
	writer := kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Distributes messages to the partition with the least bytes
		Async:    false,               // Ensures synchronous writes
	}
	defer writer.Close()

	// Create a test message to send
	message := kafka.Message{
		Key:   []byte("key-1"), // Key ensures the message always goes to the same partition
		Value: []byte("Hello, Kafka! This is a test message."),
		Time:  time.Now(),
	}

	// Context with timeout to prevent hanging indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send the message
	log.Println("Sending message to Kafka...")
	err := writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatalf("Failed to write message to Kafka: %v", err)
	}

	log.Println("Message successfully sent to Kafka.")

	// Optional: Log completion
	fmt.Println("Kafka producer setup complete. Exiting.")
}
