package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// The entry point of the application.
// It configures the Kafka producer and sends a string message to a specified topic.
func main() {
	// Define the Kafka broker address
	brokerAddress := "localhost:9092"
	// Define the Kafka topic to which the message will be sent
	topic := "example-topic"

	// Create a new Kafka writer (producer) with the specified configuration
	// Balancer: Distributes messages to partitions using the LeastBytes strategy
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		Balancer:  &kafka.LeastBytes{},
		BatchSize: 1, // Send each message immediately
	})
	// Ensure the writer is closed when the function exits
	defer writer.Close()

	// Create the message to send
	message := kafka.Message{
		Key:   []byte("example-key"), // Optional key
		Value: []byte("Hello, Kafka!"),
	}

	// Use a context with timeout to avoid indefinite blocking
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Attempt to send the message to the configured Kafka topic
	log.Println("Sending message to Kafka...")
	err := writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	log.Println("Message sent successfully.")
	fmt.Println("Kafka producer has completed sending the message.")
}
