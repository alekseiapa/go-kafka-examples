package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main is the entry point for the Kafka lag monitoring program.
// It demonstrates setting up a Kafka consumer, tracking and logging message lag in Kafka topics.
func main() {
	brokerAddress := "localhost:9092"
	topic := "example-topic"
	partition := 0

	// Create a new Kafka reader (consumer) with specified broker and topic settings.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		Partition: partition,
	})

	defer reader.Close()

	log.Println("Starting Kafka consumer to monitor message lag...")

	// Start a loop to continuously read messages
	for {
		// Set a context with timeout to prevent blocking indefinitely
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Fetch the next message, if available
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Compute the lag based on the difference between message and current time
		lag := time.Since(msg.Time)

		// Log the message details and computed lag for monitoring purposes
		log.Printf("Received message: %s | Lag: %v ", string(msg.Value), lag)

		// Handle business logic with the message here (e.g., processing)
		handleMessage(msg)
	}
}

// handleMessage processes the message received from Kafka
// This is a placeholder for where business logic would be implemented to handle and process messages.
func handleMessage(m kafka.Message) {
	// Example processing step
	log.Printf("Processing message: %s", string(m.Value))

	// Simulate processing time
	time.Sleep(1 * time.Second)
}

// To run the program, ensure Kafka is running locally
// and the specified topic (`example-topic`) exists.
// You can produce messages to test lag monitoring
// with a Kafka producer tool or another program.
