package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

// main is the entry point for the Kafka consumer group program.
// It demonstrates setting up a consumer group, configuring the group ID and auto-offset reset policy.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to consume messages from
	topic := "example-topic"
	// Consumer group ID
	groupID := "example-group"

	// Create a new context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Handle termination signals to gracefully shutdown the consumer
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("Shutting down gracefully...")
		cancel()
	}()

	// Initialize a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:                []string{brokerAddress},
		GroupID:                groupID, // Group ID for the consumer group
		Topic:                  topic,
		MinBytes:               10e3, // 10KB
		MaxBytes:               10e6, // 10MB
		CommitInterval:         0,    // Synchronous commits for demonstration
		PartitionWatchInterval: 0,
	})
	defer reader.Close()

	// Read messages in a loop until the context is cancelled
	fmt.Println("Consuming messages from Kafka...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // Exit if context is cancelled
			}
			log.Fatalf("Error reading message: %v", err)
		}

		// Process the message
		log.Printf("Message received: key=%s value=%s", string(m.Key), string(m.Value))
	}
}
