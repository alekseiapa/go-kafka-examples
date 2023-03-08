package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main is the entry point for the Kafka consumer program.
// It demonstrates setting up a Kafka consumer, handling messages with backpressure, and ensuring system stability.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to consume messages from
	topic := "example-topic"
	// Kafka group ID
	groupID := "example-group"

	// Initialize a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
	})
	defer reader.Close()

	// Create a cancellable context to allow graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to throttle message processing for backpressure handling
	messages := make(chan kafka.Message, 5) // Buffer can be adjusted based on system capacity

	// Producer routine to read messages from Kafka and add them to the processing channel
	go func() {
		for {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("Error fetching message: %v", err)
				return
			}
			messages <- msg
		}
	}()

	// Consumer routine to process messages from the channel
	for msg := range messages {
		processMessage(msg)
		// After processing, we acknowledge the message
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Failed to commit message: %v", err)
		}
	}
}

// processMessage simulates message processing and can be extended
// according to the application's requirements.
func processMessage(msg kafka.Message) {
	// Simulate message processing time
	time.Sleep(2 * time.Second)
	log.Printf("Processed message: %s", string(msg.Value))
}
