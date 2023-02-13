package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// filterMessages filters incoming Kafka messages based on content and sends relevant messages to a downstream Kafka topic.
func filterMessages(ctx context.Context, reader *kafka.Reader, writer *kafka.Writer, filterKeyword string) error {
	for {
		// Read messages from the Kafka topic
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("error reading message: %w", err)
		}

		// Check if the message value contains the filter keyword
		if containsFilterKeyword(string(msg.Value), filterKeyword) {
			// Write the filtered message to the downstream Kafka topic
			err := writer.WriteMessages(ctx, kafka.Message{
				Key:   msg.Key,
				Value: msg.Value,
				Time:  time.Now(),
			})
			if err != nil {
				return fmt.Errorf("error writing message: %w", err)
			}
		}
	}
}

// containsFilterKeyword checks if the message content contains the filter keyword.
func containsFilterKeyword(message, keyword string) bool {
	return contains(message, keyword)
}

// contains is a simple helper function that checks for the presence of a substring within a string.
func contains(str, substr string) bool {
	return stringIndex(str, substr) >= 0
}

// stringIndex finds the index of the first instance of substr in str.
func stringIndex(str, substr string) int {
	for i := 0; i+len(substr) <= len(str); i++ {
		if str[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// main is the entry point of the program. It sets up Kafka reader and writer, and handles message filtering.
func main() {
	brokerAddress := "localhost:9092"
	inputTopic := "input-topic"
	outputTopic := "output-topic"
	filterKeyword := "important"

	// Initialize a new Kafka reader (consumer) for the input topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   inputTopic,
		// GroupID ensures that multiple instances of the process share the workload
		GroupID: "message-filter-group",
	})
	defer reader.Close()

	// Initialize a new Kafka writer (producer) for the output topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress},
		Topic:    outputTopic,
		Balancer: &kafka.LeastBytes{}, // Distributes messages evenly among available partitions
	})
	defer writer.Close()

	// Context with timeout for overall operation, preventing indefinite operation in the event of issues
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Start filtering messages
	log.Println("Starting message filtering...")
	err := filterMessages(ctx, reader, writer, filterKeyword)
	if err != nil {
		log.Fatalf("Failed to filter messages: %v", err)
	}

	log.Println("Message filtering completed successfully.")
}
