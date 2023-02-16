// log_processor.go

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// logStorage simulates storing logs in a storage or visualization system.
func logStorage(logMessage string) {
	// Here we simply print the log message. Replace this with actual storage logic.
	fmt.Printf("Log Storaged: %s\n", logMessage)
}

// processLog processes a log message.
func processLog(msg kafka.Message) string {
	// Here we assume that processing is a simple cast to a string.
	// In a real-world scenario, this function could parse the log, filter it, etc.
	return string(msg.Value)
}

// consumeFromKafka consumes messages from a Kafka topic and processes them.
func consumeFromKafka(brokers []string, topic string, groupId string) {
	// Initialize Kafka reader with the given broker addresses, topic, and consumer group
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupId,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer r.Close()

	// Set Kafka reading context with a timeout to handle shutdowns gracefully
	ctx := context.Background()
	for {
		// Read a message from Kafka
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("Error while reading message: %v", err)
		}

		// Process log message
		processedLog := processLog(msg)

		// Forward the processed log to storage or visualization system
		logStorage(processedLog)

		// Mark message as processed
		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Fatalf("Failed to commit messages: %v", err)
		}
	}
}

// main is the entry point of the program
func main() {
	// Kafka configuration: broker addresses, topic, and consumer group
	brokers := []string{"localhost:9092"} // Replace with actual broker addresses
	topic := "logs"                       // Kafka topic to read logs from
	groupId := "log-consumption-group"    // Consumer group ID

	// Start log consumption and processing pipeline
	consumeFromKafka(brokers, topic, groupId)
}
