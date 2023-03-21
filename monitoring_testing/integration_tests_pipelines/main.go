package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main function orchestrates the setup and execution of integration tests
// for Kafka pipelines, demonstrating end-to-end data flow and processing.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to send and consume messages
	topic := "integration-test-topic"

	// Initialize the Kafka writer for producing messages
	writer := kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Balances the load
	}
	defer writer.Close()

	// Initialize the Kafka reader for consuming messages
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokerAddress},
		Topic:          topic,
		GroupID:        "integration-test-group",
		MinBytes:       10e1,        // 10KB
		MaxBytes:       10e6,        // 10MB
		CommitInterval: time.Second, // Commit offsets every second
	})
	defer reader.Close()

	// Execute end-to-end test
	if err := performIntegrationTest(&writer, reader); err != nil {
		log.Fatalf("Integration test failed: %v", err)
	}

	fmt.Println("Integration test completed successfully.")
}

// performIntegrationTest simulates sending and receiving a message to validate
// the Kafka pipeline's functionality.
func performIntegrationTest(writer *kafka.Writer, reader *kafka.Reader) error {
	// Create a test message to send
	message := kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte("Integration test message."),
	}

	// Context with timeout to prevent indefinite blocking
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send the message using the Kafka writer
	log.Println("Producing message...")
	if err := writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}
	log.Println("Message produced successfully.")

	// Read the message from the Kafka reader
	log.Println("Consuming message...")
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to consume message: %w", err)
	}
	log.Println("Message consumed successfully.")

	// Verify the message content
	if string(msg.Value) != string(message.Value) {
		return fmt.Errorf("message content mismatch: expected %s, got %s", message.Value, msg.Value)
	}

	log.Println("Message content validated.")
	return nil
}
