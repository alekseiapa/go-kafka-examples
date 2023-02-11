package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// Message represents the structure of the message that will be transformed.
type Message struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// main is the entry point for the program.
// It sets up a Kafka consumer to read messages, transform them, and publish the transformed messages to another Kafka topic.
func main() {
	consumer := "localhost:9092"
	topic := "input-topic"
	transformedTopic := "output-topic"

	// Initialize a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{consumer},
		Topic:   topic,
		GroupID: "message-transformer-group",
	})
	defer reader.Close()

	// Initialize a new Kafka writer (producer) for transformed messages
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{consumer},
		Topic:    transformedTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Context with cancellation for managing lifecycle of message processing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("Starting the message transformation service...")

	for {
		// Read messages from the input topic
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("Failed to read message: %v", err)
			break
		}

		// Transform the message
		transformedMessage, err := transformMessage(message)
		if err != nil {
			log.Printf("Error transforming message: %v", err)
			continue
		}

		// Write the transformed message to the output topic
		if err := writer.WriteMessages(ctx, transformedMessage); err != nil {
			log.Printf("Failed to write transformed message: %v", err)
		}
	}
}

// transformMessage takes a Kafka Message, deserializes it, applies a transformation, and returns a new Kafka Message.
func transformMessage(msg kafka.Message) (kafka.Message, error) {
	var originalMessage Message

	// Unmarshal the message value from JSON
	if err := json.Unmarshal(msg.Value, &originalMessage); err != nil {
		return kafka.Message{}, fmt.Errorf("error unmarshaling message: %w", err)
	}

	// Apply transformation (for example, convert value to uppercase)
	originalMessage.Value = transform(originalMessage.Value)

	// Marshal the transformed message back to JSON
	transformedValue, err := json.Marshal(originalMessage)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("error marshaling transformed message: %w", err)
	}

	// Return a new Kafka message with transformed data
	return kafka.Message{
		Key:   msg.Key,
		Value: transformedValue,
	}, nil
}

// transform is a helper function that modifies the message content.
// For demonstration, it converts the message value to uppercase.
func transform(value string) string {
	return fmt.Sprintf("TRANSFORMED: %s", value)
}
