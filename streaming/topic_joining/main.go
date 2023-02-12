package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// main is the entry point of the program. It demonstrates consuming messages from two Kafka topics
// and joining them to produce enriched messages to a new topic.
func main() {
	brokerAddress := "localhost:9092"
	topic1 := "user-updates"
	topic2 := "user-actions"
	outputTopic := "enriched-user-data"

	// Initialize Kafka readers for the input topics
	reader1 := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic1,
		GroupID: "enrichment-group",
	})
	defer reader1.Close()

	reader2 := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic2,
		GroupID: "enrichment-group",
	})
	defer reader2.Close()

	// Initialize Kafka writer for the output topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress},
		Topic:    outputTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Context for Kafka operations
	ctx := context.Background()

	// Start reading from the topics and join the data
	for {
		// Read messages from the first topic
		msg1, err := reader1.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message from topic1: %v", err)
			continue
		}

		// Read messages from the second topic
		msg2, err := reader2.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message from topic2: %v", err)
			continue
		}

		// Perform the join logic (mocked as concatenation for simplicity)
		enrichedValue := fmt.Sprintf("%s and %s", string(msg1.Value), string(msg2.Value))

		// Send the enriched message to the output topic
		outputMessage := kafka.Message{
			Key:   msg1.Key, // Assuming both topics can be joined on the same key
			Value: []byte(enrichedValue),
		}

		if err := writer.WriteMessages(ctx, outputMessage); err != nil {
			log.Printf("Error writing enriched message: %v", err)
		}

		log.Printf("Successfully wrote enriched message: %s", enrichedValue)
	}
}
