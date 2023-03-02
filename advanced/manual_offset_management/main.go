package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main serves as the entry point of the application.
// It demonstrates manual offset management with Kafka consumers.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to consume messages from
	topic := "example-topic"
	// Consumer group ID
	groupID := "example-group"

	// Initialize a new Kafka reader (consumer) with manual offset management
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
		// Enable manual offset management by setting CommitInterval to 0
		CommitInterval: 0,
	})
	defer reader.Close()

	// Create a context with a timeout for consuming messages
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	log.Println("Starting to consume messages...")

	for {
		// Read messages from the Kafka topic
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// Exit the loop if the context times out or an error occurs
			if err == context.DeadlineExceeded {
				log.Print("Context deadline exceeded, stopping consumer.")
				break
			}
			log.Fatalf("Failed to read message: %v", err)
		}

		// Process the consumed message
		log.Printf("Consumed message: key=%s value=%s offset=%d", string(msg.Key), string(msg.Value), msg.Offset)

		// Manually commit the offset after processing
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Fatalf("Failed to commit offset: %v", err)
		}

		log.Printf("Offset committed for message with key=%s", string(msg.Key))
	}

	fmt.Println("Consumer processing complete. Exiting.")
}