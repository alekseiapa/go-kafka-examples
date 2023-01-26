package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// produceMessages demonstrates sending a Kafka message with headers.
// Headers can be used for metadata like tracing or filtering.
func produceMessages(broker, topic string) {
	// Initialize a Kafka writer (producer) using the latest `kafka.Writer` API.
	writer := kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Balances the load by writing to the partition with the least data
	}
	defer writer.Close() // Ensure the writer closes when we are done

	// Create a Kafka message with custom headers to attach metadata.
	message := kafka.Message{
		Key:   []byte("example-key"),
		Value: []byte("Hello, Kafka with headers!"),
		Headers: []kafka.Header{
			{
				Key:   "trace-id",
				Value: []byte("12345"),
			},
			{
				Key:   "user-id",
				Value: []byte("67890"),
			},
		},
	}

	// Context with timeout to ensure the operation does not hang indefinitely.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send the message with custom headers.
	err := writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatalf("Failed to write message to Kafka: %v", err)
	}

	log.Println("Message sent successfully with headers.")
}

// main is the entry point of the application.
// It demonstrates the use of Kafka headers by running the
// produceMessages function which attaches headers to messages.
func main() {
	brokerAddress := "localhost:9092" // Kafka broker address
	topic := "example-topic"          // Kafka topic

	log.Printf("Producing messages to topic %s on broker %s\n", topic, brokerAddress)

	// Produce a message to Kafka with headers.
	produceMessages(brokerAddress, topic)

	log.Println("Kafka headers example complete. Exiting.")
}
