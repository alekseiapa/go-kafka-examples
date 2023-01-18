package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// main is the entry point of the Kafka producer program.
// This program will send messages to a Kafka topic, handling errors gracefully
// and retrying as needed in case of failures.
func main() {
	// Kafka broker address and topic configuration
	brokerAddress := "localhost:9092"
	topic := "example-topic"

	// Initialize a new Kafka writer (producer)
	writer := kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Balancer strategy to send messages to the partition with the least bytes.
		Async:    false,               // Ensure synchronous writes for reliability.
	}
	defer writer.Close()

	// Message to send
	message := kafka.Message{
		Key:   []byte("key-1"),
		Value: []byte("Hello, Kafka! Handling errors and retrying if necessary."),
		Time:  time.Now(),
	}

	// Context with a timeout to prevent indefinite hangs
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Number of retry attempts for sending a message
	const maxRetries = 3

	// Attempt to send the message, with retries
	retries := 0
	for {
		log.Println("Sending message to Kafka...")
		err := writer.WriteMessages(ctx, message)
		if err != nil {
			if retries < maxRetries {
				retries++
				log.Printf("Failed to write message to Kafka: %v. Retrying %d/%d...", err, retries, maxRetries)
				time.Sleep(2 * time.Second) // Wait before retrying
				continue
			} else {
				log.Fatalf("Failed to write message to Kafka after %d retries: %v", maxRetries, err)
			}
		}
		break
	}

	log.Println("Message successfully sent to Kafka.")

	// Optional: Indicate the end of the program
	fmt.Println("Kafka producer process complete. Exiting.")
}
