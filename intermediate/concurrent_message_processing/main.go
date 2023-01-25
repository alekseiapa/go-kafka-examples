package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
)

// consumeMessages sets up a Kafka consumer to process messages concurrently.
// It improves throughput for high-volume systems by leveraging goroutines.
func consumeMessages(brokerAddress, topic, groupID string, maxConcurrency int) error {
	// Initialize a new Kafka reader (consumer) with the given configuration
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
	})
	defer reader.Close()

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency) // Semaphore to limit concurrency

	for {
		// Read messages from Kafka
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			return fmt.Errorf("error while reading message: %v", err)
		}

		// Acquire a semaphore
		sem <- struct{}{}

		// Increment wait group counter
		wg.Add(1)

		// Process the message in a new goroutine
		go func(msg kafka.Message) {
			defer wg.Done() // Decrement wait group counter when done
			defer func() { <-sem }() // Release the semaphore

			// Simulate message processing
			log.Printf("Processing message: topic=%s partition=%d offset=%d key=%s value=%s",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			// You can add your message processing logic here
		}(m)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	return nil
}

// main is the entry point of the program.
// It demonstrates consuming messages concurrently from a Kafka topic.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to consume messages from
	topic := "example-topic"
	// Consumer group ID
	groupID := "example-group"
	// Maximum number of concurrent message processors
	maxConcurrency := 5

	// Start consuming messages concurrently
	if err := consumeMessages(brokerAddress, topic, groupID, maxConcurrency); err != nil {
		log.Fatalf("Failed to consume messages: %v", err)
	}

	// Optional: Log completion
	fmt.Println("Concurrent message processing complete. Exiting.")
}
