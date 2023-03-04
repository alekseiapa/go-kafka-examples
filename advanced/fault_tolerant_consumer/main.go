package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// main is the entry point for the fault-tolerant Kafka consumer program.
// It sets up a consumer that listens to messages on a given topic
// and demonstrates how to handle errors and interruptions gracefully.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to consume messages from
	topic := "example-topic"
	// Consumer group ID for managing consumer state
	groupID := "example-group"

	// Create a new context with cancellation ability for the consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a channel to listen for interrupt or terminate signals
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// Initialize a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3,       // 10KB
		MaxBytes: 10e6,       // 10MB
	})
	defer reader.Close()

	// Run the consumer in a separate goroutine
	go func() {
		log.Println("Consumer started, listening for messages...")
		for {
			select {
			case <-ctx.Done():
				log.Println("Consumer exiting...")
				return
			default:
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					log.Printf("Error reading message: %v", err)
					continue
				}
				log.Printf("Received message: key=%s value=%s", string(m.Key), string(m.Value))
			}
		}
	}()

	// Wait for a termination signal
	<-stopChan
	log.Println("Received termination signal, shutting down...")

	// Optional: Wait some time to complete processing before shutdown
	time.Sleep(2 * time.Second)
	log.Println("Consumer shutdown complete.")
}
