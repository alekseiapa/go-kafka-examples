package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// CustomBalancer implements a custom partitioning strategy.
// It determines the partition based on a custom hash function applied to the message key.
type CustomBalancer struct{}

// Balance determines the partition for a message.
// It takes into account the available partitions and applies custom logic.
func (b *CustomBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if len(partitions) == 0 {
		log.Fatalf("No partitions available")
	}
	// Custom logic: Use the length of the key modulo the number of partitions.
	keyLength := len(msg.Key)
	selectedPartition := keyLength % len(partitions)
	return partitions[selectedPartition]
}

// main demonstrates sending a Kafka message using a custom balancer.
func main() {
	brokerAddress := "localhost:9092"
	topic := "custom-partition-topic"

	// Initialize a Kafka writer with a custom balancer
	writer := kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &CustomBalancer{}, // Use the custom balancer
	}
	defer writer.Close()

	// Create and send a message
	message := kafka.Message{
		Key:   []byte("custom-key"), // Key used for custom partitioning
		Value: []byte("Hello, Kafka with a custom balancer!"),
		Time:  time.Now(),
	}

	// Create a context with timeout to avoid indefinite block
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Println("Sending message with custom balancer...")
	err := writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatalf("Failed to write message to Kafka: %v", err)
	}
	log.Println("Message successfully sent with custom balancer logic.")

	// Optional: Log completion
	fmt.Println("Kafka producer with custom balancer setup complete. Exiting.")
}
