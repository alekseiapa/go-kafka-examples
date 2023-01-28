package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// partitionMessage determines which partition a given message should be sent to based on its key.
// This function ensures messages with the same keys are sent to the same partition for ordered processing.
func partitionMessage(key string, totalPartitions int) int {
	// Use a simple hash function (sum of bytes) mod the number of partitions
	hash := 0
	for _, char := range key {
		hash += int(char)
	}
	return hash % totalPartitions
}

// main is the entry point for the Kafka partitioning program.
// It demonstrates setting up a Kafka producer to send messages partitioned based on their keys.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to send messages to
	topic := "example-topic"
	// Define the expected number of partitions for your topic
	partitionCount := 3

	// Initialize a Kafka connection to retrieve partition information
	conn, err := kafka.DialLeader(context.Background(), "tcp", brokerAddress, topic, 0)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer conn.Close()

	// Key of the message for partitioning
	key := "order-123"
	// Determine partition for the message
	partition := partitionMessage(key, partitionCount)
	log.Printf("Sending message with key '%s' to partition %d", key, partition)

	// Create a Kafka writer for the specific partition
	writer := kafka.Writer{
		Addr: kafka.TCP(brokerAddress),
		// Topic must be specified
		Topic: topic,
	}
	defer writer.Close()

	// Create a message
	message := kafka.Message{
		Key:       []byte(key),                              // Key ensures consistency in partitioning
		Value:     []byte("Message associated with " + key), // Message content
		Partition: partition,                                // Explicitly set the partition
		Time:      time.Now(),
	}

	// Context with timeout for sending the message
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send the message
	err = writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatalf("Failed to write message to Kafka: %v", err)
	}

	log.Println("Message successfully sent to Kafka.")

	// Optional: Log completion
	fmt.Println("Kafka message partitioning example complete. Exiting.")
}
