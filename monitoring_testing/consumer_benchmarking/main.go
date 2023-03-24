package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaConsumer represents a consumer configuration.
type KafkaConsumer struct {
	Brokers []string // List of broker addresses
	Topic   string   // Kafka topic to consume from
	GroupID string   // Consumer group ID
}

// NewKafkaConsumer initializes a new Kafka consumer configuration.
func NewKafkaConsumer(brokers []string, topic, groupID string) *KafkaConsumer {
	return &KafkaConsumer{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}
}

// consumeMessages handles message consumption from Kafka, printing consumption rates and processing efficiency.
func (kc *KafkaConsumer) consumeMessages() {
	// Initialize a new Kafka reader (consumer) with provided configuration.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: kc.Brokers,
		Topic:   kc.Topic,
		GroupID: kc.GroupID,
	})
	defer r.Close()

	log.Println("Starting message consumption...")

	// Track start time for benchmarking.
	startTime := time.Now()
	messageCount := 0

	for {
		// Context with timeout to control message fetching timeframe.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Read message from Kafka.
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			break
		}

		messageCount++
		// Simulate message processing.
		log.Printf("Received message: %s", string(m.Value))
	}

	// Calculate and print message consumption statistics.
	duration := time.Since(startTime)
	log.Printf("Consumed %d messages in %v", messageCount, duration)
	log.Printf("Consumption rate: %.2f messages/sec", float64(messageCount)/duration.Seconds())
}

// main sets up the Kafka consumer and starts consuming messages for benchmarking.
func main() {
	// Kafka broker addresses.
	brokers := []string{"localhost:9092"}
	// Kafka topic to consume from.
	topic := "example-topic"
	// Consumer group ID.
	groupID := "example-group"

	consumer := NewKafkaConsumer(brokers, topic, groupID)
	consumer.consumeMessages()

	// Log completion.
	fmt.Println("Kafka consumer benchmarking complete. Exiting.")
}
