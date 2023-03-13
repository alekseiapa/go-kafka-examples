package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer represents a Kafka consumer instance.
type Consumer struct {
	Reader *kafka.Reader
}

// NewConsumer initializes and returns a new Consumer instance.
// It configures the Kafka reader to consume messages from the given topic.
func NewConsumer(brokerAddress, topic string, groupID string) *Consumer {
	// Kafka reader configuration with brokers, topic, and group ID.
	// The Group ID allows for consumer group management.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
		// Additional settings, like setting up partitions balancing strategy
		// can be specified here if required.
	})
	return &Consumer{Reader: reader}
}

// Close gracefully shuts down the Kafka consumer.
func (c *Consumer) Close() error {
	return c.Reader.Close()
}

// ConsumeMessages processes messages from the Kafka topic. It continues to listen
// and process incoming messages until the context is canceled or an error occurs.
func (c *Consumer) ConsumeMessages(ctx context.Context) {
	log.Println("Starting Kafka consumer...")
	for {
		// Read messages from Kafka
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			// Break on context cancellation or serious errors.
			if ctx.Err() != nil {
				return
			}
			continue
		}
		// Log the message key and value.
		log.Printf("Received message: key=%s value=%s\n", string(msg.Key), string(msg.Value))
		// Ideally, process the message here, e.g., writing to a database/ file, etc.
	}
}

func main() {
	// Define Kafka configuration
	brokerAddress := "localhost:9092"
	topic := "example-topic"
	groupID := "example-group"

	// Initialize the consumer
	consumer := NewConsumer(brokerAddress, topic, groupID)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalf("Failed closing consumer: %v", err)
		}
	}()

	// Create a context with a timeout to allow graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start consuming messages
	consumer.ConsumeMessages(ctx)

	log.Println("Kafka consumer stopped.")
}
