package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message represents the structure of the JSON message to be sent to Kafka.
type Message struct {
	ID      int       `json:"id"`
	Content string    `json:"content"`
	Time    time.Time `json:"time"`
}

// produceMessages initializes a Kafka writer, creates a JSON message, and sends it to the specified topic.
func produceMessages(brokers []string, topic string) {
	// Create a new Kafka writer that writes messages to the specified Kafka topic.
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:   brokers,
		Topic:     topic,
		Balancer:  &kafka.LeastBytes{},
		BatchSize: 1, // Send messages immediately for simplicity
	})
	defer writer.Close()

	// Construct a message with sample data.
	msg := Message{
		ID:      1,
		Content: "This is a JSON serialized message",
		Time:    time.Now(),
	}

	// Serialize the message to JSON format.
	data, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("Failed to serialize message to JSON: %v", err)
	}

	// Prepare a Kafka message with a key, serialized JSON value, and a timestamp.
	message := kafka.Message{
		Key:   []byte("key-1"),
		Value: data,
		Time:  time.Now(),
	}

	// Create a context with a timeout to ensure the operation doesn’t hang indefinitely.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send the serialized JSON message to the Kafka topic using the writer.
	log.Println("Sending message to Kafka...")
	err = writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatalf("Failed to write message to Kafka: %v", err)
	}

	log.Println("Message successfully sent as JSON to Kafka.")
}

func main() {
	// Specify the Kafka broker address and the target topic to send messages.
	brokerAddress := "localhost:9092"
	topic := "json-topic"

	// Call the function to produce messages to the specified Kafka topic.
	produceMessages([]string{brokerAddress}, topic)

	// Optional: Log completion of the producer setup.
	log.Println("Kafka JSON producer setup complete. Exiting.")
}
