package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// Message represents the structured data format we expect in the Kafka message.
type Message struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

// connectKafka initiates a connection to the Kafka broker.
// It returns a Kafka reader configured for the specified topic and group ID.
func connectKafka(brokers []string, topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

// consumeMessages reads messages from the Kafka topic, deserializes them
// into the Message struct, and processes them.
func consumeMessages(reader *kafka.Reader) {
	ctx := context.Background()

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %s", err)
			continue
		}

		var msg Message
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("Error parsing message: %s", err)
			continue
		}

		// Process the message - replace this with actual logic
		processMessage(msg)
	}
}

// processMessage handles the business logic for each consumed message.
// This function can be expanded to include complex processing capabilities.
func processMessage(msg Message) {
	fmt.Printf("Processed message: ID=%s, Name=%s, Age=%d\n", msg.ID, msg.Name, msg.Age)
}

// main is the entry point for the Kafka consumer program.
// It initializes the Kafka connection and begins consuming messages.
func main() {
	brokerAddress := "localhost:9092" // Kafka broker address
	topic := "example-topic"          // Topic to consume messages from
	groupID := "example-group"        // Consumer group ID

	// Connect to Kafka
	kafkaReader := connectKafka([]string{brokerAddress}, topic, groupID)
	defer kafkaReader.Close()

	log.Println("Started consuming messages...")
	consumeMessages(kafkaReader)
}

// To test this program, ensure that Kafka is running locally and there is a producer sending JSON messages
// to the 'example-topic'. A sample JSON message could look like {"id": "1", "name": "John Doe", "age": 30}.
// Run the consumer with `go run main.go`.
