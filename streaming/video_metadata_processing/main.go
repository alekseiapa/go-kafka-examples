package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// VideoMetadata represents the structure of the metadata associated with a video.
type VideoMetadata struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Duration    int    `json:"duration"`
	Resolution  string `json:"resolution"`
	Description string `json:"description"`
}

// main is the entry point for the video metadata processing application.
// It demonstrates consuming messages from a Kafka topic, transforming them,
// and logging the transformed metadata.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic from which the metadata is consumed
	topic := "video-metadata"

	// Create a new Kafka reader (consumer) with the specified configuration
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	// Context with timeout to ensure the program does not hang indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		// Read messages from the Kafka topic
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("Failed to read message: %v", err)
		}

		// Process the received message by unmarshaling it into a VideoMetadata struct
		var metadata VideoMetadata
		if err := json.Unmarshal(m.Value, &metadata); err != nil {
			log.Printf("Error unmarshaling message: %v", err)
			continue
		}

		// Perform any necessary transformation on the metadata
		// For example, here we log the transformed metadata
		log.Printf("Processed video metadata: ID=%s, Title=%s, Duration=%d seconds, Resolution=%s, Description=%s",
			metadata.ID, metadata.Title, metadata.Duration, metadata.Resolution, metadata.Description)

		// Optional: Print the message on console
		fmt.Printf("Video Metadata: %+v\n", metadata)
	}
}

// Note: To test this program, a Kafka producer should be used to send video metadata messages to the specified Kafka topic.