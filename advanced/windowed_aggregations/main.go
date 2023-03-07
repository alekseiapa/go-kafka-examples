package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// main initializes the Kafka consumer and performs windowed aggregations.
func main() {
	// Kafka setup
	brokerAddress := "localhost:9092"
	topic := "example-topic"
	groupID := "example-group"

	// Create a new Kafka reader (consumer) for reading messages from the topic.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
		// Read messages from the earliest position
		StartOffset: kafka.FirstOffset,
		// Poll for new messages every second
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	// Start windowed aggregation processing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	windowDuration := 30 * time.Second
	processWindowedAggregation(ctx, reader, windowDuration)
}

// processWindowedAggregation handles message consumption and windowed aggregation.
func processWindowedAggregation(ctx context.Context, reader *kafka.Reader, windowDuration time.Duration) {
	var mu sync.Mutex
	messageBuffer := make([]kafka.Message, 0)

	// Timer to trigger window processing
	ticker := time.NewTicker(windowDuration)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				if len(messageBuffer) > 0 {
					log.Println("Processing windowed aggregation...")
					aggregateMessages(messageBuffer)
					messageBuffer = make([]kafka.Message, 0) // Clear buffer after processing
				}
				mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Read messages and add them to the buffer
	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		mu.Lock()
		messageBuffer = append(messageBuffer, message)
		mu.Unlock()

		log.Printf("Received message: %s", string(message.Value))
	}
}

// aggregateMessages performs the aggregation on the buffered messages.
func aggregateMessages(messages []kafka.Message) {
	// Example: Count messages by key
	keyCounts := make(map[string]int)
	for _, message := range messages {
		key := string(message.Key)
		keyCounts[key]++
	}

	// Display aggregated results
	log.Println("Aggregated Results:")
	for key, count := range keyCounts {
		fmt.Printf("Key: %s, Count: %d\n", key, count)
	}
}
