package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Event represents a simple event structure with an ID and a Message.
type Event struct {
	ID      string
	Message string
}

// State represents the reconstructed state through event sourcing.
type State struct {
	Events []Event
}

// Producer sends events to a Kafka topic.
func Producer(ctx context.Context, topic string, brokers []string, events []Event) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Ensures efficient partition distribution
	})
	defer writer.Close()

	for _, event := range events {
		// Prepare the message
		msg := kafka.Message{
			Key:   []byte(event.ID),
			Value: []byte(event.Message),
			Time:  time.Now(),
		}

		// Send the message with context for timeout management
		if err := writer.WriteMessages(ctx, msg); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
		log.Printf("Event with ID=%s sent to Kafka\n", event.ID)
	}
	return nil
}

// Consumer reads events from a Kafka topic and reconstructs the state.
func Consumer(ctx context.Context, topic string, brokers []string) (*State, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "example-group",
	})
	defer r.Close()

	state := &State{}

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch message: %w", err)
		}

		// Log and append event for state reconstruction
		event := Event{ID: string(m.Key), Message: string(m.Value)}
		log.Printf("Event fetched with ID=%s\n", event.ID)
		state.Events = append(state.Events, event)

		// Acknowledge successfully processed messages
		if err := r.CommitMessages(ctx, m); err != nil {
			return nil, fmt.Errorf("failed to commit message: %w", err)
		}
	}
}

// main is the entry point for the event sourcing system demonstration using Kafka.
func main() {
	// Example configuration for Kafka broker and topic
	brokers := []string{"localhost:9092"}
	topic := "events"

	// Example events to be produced
	events := []Event{
		{ID: "1", Message: "User signed up."},
		{ID: "2", Message: "User updated profile."},
	}

	// Create a context with a timeout to prevent indefinite blocking
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Produce events to Kafka
	if err := Producer(ctx, topic, brokers, events); err != nil {
		log.Fatalf("Producer error: %v", err)
	}

	// Consume events from Kafka to reconstruct the state
	state, err := Consumer(ctx, topic, brokers)
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}

	// Output the reconstructed state for verification
	for _, event := range state.Events {
		fmt.Printf("Event ID=%s, Message=%s\n", event.ID, event.Message)
	}

	// Log completion
	fmt.Println("Event sourcing process complete.")
}