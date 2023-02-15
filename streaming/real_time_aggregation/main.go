package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// Aggregator holds the state of aggregated statistics.
type Aggregator struct {
	count int
	sum   float64
}

// NewAggregator initializes and returns a new Aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{}
}

// ProcessMessage updates the aggregator with a new piece of data.
func (a *Aggregator) ProcessMessage(value float64) {
	a.count++
	a.sum += value
}

// GetAverage calculates and returns the average value from the aggregated data.
func (a *Aggregator) GetAverage() float64 {
	if a.count == 0 {
		return 0.0
	}
	return a.sum / float64(a.count)
}

// consumeMessages processes the Kafka messages from a given topic and applies aggregation logic.
func consumeMessages(ctx context.Context, reader *kafka.Reader, aggregator *Aggregator) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping message consumption...")
			return
		default:
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Fatalf("Error reading message: %v", err)
			}

			// Convert message value to float64
			var value float64
			if _, err := fmt.Sscanf(string(m.Value), "%f", &value); err != nil {
				log.Printf("Invalid message value: %v", err)
				continue
			}

			// Process the message with the aggregator
			aggregator.ProcessMessage(value)

			// For demonstration, print the current average
			log.Printf("Current Average: %.2f", aggregator.GetAverage())
		}
	}
}

func main() {
	// Configuring Kafka consumer
	brokerAddress := "localhost:9092"
	topic := "aggregated-topic"

	// Initialize Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "statistics-group",
	})
	defer reader.Close()

	// Instantiate a new Aggregator
	aggregator := NewAggregator()

	// Setup signal handling for graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Create a context that will be canceled on signal reception
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the consumeMessages function in a goroutine
	go consumeMessages(ctx, reader, aggregator)

	// Wait for termination signal
	<-signals
	fmt.Println("Shutting down gracefully...")
}
