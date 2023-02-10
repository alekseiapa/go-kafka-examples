package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// MetricsMessage represents a simple metrics message structure.
type MetricsMessage struct {
	Timestamp time.Time
	Key       string
	Value     float64
}

// producerConfig defines the configuration for Kafka producer.
var producerConfig = kafka.WriterConfig{
	Brokers:  []string{"localhost:9092"},
	Topic:    "metrics",
	Balancer: &kafka.LeastBytes{},
}

// consumerConfig defines the configuration for Kafka consumer.
var consumerConfig = kafka.ReaderConfig{
	Brokers:  []string{"localhost:9092"},
	Topic:    "metrics",
	GroupID:  "metrics-consumer-group",
	MaxBytes: 10e6, // 10MB
}

// produceMetrics simulates producing metrics to a Kafka topic.
func produceMetrics(wg *sync.WaitGroup) {
	defer wg.Done()
	writer := kafka.NewWriter(producerConfig)
	defer writer.Close()

	for i := 0; i < 10; i++ {
		msg := MetricsMessage{
			Timestamp: time.Now(),
			Key:       fmt.Sprintf("metric-%d", i),
			Value:     float64(i) * 1.23,
		}

		message := kafka.Message{
			Key:   []byte(msg.Key),
			Value: []byte(fmt.Sprintf("%f", msg.Value)),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		log.Printf("Sending metrics: %s -> %f", msg.Key, msg.Value)

		if err := writer.WriteMessages(ctx, message); err != nil {
			log.Printf("Failed to write message: %v", err)
		}

		time.Sleep(time.Second)
	}
}

// consumeMetrics reads and logs metrics from a Kafka topic.
func consumeMetrics(wg *sync.WaitGroup, stop <-chan struct{}) {
	defer wg.Done()
	reader := kafka.NewReader(consumerConfig)
	defer reader.Close()

	for {
		select {
		case <-stop:
			log.Println("Stopping consumer")
			return
		default:
			m, err := reader.FetchMessage(context.Background())
			if err != nil {
				log.Printf("Error fetching message: %v", err)
				return
			}

			log.Printf("Consumed metrics: %s -> %s", string(m.Key), string(m.Value))

			if err := reader.CommitMessages(context.Background(), m); err != nil {
				log.Printf("Error committing message: %v", err)
			}
		}
	}
}

func main() {
	var wg sync.WaitGroup

	// Channel for clean shutdown
	stop := make(chan struct{})

	log.Println("Starting metrics pipeline")

	// Start producing and consuming metrics
	wg.Add(1)
	go produceMetrics(&wg)

	wg.Add(1)
	go consumeMetrics(&wg, stop)

	// Wait for SIGINT or SIGTERM for termination to handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Shutdown signal received")

	// Signal goroutines to stop
	close(stop)

	// Wait for all goroutines to complete
	wg.Wait()

	log.Println("Metrics pipeline stopped")
}
