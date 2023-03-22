package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// ProducerConfig holds configuration for the Kafka producer.
type ProducerConfig struct {
	Brokers      []string
	Topic        string
	BatchSize    int
	BatchTimeout time.Duration
}

// KafkaProducer represents a Kafka producer instance.
type KafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer creates and returns a new KafkaProducer instance based on the provided configuration.
func NewKafkaProducer(cfg ProducerConfig) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			BatchSize:    cfg.BatchSize,
			BatchTimeout: cfg.BatchTimeout,
			Balancer:     &kafka.LeastBytes{},
		},
	}
}

// Close closes the Kafka writer, ensuring all messages are sent.
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

// SendMessage asynchronously sends a message to Kafka.
func (p *KafkaProducer) SendMessage(ctx context.Context, key, value []byte) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}
	return p.writer.WriteMessages(ctx, msg)
}

// BenchmarkProducer benchmarks the Kafka producer over a specific duration.
func BenchmarkProducer(producer *KafkaProducer, runDuration time.Duration, messagesPerSecond int, wg *sync.WaitGroup) {
	defer wg.Done()

	// Compute interval between messages
	interval := time.Second / time.Duration(messagesPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	endTime := time.Now().Add(runDuration)

	for time.Now().Before(endTime) {
		<-ticker.C // Tick according to the messages per second

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		key := []byte(fmt.Sprintf("key-%d", rand.Intn(1000)))
		value := []byte("This is a test message")

		if err := producer.SendMessage(ctx, key, value); err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			log.Println("Message sent successfully")
		}
	}
}

// main is the entry point of the program.
// It initializes the producer and runs the benchmark.
func main() {
	// Kafka configuration
	config := ProducerConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "example-topic",
		BatchSize:    100,
		BatchTimeout: 100 * time.Millisecond,
	}

	producer := NewKafkaProducer(config)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Failed to close producer: %v", err)
		}
	}()

	benchmarkDuration := 10 * time.Second
	messagesPerSecond := 10
	var wg sync.WaitGroup

	wg.Add(1)

	// Run producer benchmark
	go BenchmarkProducer(producer, benchmarkDuration, messagesPerSecond, &wg)

	wg.Wait()

	log.Println("Producer benchmark completed.")
}
