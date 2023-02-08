package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

// consumerConfig holds configurations for the Kafka consumer.
type consumerConfig struct {
	brokers    []string
	topic      string
	groupID    string
	maxRetries int
}

// backoff contains the delay configuration for retries.
type backoff struct {
	initialInterval time.Duration
	maxInterval     time.Duration
}

type kafkaConsumer struct {
	reader  *kafka.Reader
	backoff backoff
	config  consumerConfig
}

// newKafkaConsumer initializes a new Kafka reader with the specified configurations.
func newKafkaConsumer(config consumerConfig) *kafkaConsumer {
	return &kafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: config.brokers,
			Topic:   config.topic,
			GroupID: config.groupID,
		}),
		backoff: backoff{
			initialInterval: 1 * time.Second,
			maxInterval:     10 * time.Second,
		},
		config: config,
	}
}

// start consumes messages from Kafka and processes them with retry logic.
func (kc *kafkaConsumer) start(ctx context.Context) {
	defer kc.reader.Close()
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping consumer.")
			return
		default:
			msg, err := kc.reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("Error fetching message: %v", err)
				continue
			}
			// Process the message with retry logic
			retries := 0
			for {
				err = kc.processMessage(ctx, msg)
				if err == nil {
					log.Printf("Message processed successfully: %s", string(msg.Value))
					break
				}
				if retries >= kc.config.maxRetries {
					log.Printf("Max retries reached. Skipping message: %s", string(msg.Value))
					break
				}
				// Apply backoff delay
				delay := kc.calculateBackoffDelay(retries)
				log.Printf("Transient error: %v. Retrying in %v...", err, delay)
				time.Sleep(delay)
				retries++
			}
			// Commit the message offset after processing
			if err := kc.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
		}
	}
}

// processMessage simulates message processing which may fail.transient errors.
func (kc *kafkaConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	// Simulate random processing errors
	random := rand.Intn(10)
	if random < 7 { // 70% chance of a transient error
		return errors.New("simulated transient error")
	}
	// Otherwise processing is successful
	log.Printf("Processing message: %s", msg.Value)
	return nil
}

// calculateBackoffDelay calculates the delay before the next retry, capped by a max delay.
func (kc *kafkaConsumer) calculateBackoffDelay(retries int) time.Duration {
	delay := kc.backoff.initialInterval << retries
	if delay > kc.backoff.maxInterval {
		delay = kc.backoff.maxInterval
	}
	return delay
}

func main() {
	// Kafka broker configuration
	brokers := []string{"localhost:9092"}
	consumer := newKafkaConsumer(consumerConfig{
		brokers:    brokers,
		topic:      "example-topic",
		groupID:    "example-group",
		maxRetries: 5,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumer.start(ctx)

	// Simulate running the consumer
	time.Sleep(60 * time.Second)

	log.Println("Shutting down consumer...")
	// Cancel the context to stop the consumer
	cancel()
}
