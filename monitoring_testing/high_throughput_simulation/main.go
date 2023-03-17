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

// ProduceMessages simulates high-throughput message production to a Kafka topic.
// It incrementally increases the number of messages sent to Kafka.
func ProduceMessages(writer *kafka.Writer, wg *sync.WaitGroup, done chan bool) {
	defer wg.Done()
	messageCount := 1000 // Number of messages to simulate high throughput

	for i := 0; i < messageCount; i++ {
		select {
		case <-done:
			return
		default:
		}

		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", i)),
			Value: []byte(fmt.Sprintf("This is message %d", i)),
		}

		// Context with timeout for each message
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Try to write message to Kafka
		if err := writer.WriteMessages(ctx, message); err != nil {
			log.Printf("Could not write message %d to Kafka: %v", i, err)
		}
	}
}

// ConsumeMessages reads messages from a Kafka topic to simulate high-throughput consumption.
func ConsumeMessages(reader *kafka.Reader, wg *sync.WaitGroup, done chan bool) {
	defer wg.Done()

	for {
		select {
		case <-done:
			return
		default:
		}

		// Read the next message
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Could not read message: %v", err)
			continue
		}
		log.Printf("Consumed message: %s = %s\n", string(msg.Key), string(msg.Value))
	}
}

func main() {
	// Configure Kafka broker addresses
	brokerAddress := "localhost:9092"
	// Kafka topic to produce and consume messages
	topic := "high-throughput-topic"

	// Initialize Kafka writer (producer) using the latest API
	writer := kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Load balancing
	}
	defer writer.Close()

	// Initialize Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		Partition: 0,
	})
	defer reader.Close()

	// Wait group for goroutine synchronization
	wg := sync.WaitGroup{}

	// Channel for stopping the goroutines
	done := make(chan bool)

	// Start producer and consumer as goroutines
	wg.Add(2)
	go ProduceMessages(&writer, &wg, done)
	go ConsumeMessages(reader, &wg, done)

	// Capture OS signals for clean shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	<-signalChan
	log.Println("Stopping producer and consumer...")
	close(done)

	// Wait for all goroutines to finish
	wg.Wait()
	log.Println("Simulation finished.")
}
