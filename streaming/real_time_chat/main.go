package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// messageBrokerConfig encapsulates the settings for Kafka communication.
var messageBrokerConfig = struct {
	BrokerAddress string
	Topic         string
}{
	BrokerAddress: "localhost:9092",
	Topic:         "chat-topic",
}

func main() {
	// Channel to handle OS signals for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the consumer in a separate goroutine
	go startConsumer()

	// Start the producer for sending messages
	startProducer(signalChan)
}

// startConsumer initializes Kafka consumer to listen for incoming messages and output them to the console.
func startConsumer() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{messageBrokerConfig.BrokerAddress},
		Topic:    messageBrokerConfig.Topic,
		GroupID:  "chat-consumer-group", // Use a group ID for consumer groups
		MinBytes: 10e3,                  // 10KB
		MaxBytes: 10e6,                  // 10MB
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Fatalf("Failed to close reader: %v", err)
		}
	}()

	fmt.Println("Consumer started")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("Could not read message: %v", err)
		}
		fmt.Printf("Received message: %s\n", string(msg.Value))
	}
}

// startProducer initializes the Kafka producer to send messages and listens for termination signals.
func startProducer(signalChan chan os.Signal) {
	writer := kafka.Writer{
		Addr:     kafka.TCP(messageBrokerConfig.BrokerAddress),
		Topic:    messageBrokerConfig.Topic,
		Balancer: &kafka.LeastBytes{}, // Distribute messages based on partition size
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Fatalf("Failed to close writer: %v", err)
		}
	}()

	fmt.Println("Producer started")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messageNum := 0

	for {
		select {
		case <-signalChan:
			log.Println("Termination signal received, shutting down gracefully...")
			return
		default:
			// Create an example message
			message := kafka.Message{
				Key:   []byte(fmt.Sprintf("key-%d", messageNum)),
				Value: []byte(fmt.Sprintf("Hello Kafka %d", messageNum)),
				Time:  time.Now(),
			}

			// Attempt to send the message
			if err := writer.WriteMessages(ctx, message); err != nil {
				log.Fatalf("Failed to write message: %v", err)
			}

			log.Printf("Message %d sent successfully", messageNum)
			messageNum++

			// Simulate a delay between messages
			time.Sleep(2 * time.Second)
		}
	}
}
