package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// main function is the entry point of the Kafka Streams API example.
// This example will create a Kafka consumer to read messages from a topic,
// process the messages (transform them in this case), and output processed messages.
func main() {
	// Kafka broker address
	brokerAddress := "localhost:9092"
	// Kafka topic to consume from
	inputTopic := "input-topic"
	// Kafka topic to produce to
	outputTopic := "output-topic"

	// Setting up a Kafka reader (consumer) for the input topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     inputTopic,
		Partition: 0,
		// GroupID enables consumer groups for load balancing across multiple consumers
		GroupID: "my-group",
	})
	defer reader.Close()

	// Setting up a Kafka writer (producer) for the output topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress},
		Topic:    outputTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Channel to listen for interrupt or termination signal
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Starting Kafka stream processing...")

	// Loop to continuously read messages from Kafka
	for {
		select {
		case <-signals:
			log.Println("Received signal, shutting down...")
			return
		default:
			// Reading a single message from Kafka
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Failed to read message: %v", err)
				continue
			}

			log.Printf("Received message: %s", string(msg.Value))

			// Simulating message processing by transforming it
			processedMessage := transformMessage(msg.Value)

			// Sending the processed message to the output topic
			err = writer.WriteMessages(context.Background(), kafka.Message{
				Key:   msg.Key,
				Value: processedMessage,
				Time:  time.Now(),
			})
			if err != nil {
				log.Printf("Failed to write message: %v", err)
			} else {
				log.Printf("Processed and sent message: %s", string(processedMessage))
			}
		}
	}
}

// transformMessage performs a simple transformation on the input message value.
// In a real-world scenario, this could perform more complex processing logic.
func transformMessage(value []byte) []byte {
	// Converting message to uppercase as a simple transformation
	return []byte(string(value)) // Replace with actual transformation logic
}
