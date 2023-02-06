package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// main demonstrates Kafka transactions to achieve exactly-once semantics.
func main() {
	// Kafka broker and topic configuration
	broker := "localhost:9092"
	topic := "example-topic"

	// Create a transactional producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        broker,
		"transactional.id":         "transactional-id-example",
		"enable.idempotence":       true, // Ensures exactly-once delivery
		"message.send.max.retries": 3,
		"acks":                     "all",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Initialize transactions
	err = producer.InitTransactions(nil)
	if err != nil {
		log.Fatalf("Failed to initialize transactions: %v", err)
	}

	// Handle graceful shutdown on signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Caught signal %v: terminating", sig)
		producer.Close()
		os.Exit(0)
	}()

	// Begin a transaction
	log.Println("Beginning transaction...")
	err = producer.BeginTransaction()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a test message
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("key-1"),
		Value:          []byte("Hello, Kafka! This message is part of a transactional batch."),
		Timestamp:      time.Now(),
	}

	// Produce the message within the transaction
	log.Println("Producing transactional message...")
	err = producer.Produce(message, nil)
	if err != nil {
		log.Printf("Error occurred while producing message: %v", err)
		log.Println("Aborting transaction...")
		err = producer.AbortTransaction(nil)
		if err != nil {
			log.Fatalf("Failed to abort transaction: %v", err)
		}
		log.Fatalf("Transaction aborted due to error: %v", err)
	}

	// Commit the transaction
	log.Println("Committing transaction...")
	err = producer.CommitTransaction(nil)
	if err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	log.Println("Transaction committed successfully, ensuring exactly-once message delivery.")
}
