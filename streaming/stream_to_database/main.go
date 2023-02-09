package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

// main is the entry point of the program demonstrating real-time data integration
// from Kafka to a PostgreSQL database.
func main() {
	// Kafka configuration
	brokerAddress := "localhost:9092"
	topic := "example-topic"
	dbConnStr := "postgres://username:password@localhost/dbname?sslmode=disable"

	// Kafka reader configuration for consuming messages
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	// Database connection
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatalf("failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Ensure database connection is alive
	err = db.Ping()
	if err != nil {
		log.Fatalf("failed to ping the database: %v", err)
	}

	fmt.Println("Connected to Kafka and database, starting to read messages...")

	// Infinite loop to continuously read from Kafka	handle" functions
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("failed to read message: %v", err)
			continue
		}

		fmt.Printf("Message received: Key=%s, Value=%s", string(msg.Key), string(msg.Value))

		// Example of simple data storage
		query := "INSERT INTO messages (key, value) VALUES ($1, $2)"
		_, err = db.Exec(query, string(msg.Key), string(msg.Value))
		if err != nil {
			log.Printf("failed to execute query: %v", err)
		} else {
			fmt.Println("Message stored in database")
		}
	}
}

// handleError handles error logging in a consistent manner.
// It demonstrates structured error handling as a best practice in Go.
func handleError(err error, message string) {
	if err != nil {
		log.Fatalf("%s: %v", message, err)
	}
}
