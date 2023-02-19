package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaMessage represents the structure of the message to be sent to the REST API.
type KafkaMessage struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// main is the entry point of the program. It sets up a Kafka consumer to listen for messages and sends them to a REST API.
func main() {
	brokerAddress := "localhost:9092"
	topic := "example-topic"

	// Initialize a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		GroupID:  "example-group",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	defer func() {
		if err := reader.Close(); err != nil {
			log.Fatalf("failed to close reader: %v", err)
		}
	}()

	log.Println("Listening for messages...")

	for {
		// Read message from Kafka
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("failed to read message: %v", err)
		}

		log.Printf("received: %s = %s\n", string(m.Key), string(m.Value))

		// Create a KafkaMessage instance from the received message
		kafkaMessage := KafkaMessage{
			Key:   string(m.Key),
			Value: string(m.Value),
		}

		// Send the message to the REST API
		sendMessageToAPI(kafkaMessage)
	}
}

// sendMessageToAPI sends a Kafka message to a REST API for further processing or storage.
func sendMessageToAPI(msg KafkaMessage) {
	apiURL := "http://localhost:8080/messages"

	// Marshal the KafkaMessage object into JSON
	jsonData, err := json.Marshal(msg)
	if err != nil {
		log.Printf("failed to marshal message: %v", err)
		return
	}

	// Create a new HTTP request to send the JSON data
	req, err := http.NewRequest(http.MethodPost, apiURL, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("failed to create request: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	// Execute the HTTP request
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("failed to send request: %v", err)
		return
	}
	defer resp.Body.Close()

	// Check if the request was successful
	if resp.StatusCode != http.StatusOK {
		log.Printf("failed to send message, server responded with status: %d", resp.StatusCode)
	} else {
		log.Println("message successfully sent to the REST API")
	}
}

// Ensure your Kafka service and REST endpoint are running before testing this program.
// The Kafka brokerAddress should be modified according to your environment settings.
// Similarly, update the apiURL to the correct endpoint you are testing against.
