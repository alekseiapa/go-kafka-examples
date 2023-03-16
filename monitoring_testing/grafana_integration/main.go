package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

// This program demonstrates how to integrate Kafka metrics with Grafana using Prometheus for real-time visualization.
// It simulates Kafka producer and consumer workloads, collects metrics, and exposes them via an HTTP server for Prometheus to scrape.

// Kafka configuration constants
const (
	brokerAddress = "localhost:9092"
	topic         = "metrics-topic"
)

// Prometheus metrics
var (
	producedMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "produced_messages_total",
			Help: "Total number of messages produced",
		},
		[]string{"status"},
	)

	consumedMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumed_messages_total",
			Help: "Total number of messages consumed",
		},
		[]string{"status"},
	)
)

func init() {
	// Register the metrics with Prometheus's default registry
	prometheus.MustRegister(producedMessages)
	prometheus.MustRegister(consumedMessages)
}

// main is the entry point to the program. It sets up Kafka producers and consumers and starts the HTTP server for metrics.
func main() {
	go startProducer()
	go startConsumer()
	startMetricsServer()
}

// startProducer sets up a Kafka producer and sends messages periodically.
func startProducer() {
	// Create a new Kafka writer (producer)
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})
	defer writer.Close()

	for {
		// Create a test message to send
		message := kafka.Message{
			Key:   []byte("key-1"),
			Value: []byte("Test message"),
		}

		// Context with timeout to prevent hanging indefinitely
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Send the message
		if err := writer.WriteMessages(ctx, message); err != nil {
			log.Printf("Failed to produce message: %v", err)
			producedMessages.WithLabelValues("failed").Inc()
		} else {
			producedMessages.WithLabelValues("success").Inc()
			log.Println("Message produced successfully")
		}
		// Wait before sending the next message
		time.Sleep(2 * time.Second)
	}
}

// startConsumer sets up a Kafka consumer and consumes messages, updating the metrics as it goes.
func startConsumer() {
	// Create a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	for {
		// Read a message from Kafka
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Failed to consume message: %v", err)
			consumedMessages.WithLabelValues("failed").Inc()
			continue
		}
		log.Printf("Message consumed: %s", string(message.Value))
		consumedMessages.WithLabelValues("success").Inc()
	}
}

// startMetricsServer starts an HTTP server to expose metrics for Prometheus.
func startMetricsServer() {
	// Set up a handler for the /metrics endpoint
	http.Handle("/metrics", promhttp.Handler())

	// Start the HTTP server
	log.Println("Starting HTTP server on :8080 for metrics...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
