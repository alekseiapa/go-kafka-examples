package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/attribute"
	"github.com/segmentio/kafka-go"
)

// SetupTracing configures OpenTelemetry with a stdout exporter for demonstration.
func SetupTracing() func() {
	exporter, _ := stdouttrace.New(stdouttrace.WithPrettyPrint())
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(tp)

	// Return a function to ensure cleanup is handled at program exit
	return func() {
		_ = tp.Shutdown(context.Background())
	}
}

// createKafkaWriter initializes a new Kafka writer (producer) with the given broker address and topic.
func createKafkaWriter(brokerAddress, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

// sendMessage sends a message to a Kafka topic with distributed tracing.
func sendMessage(ctx context.Context, writer *kafka.Writer, tracer trace.Tracer) error {
	// Start a new span for the message sending operation
	ctx, span := tracer.Start(ctx, "send-message")
	defer span.End()

	message := kafka.Message{
		Key:   []byte("key-1"),
		Value: []byte("Tracing Kafka message flow with OpenTelemetry."),
		Time:  time.Now(),
	}

	// Add attributes to the span for better tracing
	span.SetAttributes(attribute.String("kafka.key", string(message.Key)))
	span.SetAttributes(attribute.String("kafka.value", string(message.Value)))

	log.Println("Sending message to Kafka...")
	if err := writer.WriteMessages(ctx, message); err != nil {
		span.RecordError(err)
		log.Fatalf("Failed to write message to Kafka: %v", err)
		return err
	}

	log.Println("Message successfully sent to Kafka.")
	return nil
}

// main function demonstrates Kafka producer and consumer with distributed tracing using OpenTelemetry.
func main() {
	// Broker and topic configuration
	brokerAddress := "localhost:9092"
	topic := "distributed-tracing-topic"

	// Setup OpenTelemetry tracing and defer cleanup
	cleanup := SetupTracing()
	defer cleanup()

	// Get a tracer, used to start spans
	tracer := otel.Tracer("kafka-producer")

	// Initialize Kafka writer
	writer := createKafkaWriter(brokerAddress, topic)
	defer writer.Close()

	// Create a context with a timeout for sending the message
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send a test message to Kafka with tracing
	if err := sendMessage(ctx, writer, tracer); err != nil {
		log.Fatal(err)
	}

	// Optional: Log completion message
	fmt.Println("Kafka producer with tracing setup complete. Exiting.")
}