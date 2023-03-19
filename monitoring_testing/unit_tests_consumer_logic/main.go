package main

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/segmentio/kafka-go"
)

// MockReader simulates a Kafka message reader for testing purposes.
type MockReader struct {
	messages []kafka.Message
	index    int
}

// FetchMessage simulates reading a message from Kafka.
// It returns a message from the predefined list or io.EOF when no more messages are available.
func (m *MockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.index >= len(m.messages) {
		return kafka.Message{}, io.EOF
	}
	msg := m.messages[m.index]
	m.index++
	return msg, nil
}

// Consumer represents a Kafka consumer that processes messages.
type Consumer struct {
	reader *MockReader
}

// NewConsumer initializes a new Consumer with the provided Kafka reader.
func NewConsumer(reader *MockReader) *Consumer {
	return &Consumer{reader: reader}
}

// ConsumeMessages consumes messages from Kafka, processes them, and logs each message.
func (c *Consumer) ConsumeMessages(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		log.Printf("Message received: %s", string(msg.Value))
	}
	return nil
}

// TestConsumeMessages tests the ConsumeMessages function of the Consumer struct.
// It uses a mock reader to simulate message retrieval and ensures messages are consumed as expected.
func TestConsumeMessages(t *testing.T) {
	// Arrange
	mockMessages := []kafka.Message{
		{Value: []byte("message-1")},
		{Value: []byte("message-2")},
		{Value: []byte("message-3")},
	}
	reader := &MockReader{messages: mockMessages}
	consumer := NewConsumer(reader)

	// Act
	err := consumer.ConsumeMessages(context.Background())

	// Assert
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify that all messages are consumed
	if reader.index != len(mockMessages) {
		t.Fatalf("expected %d messages to be consumed, got %d", len(mockMessages), reader.index)
	}
}
