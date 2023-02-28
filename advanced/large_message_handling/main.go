package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

// largeMessageHandler is the main structure that will handle the large messages
// It abstracts Kafka producer and consumer
// It uses compression to handle large messages efficiently
// It uses temporary storage to optimize broker load

type largeMessageHandler struct {
	producer *kafka.Writer
	consumer *kafka.Reader
}

func newLargeMessageHandler(brokerAddress, topic string) *largeMessageHandler {
	return &largeMessageHandler{
		producer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{brokerAddress},
			Topic:   topic,
			// Use LeastBytes to distribute load among partitions
			// This ensures efficient handling of messages
			Balancer: &kafka.LeastBytes{},
		}),
		consumer: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{brokerAddress},
			Topic:   topic,
			GroupID: "large-message-handler-group",
			// Auto offset reset to earliest ensures no message is missed
			StartOffset: kafka.FirstOffset,
		}),
	}
}

// compressMessage compresses the input data using gzip
// This function reduces the size of large messages for more efficient handling
func compressMessage(data string) ([]byte, error) {
	var b strings.Builder
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write([]byte(data)); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

// decompressMessage decompresses gzip-compressed data
// This is used to retrieve the original message from storage
func decompressMessage(data []byte) (string, error) {
	gz, err := gzip.NewReader(strings.NewReader(string(data)))
	if err != nil {
		return "", err
	}
	defer gz.Close()

	res, err := ioutil.ReadAll(gz)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// storeMessageToFile writes the compressed message to a temporary file
// This simulates storing oversized messages
func storeMessageToFile(data []byte) (string, error) {
	file, err := ioutil.TempFile(".", "msg-*.gz")
	if err != nil {
		return "", err
	}
	if _, err := file.Write(data); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return file.Name(), nil
}

// loadMessageFromFile reads the compressed message from the specified file
// This is used to simulate loading oversized messages
func loadMessageFromFile(filename string) ([]byte, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// produce sends a large message to Kafka. It compresses data
// and writes it to a temporary file if too large,
// then only stores the file name in Kafka.
func (h *largeMessageHandler) produce(ctx context.Context, message string) error {
	compressedMessage, err := compressMessage(message)
	if err != nil {
		return fmt.Errorf("failed to compress message: %w", err)
	}

	// Check if the message is too large for Kafka. Assume 1MB is too large
	if len(compressedMessage) > 1024*1024 {
		filename, err := storeMessageToFile(compressedMessage)
		if err != nil {
			return fmt.Errorf("failed to store message: %w", err)
		}
		compressedMessage = []byte(filename) // Store filename in Kafka instead
	}

	return h.producer.WriteMessages(ctx, kafka.Message{
		Value: compressedMessage,
	})
}

// consume reads messages from Kafka. If the message represents
// a file, it loads and decompresses the original message.
func (h *largeMessageHandler) consume(ctx context.Context) error {
	for {
		m, err := h.consumer.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		message := string(m.Value)
		var decompressedMessage string
		if strings.HasPrefix(message, "msg-") {
			// It's a filename, we need to load and decompress the original message

			data, err := loadMessageFromFile(message)
			if err != nil {
				return fmt.Errorf("failed to load file: %w", err)
			}
			decompressedMessage, err = decompressMessage(data)
			if err != nil {
				return fmt.Errorf("failed to decompress message: %w", err)
			}
			// Clean up the temporary file
			os.Remove(message)
		} else {
			// It's already the complete message
			decompressedMessage = message
		}
		fmt.Printf("Consumed: %s", decompressedMessage)
	}
}

func main() {
	ctx := context.Background()
	brokerAddress := "localhost:9092"
	topic := "large-messages"

	handler := newLargeMessageHandler(brokerAddress, topic)

	defer handler.producer.Close()
	defer handler.consumer.Close()

	go func() {
		for {
			if err := handler.consume(ctx); err != nil {
				log.Fatalf("Error consuming messages: %v", err)
			}
		}
	}()

	testMessage := "This is a very large message intended to test compression and file storage for Kafka handling."
	err := handler.produce(ctx, testMessage)
	if err != nil {
		log.Fatalf("Error producing message: %v", err)
	}
	fmt.Println("Produced message successfully.")
}
