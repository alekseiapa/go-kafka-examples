package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
)

// SchemaRegistryConfig holds configuration details for the schema registry
// connection.
type SchemaRegistryConfig struct {
	URL string
}

// KafkaConfig holds configuration details for Kafka connection.
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

// Example Avro schema.
const avroSchema = `{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}`

// User is a representation of the data matching the Avro schema.
type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

// createKafkaWriter initializes a new Kafka writer for producing messages.
func createKafkaWriter(conf KafkaConfig) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  conf.Brokers,
		Topic:    conf.Topic,
		Balancer: &kafka.LeastBytes{},
	})
}

// produceMessage sends a User message encoded with Avro to the Kafka topic.
func produceMessage(ctx context.Context, writer *kafka.Writer, codec *goavro.Codec, user User) error {
	// Encode user data to conform to the Avro schema
	dataMap := map[string]interface{}{
		"name": user.Name,
		"age":  user.Age,
	}
	avroData, err := codec.BinaryFromNative(nil, dataMap)
	if err != nil {
		return fmt.Errorf("failed to encode user data: %w", err)
	}

	// Create message with Avro encoded data
	message := kafka.Message{
		Value: avroData,
	}

	// Send the Avro-encoded message to Kafka
	return writer.WriteMessages(ctx, message)
}

// consumeMessage reads messages from Kafka and decodes them using the provided Avro schema.
func consumeMessage(ctx context.Context, reader *kafka.Reader, codec *goavro.Codec) (*User, error) {
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	// Decode message data using Avro
	nativeData, _, err := codec.NativeFromBinary(msg.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message data: %w", err)
	}

	// Convert decoded data to User struct
	userData, err := json.Marshal(nativeData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal decoded data: %w", err)
	}

	var user User
	err = json.Unmarshal(userData, &user)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal to user struct: %w", err)
	}

	return &user, nil
}

func main() {
	// Configure Kafka details
	kafkaConfig := KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "example-topic",
	}

	// Create Avro codec based on schema
	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		log.Fatalf("failed to create Avro codec: %v", err)
	}

	// Initialize a Kafka writer (producer)
	writer := createKafkaWriter(kafkaConfig)
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce a test message
	user := User{Name: "Alice", Age: 30}
	log.Println("Producing message...")
	err = produceMessage(ctx, writer, codec, user)
	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}
	log.Println("Message successfully produced.")

	// Set up Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: kafkaConfig.Brokers,
		Topic:   kafkaConfig.Topic,
		GroupID: "example-group",
	})
	defer reader.Close()

	// Consume a test message
	log.Println("Consuming message...")
	consumedUser, err := consumeMessage(ctx, reader, codec)
	if err != nil {
		log.Fatalf("Failed to consume message: %v", err)
	}
	log.Printf("Message consumed: %+v", consumedUser)
}