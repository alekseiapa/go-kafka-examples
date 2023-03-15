package main

import (
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// KafkaMetricsExporter exports Kafka consumer and producer metrics to Prometheus.
type KafkaMetricsExporter struct {
	producer         sarama.AsyncProducer
	consumer         sarama.Consumer
	inFlightMessages prometheus.Gauge
	consumedMessages prometheus.Counter
}

// NewKafkaMetricsExporter initializes a KafkaMetricsExporter.
func NewKafkaMetricsExporter(producer sarama.AsyncProducer, consumer sarama.Consumer) *KafkaMetricsExporter {
	return &KafkaMetricsExporter{
		producer: producer,
		consumer: consumer,
		inFlightMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kafka_in_flight_messages",
			Help: "Number of messages in flight to Kafka.",
		}),
		consumedMessages: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_consumed_messages",
			Help: "Total number of messages consumed from Kafka.",
		}),
	}
}

// RegisterMetrics registers Prometheus metrics.
func (k *KafkaMetricsExporter) RegisterMetrics() {
	prometheus.MustRegister(k.inFlightMessages)
	prometheus.MustRegister(k.consumedMessages)
}

// ProduceMessage sends a message and updates metrics.
func (k *KafkaMetricsExporter) ProduceMessage(msg string, topic string) {
	k.inFlightMessages.Inc()
	k.producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}

	go func() {
		for range k.producer.Successes() {
			k.inFlightMessages.Dec()
		}
	}()
	go func() {
		for err := range k.producer.Errors() {
			log.Printf("Failed to produce message: %v", err)
			k.inFlightMessages.Dec()
		}
	}()
}

// ConsumeMessages consumes messages and updates metrics.
func (k *KafkaMetricsExporter) ConsumeMessages(topic string, partition int32) {
	pc, err := k.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	go func() {
		for msg := range pc.Messages() {
			log.Printf("Consumed message offset %d: %s", msg.Offset, string(msg.Value))
			k.consumedMessages.Inc()
		}
	}()
}

// RunHTTPServer starts a Prometheus metrics server.
func (k *KafkaMetricsExporter) RunHTTPServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Starting metrics server at %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func main() {
	brokerList := []string{"localhost:9092"}
	topic := "metrics-test"
	partition := int32(0)

	// Create Kafka producer and consumer
	producer, err := newKafkaProducer(brokerList)
	if err != nil {
		log.Fatalf("Failed to setup Kafka producer: %v", err)
	}
	defer producer.Close()

	consumer, err := newKafkaConsumer(brokerList)
	if err != nil {
		log.Fatalf("Failed to setup Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Initialize metrics exporter
	metricsExporter := NewKafkaMetricsExporter(producer, consumer)
	metricsExporter.RegisterMetrics()

	// Produce and consume messages
	metricsExporter.ProduceMessage("Hello, Kafka!", topic)
	metricsExporter.ConsumeMessages(topic, partition)

	// Start the Prometheus metrics server
	metricsExporter.RunHTTPServer(":8080")
}

// newKafkaProducer creates a new asynchronous Kafka producer.
func newKafkaProducer(brokers []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	return sarama.NewAsyncProducer(brokers, config)
}

// newKafkaConsumer creates a new Kafka consumer.
func newKafkaConsumer(brokers []string) (sarama.Consumer, error) {
	return sarama.NewConsumer(brokers, nil)
}
