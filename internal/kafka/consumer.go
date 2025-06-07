package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"stream-rag-agent/internal/config"
	"stream-rag-agent/internal/window"
)

type Consumer struct {
	reader *kafka.Reader
	config config.KafkaTopicConfig
	wm     *window.Manager // Window Manager for this topic's messages
}

func NewConsumer(cfg config.KafkaTopicConfig, consumerGroupID string, brokers []string, wm *window.Manager) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  consumerGroupID,
		Topic:    cfg.Name,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  1 * time.Second,
	})
	return &Consumer{
		reader: reader,
		config: cfg,
		wm:     wm,
	}
}

func (c *Consumer) StartConsuming(ctx context.Context, partition int32) {
	log.Printf("Starting Kafka consumer for topic: %s, partition: %d", c.config.Name, partition)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping Kafka consumer for topic: %s, partition: %d", c.config.Name, partition)
			return
		default:
			msg, err := c.reader.FetchMessage(ctx) // Fetch one message
			if err != nil {
				log.Printf("Error fetching message from Kafka topic %s: %v", c.config.Name, err)
				if ctx.Err() != nil {
					return
				}
				time.Sleep(time.Second)
				continue
			}

			kafkaMsg := window.RawKafkaMessage{
				Topic:     msg.Topic,
				Partition: int32(msg.Partition),
				Offset:    msg.Offset,
				Key:       msg.Key,
				Value:     msg.Value,
				Timestamp: msg.Time,
			}
			c.wm.AddMessage(kafkaMsg)

			// Commit
			err = c.reader.CommitMessages(ctx, msg)
			if err != nil {
				log.Printf("Error committing offset for topic %s, partition %d, offset %d: %v", msg.Topic, msg.Partition, msg.Offset, err)
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
