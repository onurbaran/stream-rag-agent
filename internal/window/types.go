package window

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

type RawKafkaMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

type Window struct {
	ID           string // Unique ID for this window (e.g., topic_partition_offset)
	Topic        string
	Partition    int32
	StartTime    time.Time
	EndTime      time.Time
	Messages     []RawKafkaMessage
	Context      string // Context provided for the topic from config file
	IsClosed     bool
	MessageCount int
}

func NewWindow(topic string, partition int32, startTime time.Time, topicContext string) *Window {
	return &Window{
		ID:           fmt.Sprintf("%s_%d_%d", topic, partition, startTime.UnixNano()),
		Topic:        topic,
		Partition:    partition,
		StartTime:    startTime,
		Context:      topicContext,
		Messages:     make([]RawKafkaMessage, 0),
		IsClosed:     false,
		MessageCount: 0,
	}
}

func (w *Window) AddMessage(msg RawKafkaMessage) {
	w.Messages = append(w.Messages, msg)
	w.MessageCount++
	w.EndTime = msg.Timestamp // Update end time with the latest message
}

func (w *Window) ToContextString() (string, error) {
	if len(w.Messages) == 0 {
		return fmt.Sprintf("Topic: %s, Window ID: %s, No messages in this window.", w.Topic, w.ID), nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Kafka Topic: %s\n", w.Topic))
	sb.WriteString(fmt.Sprintf("Topic Context: %s\n", w.Context))
	sb.WriteString(fmt.Sprintf("Window ID: %s, Time Range: %s - %s, Total Messages: %d\n",
		w.ID, w.StartTime.Format(time.RFC3339), w.EndTime.Format(time.RFC3339), w.MessageCount))
	sb.WriteString("Messages:\n")

	maxSummarizeMessages := 10
	if w.MessageCount < maxSummarizeMessages {
		maxSummarizeMessages = w.MessageCount
	}

	for i := 0; i < maxSummarizeMessages; i++ {
		msg := w.Messages[i]
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			log.Printf("Warning: Could not unmarshal message (Offset: %d) as JSON: %v. Using raw string.\n", msg.Offset, err)
			sb.WriteString(fmt.Sprintf("  - Raw Message (Offset: %d): %s\n", msg.Offset, string(msg.Value)))
		} else {
			sb.WriteString(fmt.Sprintf("  - Message (Offset: %d) Details:\n", msg.Offset))

			for k, v := range data {
				sb.WriteString(fmt.Sprintf("    - %s: %v\n", k, v))
			}

		}
	}

	if w.MessageCount > maxSummarizeMessages {
		sb.WriteString(fmt.Sprintf("  ...and %d more messages (truncated for summary).\n", w.MessageCount-maxSummarizeMessages))
	}

	return sb.String(), nil
}

type EmbeddedWindow struct {
	WindowID      string            `json:"window_id"`
	Topic         string            `json:"topic"`
	Partition     int32             `json:"partition"`
	StartTime     time.Time         `json:"start_time"`
	EndTime       time.Time         `json:"end_time"`
	MessageCount  int               `json:"message_count"`
	ContextText   string            `json:"context_text"`             // The text that was embedded
	Embedding     []float32         `json:"embedding"`                // The vector embedding
	KafkaMessages []RawKafkaMessage `json:"kafka_messages,omitempty"` // Store raw messages if needed, or just their IDs
}
