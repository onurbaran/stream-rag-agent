package window

import (
	"fmt"
	"log"
	"sync"
	"time"

	"stream-rag-agent/internal/config"
)

type WindowProcessor interface {
	ProcessWindow(w *Window) error
}

type Manager struct {
	windows      map[string]*Window // Key: topic_partition_id -> Window
	mu           sync.Mutex
	config       config.KafkaTopicConfig
	processor    WindowProcessor
	flushTrigger chan struct{}
}

func NewManager(cfg config.KafkaTopicConfig, processor WindowProcessor) *Manager {
	return &Manager{
		windows:      make(map[string]*Window),
		config:       cfg,
		processor:    processor,
		flushTrigger: make(chan struct{}, 1),
	}
}

func (m *Manager) Start(partition int32) {
	log.Printf("Starting window manager for topic: %s, partition: %d", m.config.Name, partition)

	currentWindow := NewWindow(m.config.Name, partition, time.Now(), m.config.Context)
	m.mu.Lock()
	m.windows[fmt.Sprintf("%s_%d", m.config.Name, partition)] = currentWindow
	m.mu.Unlock()

	// Goroutine for time-based window closing
	go m.timeBasedFlusher(currentWindow)

	// Keep this goroutine alive, messages are added externally.
	// We only need this goroutine to process windows when they close.
}

// AddMessage adds a message to the current window for its topic/partition.
// This is called by the Kafka consumer.
func (m *Manager) AddMessage(msg RawKafkaMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s_%d", msg.Topic, msg.Partition)
	currentWindow, ok := m.windows[key]
	if !ok {
		log.Printf("Warning: No active window for topic %s, partition %d. Creating new.", msg.Topic, msg.Partition)
		currentWindow = NewWindow(msg.Topic, msg.Partition, msg.Timestamp, m.config.Context)
		m.windows[key] = currentWindow
		go m.timeBasedFlusher(currentWindow) // Ensure flusher is running for new window
	}

	currentWindow.AddMessage(msg)

	// Check if message count limit is reached
	if m.config.WindowMaxMessages > 0 && currentWindow.MessageCount >= m.config.WindowMaxMessages {
		log.Printf("Window for %s/%d reached max messages (%d). Closing.", m.config.Name, currentWindow.Partition, currentWindow.MessageCount)
		m.closeWindow(currentWindow)
	}
}

// timeBasedFlusher closes the window after a specified duration.
func (m *Manager) timeBasedFlusher(w *Window) {
	ticker := time.NewTicker(time.Duration(m.config.WindowDurationSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.mu.Lock()
			if !w.IsClosed && time.Since(w.StartTime) >= time.Duration(m.config.WindowDurationSeconds)*time.Second {
				log.Printf("Window for %s/%d timed out (%d sec). Closing.", m.config.Name, w.Partition, m.config.WindowDurationSeconds)
				m.closeWindow(w)
				m.mu.Unlock()
				return
			}
			m.mu.Unlock()
		case <-m.flushTrigger:
			m.mu.Lock()
			if !w.IsClosed {
				log.Printf("Window for %s/%d explicitly flushed. Closing.", m.config.Name, w.Partition)
				m.closeWindow(w)
			}
			m.mu.Unlock()
			return // Stop this flusher goroutine
		}
	}
}

func (m *Manager) closeWindow(w *Window) {
	if w.IsClosed {
		return
	}
	w.IsClosed = true
	w.EndTime = time.Now()

	go func() {
		err := m.processor.ProcessWindow(w)
		if err != nil {
			log.Printf("Error processing window %s: %v", w.ID, err)
		}
		// After processing, remove the closed window and start a new one for continuous streaming
		m.mu.Lock()
		defer m.mu.Unlock()
		key := fmt.Sprintf("%s_%d", w.Topic, w.Partition)
		delete(m.windows, key) // Remove old window
		newWindow := NewWindow(w.Topic, w.Partition, time.Now(), m.config.Context)
		m.windows[key] = newWindow
		go m.timeBasedFlusher(newWindow) // Start flusher for the new window
	}()
}

func (m *Manager) FlushAllWindows() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range m.windows {
		if !w.IsClosed {
			select {
			case m.flushTrigger <- struct{}{}:
			default:
				// Already flushing or channel full, ignore
			}
		}
	}
}
