package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"stream-rag-agent/internal/api"
	"stream-rag-agent/internal/config"
	"stream-rag-agent/internal/embedding"
	"stream-rag-agent/internal/kafka"
	"stream-rag-agent/internal/llm"
	"stream-rag-agent/internal/vectordb"
	"stream-rag-agent/internal/window"
)

type MainProcessor struct {
	embeddingService *embedding.Service
	esClient         *vectordb.ElasticsearchClient
}

func NewMainProcessor(es *vectordb.ElasticsearchClient, embedSvc *embedding.Service) *MainProcessor {
	return &MainProcessor{
		embeddingService: embedSvc,
		esClient:         es,
	}
}

func (mp *MainProcessor) ProcessWindow(w *window.Window) error {
	log.Printf("Processing window %s (Topic: %s, Messages: %d)", w.ID, w.Topic, w.MessageCount)

	// 1. Convert window messages to a single context string
	contextText, err := w.ToContextString()
	if err != nil {
		return fmt.Errorf("failed to convert window to context string: %w", err)
	}

	// 2. Get embedding from Ollama
	embeddingVector, err := mp.embeddingService.GetEmbedding(contextText)
	if err != nil {
		return fmt.Errorf("failed to get embedding for window %s: %w", w.ID, err)
	}

	// 3. Create EmbeddedWindow struct
	embeddedWindow := &window.EmbeddedWindow{
		WindowID:     w.ID,
		Topic:        w.Topic,
		Partition:    w.Partition,
		StartTime:    w.StartTime,
		EndTime:      w.EndTime,
		MessageCount: w.MessageCount,
		ContextText:  contextText,
		Embedding:    embeddingVector,
	}

	// 4. Save to Elasticsearch
	err = mp.esClient.SaveEmbeddedWindow(embeddedWindow)
	if err != nil {
		return fmt.Errorf("failed to save embedded window to Elasticsearch: %w", err)
	}

	log.Printf("Successfully processed and saved window %s to Elasticsearch.", w.ID)
	return nil
}

func main() {
	cfg, err := config.LoadConfig("/Users/onur.baran/Projects/stream-rag-agent/configs/configs.yml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Setup Services
	esClient, err := vectordb.NewElasticsearchClient(&cfg.Elasticsearch)
	if err != nil {
		log.Fatalf("Failed to initialize Elasticsearch client: %v", err)
	}

	embedSvc := embedding.NewService(&cfg.Ollama)
	llmSvc := llm.NewService(&cfg.Ollama)

	mainProcessor := NewMainProcessor(esClient, embedSvc)

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Start Kafka Consumers and Window Managers
	consumers := []*kafka.Consumer{}
	windowManagers := []*window.Manager{}

	for _, topicCfg := range cfg.Kafka.Topics {
		wm := window.NewManager(topicCfg, mainProcessor)
		windowManagers = append(windowManagers, wm)
		wm.Start(0)

		consumer := kafka.NewConsumer(topicCfg, cfg.Kafka.ConsumerGroupID, cfg.Kafka.Brokers, wm)
		consumers = append(consumers, consumer)

		wg.Add(1)
		go func(c *kafka.Consumer, p int32) {
			defer wg.Done()
			c.StartConsuming(ctx, p)
		}(consumer, 0)
	}

	// Start API Server
	apiServer := api.NewAPIServer(embedSvc, llmSvc, esClient)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := apiServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("API server failed to start: %v", err)
		}
	}()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Println("Shutting down gracefully...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("API server shutdown error: %v", err)
	}

	// Flush any remaining windows before closing
	for _, wm := range windowManagers {
		wm.FlushAllWindows()
	}
	// Give a small grace period for window processing to complete
	time.Sleep(5 * time.Second)

	// Close Kafka consumers
	for _, consumer := range consumers {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing Kafka consumer: %v", err)
		}
	}

	wg.Wait()

	log.Println("Agent stopped.")
}
