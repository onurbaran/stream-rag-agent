package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type KafkaTopicConfig struct {
	Name                  string `yaml:"name"`
	Context               string `yaml:"context"`
	WindowDurationSeconds int    `yaml:"window_duration_seconds"`
	WindowMaxMessages     int    `yaml:"window_max_messages"`
}

type KafkaConfig struct {
	Brokers         []string           `yaml:"brokers"`
	ConsumerGroupID string             `yaml:"consumer_group_id"`
	Topics          []KafkaTopicConfig `yaml:"topics"`
}

type OllamaConfig struct {
	URL            string `yaml:"url"`
	EmbeddingModel string `yaml:"embedding_model"`
	LLMModel       string `yaml:"llm_model"`
}

type ElasticsearchConfig struct {
	Addresses []string `yaml:"addresses"`
	IndexName string   `yaml:"index_name"`
}

type AppConfig struct {
	Kafka         KafkaConfig         `yaml:"kafka"`
	Ollama        OllamaConfig        `yaml:"ollama"`
	Elasticsearch ElasticsearchConfig `yaml:"elasticsearch"`
}

func LoadConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg AppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}
