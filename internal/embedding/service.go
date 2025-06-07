package embedding

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"stream-rag-agent/internal/config"
)

type OllamaEmbedRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type OllamaEmbedResponse struct {
	Embedding []float32 `json:"embedding"`
}

type Service struct {
	ollamaURL      string
	embeddingModel string
	httpClient     *http.Client
}

func NewService(cfg *config.OllamaConfig) *Service {
	return &Service{
		ollamaURL:      cfg.URL,
		embeddingModel: cfg.EmbeddingModel,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (s *Service) GetEmbedding(text string) ([]float32, error) {
	reqBody, err := json.Marshal(OllamaEmbedRequest{
		Model:  s.embeddingModel,
		Prompt: text,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ollama embed request: %w", err)
	}

	url := fmt.Sprintf("%s/api/embeddings", s.ollamaURL)
	resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to call ollama embeddings API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama embeddings API returned non-OK status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var embedResp OllamaEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&embedResp); err != nil {
		return nil, fmt.Errorf("failed to decode ollama embed response: %w", err)
	}

	return embedResp.Embedding, nil
}
