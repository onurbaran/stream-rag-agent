package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"stream-rag-agent/internal/config"
)

type OllamaGenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type OllamaGenerateResponse struct {
	Response string `json:"response"`
}

type Service struct {
	ollamaURL  string
	llmModel   string
	httpClient *http.Client
}

func NewService(cfg *config.OllamaConfig) *Service {
	return &Service{
		ollamaURL: cfg.URL,
		llmModel:  cfg.LLMModel,
		httpClient: &http.Client{
			Timeout: 120 * time.Second, // LLM calls can take longer
		},
	}
}

func (s *Service) GenerateContent(prompt string) (string, error) {
	reqBody, err := json.Marshal(OllamaGenerateRequest{
		Model:  s.llmModel,
		Prompt: prompt,
		Stream: false,
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal ollama generate request: %w", err)
	}

	url := fmt.Sprintf("%s/api/generate", s.ollamaURL)
	resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("failed to call ollama generate API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ollama generate API returned non-OK status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var genResp OllamaGenerateResponse
	if err := json.NewDecoder(resp.Body).Decode(&genResp); err != nil {
		return "", fmt.Errorf("failed to decode ollama generate response: %w", err)
	}

	return genResp.Response, nil
}
