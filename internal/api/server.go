package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"stream-rag-agent/internal/embedding"
	"stream-rag-agent/internal/llm"
	"stream-rag-agent/internal/vectordb"
	"stream-rag-agent/internal/window"
)

type APIServer struct {
	httpServer       *http.Server
	embeddingService *embedding.Service
	llmService       *llm.Service
	esClient         *vectordb.ElasticsearchClient
}

type QueryRequest struct {
	Prompt string `json:"prompt"`
}

type QueryResponse struct {
	Answer string `json:"answer"`
	Error  string `json:"error,omitempty"`
}

func NewAPIServer(embedSvc *embedding.Service, llmSvc *llm.Service, esClient *vectordb.ElasticsearchClient) *APIServer {
	mux := http.NewServeMux()
	server := &APIServer{
		embeddingService: embedSvc,
		llmService:       llmSvc,
		esClient:         esClient,
		httpServer: &http.Server{
			Addr:         ":8080",
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
	}

	mux.HandleFunc("/query", server.handleQuery)
	mux.HandleFunc("/health", server.handleHealth)
	return server
}

func (s *APIServer) Start() error {
	log.Printf("API server starting on %s", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

func (s *APIServer) Shutdown(ctx context.Context) error {
	log.Println("API server shutting down...")
	return s.httpServer.Shutdown(ctx)
}

func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *APIServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Prompt == "" {
		http.Error(w, "Prompt cannot be empty", http.StatusBadRequest)
		return
	}

	log.Printf("Received query: %s", req.Prompt)

	// 1. Get embedding for the user's prompt
	queryEmbedding, err := s.embeddingService.GetEmbedding(req.Prompt)
	if err != nil {
		log.Printf("Error getting embedding for prompt '%s': %v", req.Prompt, err)
		writeJSONResponse(w, http.StatusInternalServerError, QueryResponse{Error: "Failed to embed prompt"})
		return
	}

	// 2. Search for similar windows in Elasticsearch
	// Adjust 'k' (number of results) as needed for context size vs. LLM token limit
	topK := 5 // Retrieve top 5 most similar windows
	similarWindows, err := s.esClient.SearchSimilarWindows(queryEmbedding, topK)
	if err != nil {
		log.Printf("Error searching similar windows in Elasticsearch: %v", err)
		writeJSONResponse(w, http.StatusInternalServerError, QueryResponse{Error: "Failed to retrieve relevant context"})
		return
	}

	// 3. Construct RAG prompt with retrieved context
	ragPrompt := buildRAGPrompt(req.Prompt, similarWindows)
	log.Printf("Sending RAG prompt to LLM (truncated): %s...", ragPrompt[:min(len(ragPrompt), 500)])

	// 4. Generate LLM response
	llmAnswer, err := s.llmService.GenerateContent(ragPrompt)
	if err != nil {
		log.Printf("Error generating LLM content: %v", err)
		writeJSONResponse(w, http.StatusInternalServerError, QueryResponse{Error: "Failed to generate LLM response"})
		return
	}

	log.Printf("Successfully generated LLM answer for query: %s", req.Prompt)
	writeJSONResponse(w, http.StatusOK, QueryResponse{Answer: llmAnswer})
}

// buildRAGPrompt constructs the prompt to be sent to the LLM, including retrieved context.
func buildRAGPrompt(userPrompt string, contextWindows []window.EmbeddedWindow) string {
	var sb strings.Builder
	sb.WriteString("You are an AI assistant specialized in analyzing Kafka streaming data. ")
	sb.WriteString("Use the provided data from Kafka topics to answer the user's question. ")
	sb.WriteString("If the answer is not in the provided data, state that you don't have enough information. ")
	sb.WriteString("Do NOT make up information.\n\n")

	sb.WriteString("--- RELEVANT KAFKA DATA ---\n")
	if len(contextWindows) == 0 {
		sb.WriteString("No relevant Kafka data found.\n")
	} else {
		for i, w := range contextWindows {
			sb.WriteString(fmt.Sprintf("--- Window %d (Topic: %s, ID: %s) ---\n", i+1, w.Topic, w.WindowID))
			sb.WriteString(w.ContextText) // summarized text from Kafka window
			sb.WriteString("\n\n")
		}
	}
	sb.WriteString("--------------------------\n\n")
	sb.WriteString("USER QUESTION: ")
	sb.WriteString(userPrompt)
	sb.WriteString("\n")

	return sb.String()
}

func writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error writing JSON response: %v", err)
	}
}
