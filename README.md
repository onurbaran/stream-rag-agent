# Streaming RAG Agent

Streaming Retrieval-Augmented Generation (RAG) agent in Go. It consumes real-time data from Kafka topics, processes it in configurable windows, converts the window content into embeddings using Ollama, and stores these embeddings (along with the original text) in Elasticsearch for near real-time and historical context retrieval by Large Language Models (LLMs).

## Features

* **Kafka Integration:** Consumes messages from multiple Kafka topics.
* **Configurable Windows:** Messages are grouped into time-based or message-count-based windows.
* **Contextualization:** Converts raw JSON Kafka messages into human-readable, contextualized text for embedding.
* **Ollama Integration:** Uses a local Ollama instance for generating text embeddings and LLM responses.
* **Elasticsearch Storage:** Persists embedded window data in Elasticsearch for efficient vector search and filtering.
* **Graceful Shutdown:** Ensures all open windows are processed before the agent stops.

---

## Getting Started

### Prerequisites

* **Go** (1.21 or higher)
* **Kafka** (running and accessible)
* **Ollama** (running locally, with `nomic-embed-text` and `llama3` models pulled (or what if you want))
* **Elasticsearch** (running and accessible)

### Setup

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/onurbaran/stream-rag-agent.git]
    cd stream-rag-agent
    ```

2.  **Initialize Go Module:**
    ```bash
    go mod tidy
    ```

3.  **Configure:**
    Edit the `configs/config.yaml` file to match your Kafka, Ollama, and Elasticsearch settings.

    ```yaml
    # Example snippet from configs/config.yaml
    kafka:
      brokers:
        - localhost:9092
      # ... other kafka configs
    ollama:
      url: http://localhost:11434
      embedding_model: nomic-embed-text
      llm_model: llama3 # Updated to llama3
    elasticsearch:
      addresses:
        - http://localhost:9200
      index_name: rag_embeddings
    ```

4.  **Run Ollama and pull models:**
    Ensure Ollama is running and you have pulled the necessary models:

    ```bash
    ollama pull nomic-embed-text
    ollama pull llama3
    ```

---

## Running the Agent

```bash
go run cmd/agent/main.go
```
## API Usage Examples

Once the agent is running, you can send queries to its API endpoint. The agent will retrieve relevant context from Elasticsearch and augment the LLM's response.

The API endpoint is `http://localhost:8080/query`. You should always include a `--max-time` parameter to ensure `curl` waits long enough for the LLM to generate a response, especially with larger models like Llama3. A value of `90` seconds or more is recommended.

### Example 1: Querying for a specific account ID

```bash
curl -X POST -H "Content-Type: application/json" -d '{"prompt": "Get data matching account_id: ACC-0833"}' --max-time 90 http://localhost:8080/query
```

Response 
```bash
{"answer":"Here are the transactions for account_id ACC-0833: ..."}
```
Another example 
```bash
curl -X POST -H "Content-Type: application/json" -d '{"prompt": "Are there any transactions made in Euro (EUR)?"}' --max-time 90 http://localhost:8080/query
```
Response
```bash
{"answer":"Yes, I found transactions in Euro (EUR) including: ..."}
```