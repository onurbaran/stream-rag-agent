# stream-rag-agent
Streaming Retrieval-Augmented Generation (RAG) agent in Go. It consumes real-time data from Kafka topics, processes it in configurable windows, converts the window content into embeddings using Ollama, and stores these embeddings (along with the original text) in Elasticsearch 
