package vectordb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	elastic "github.com/olivere/elastic/v7"
	"stream-rag-agent/internal/config"
	"stream-rag-agent/internal/window"
)

type ElasticsearchClient struct {
	client    *elastic.Client
	indexName string
}

func NewElasticsearchClient(cfg *config.ElasticsearchConfig) (*ElasticsearchClient, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(cfg.Addresses...),
		elastic.SetSniff(false), // Disable sniffing for local/simple setups, enable for production
		elastic.SetHealthcheck(false),
		elastic.SetGzip(true),
		elastic.SetInfoLog(log.New(os.Stdout, "ES_INFO: ", log.LstdFlags)),
		elastic.SetErrorLog(log.New(os.Stderr, "ES_ERROR: ", log.LstdFlags)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	_, _, err = client.Ping(cfg.Addresses[0]).Do(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to ping elasticsearch cluster: %w", err)
	}
	log.Printf("Connected to Elasticsearch cluster: %v", cfg.Addresses)

	esClient := &ElasticsearchClient{
		client:    client,
		indexName: cfg.IndexName,
	}

	err = esClient.createIndexWithMapping()
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch index with mapping: %w", err)
	}

	return esClient, nil
}

func (c *ElasticsearchClient) createIndexWithMapping() error {
	ctx := context.Background()
	exists, err := c.client.IndexExists(c.indexName).Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if index exists: %w", err)
	}

	if exists {
		log.Printf("Elasticsearch index '%s' already exists. Skipping creation.", c.indexName)
		return nil
	}

	// Mapping for the index (adjust dimension based on your Ollama embedding model)
	// nomic-embed-text typically has 768 dimensions
	mapping := `{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		},
		"mappings": {
			"properties": {
				"window_id":      {"type": "keyword"},
				"topic":          {"type": "keyword"},
				"partition":      {"type": "integer"},
				"start_time":     {"type": "date"},
				"end_time":       {"type": "date"},
				"message_count":  {"type": "integer"},
				"context_text":   {"type": "text"},
				"embedding": {
					"type": "dense_vector",
					"dims": 768,  // IMPORTANT: Adjust this dimension based on your Ollama embedding model
					"index": true,
                    "similarity": "cosine"
				}
			}
		}
	}`

	createIndex, err := c.client.CreateIndex(c.indexName).BodyString(mapping).Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to create index '%s': %w", c.indexName, err)
	}
	if !createIndex.Acknowledged {
		return fmt.Errorf("failed to create index '%s': not acknowledged", c.indexName)
	}
	log.Printf("Elasticsearch index '%s' created successfully.", c.indexName)
	return nil
}

func (c *ElasticsearchClient) SaveEmbeddedWindow(ew *window.EmbeddedWindow) error {
	ctx := context.Background()

	// Use the window ID as the document ID for idempotency
	_, err := c.client.Index().
		Index(c.indexName).
		Id(ew.WindowID).
		BodyJson(ew).
		Do(ctx)

	if err != nil {
		return fmt.Errorf("failed to save embedded window to Elasticsearch: %w", err)
	}
	log.Printf("Saved window '%s' to Elasticsearch index '%s'.", ew.WindowID, c.indexName)
	return nil
}

func (c *ElasticsearchClient) SearchSimilarWindows(queryEmbedding []float32, k int) ([]window.EmbeddedWindow, error) {
	ctx := context.Background()

	searchBody := map[string]interface{}{
		"knn": map[string]interface{}{
			"field":          "embedding",
			"query_vector":   queryEmbedding,
			"k":              k,
			"num_candidates": 100,
		},
	}

	debugQueryJSON, err := json.Marshal(searchBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal k-NN query map for debug log: %w", err)
	}
	log.Printf("DEBUG: Sending ES k-NN search request to index '%s' with body: %s", c.indexName, string(debugQueryJSON))

	searchResult, err := c.client.Search().
		Index(c.indexName).
		Source(searchBody).
		Do(ctx)

	if err != nil {
		log.Printf("ERROR: Elasticsearch search failed: %v", err)
		return nil, fmt.Errorf("failed to execute elasticsearch k-NN search: %w", err)
	}

	if searchResult.Hits == nil || searchResult.Hits.Hits == nil {
		log.Println("DEBUG: No hits found for the k-NN search.")
		return []window.EmbeddedWindow{}, nil
	}

	var foundWindows []window.EmbeddedWindow
	for _, hit := range searchResult.Hits.Hits {
		var ew window.EmbeddedWindow
		if err := json.Unmarshal(hit.Source, &ew); err != nil {
			log.Printf("Error unmarshaling embedded window from ES hit: %v", err)
			continue
		}
		foundWindows = append(foundWindows, ew)
	}

	log.Printf("DEBUG: Found %d similar windows.", len(foundWindows))
	return foundWindows, nil
}
