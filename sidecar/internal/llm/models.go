package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ModelInfo describes an LLM model returned by the provider.
type ModelInfo struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	InputTokenLimit  int    `json:"input_token_limit,omitempty"`
	OutputTokenLimit int    `json:"output_token_limit,omitempty"`
	Description      string `json:"description,omitempty"`
}

// modelCache stores cached model listings with TTL.
type modelCache struct {
	mu      sync.Mutex
	models  []ModelInfo
	fetched time.Time
	ttl     time.Duration
}

// defaultCache is the package-level cache (1-hour TTL).
var defaultCache = &modelCache{ttl: time.Hour}

func (c *modelCache) get() ([]ModelInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.models != nil && time.Since(c.fetched) < c.ttl {
		return c.models, true
	}
	return nil, false
}

func (c *modelCache) set(models []ModelInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.models = models
	c.fetched = time.Now()
}

// ListModels queries the LLM provider for available models.
// Results are cached for 1 hour.
func ListModels(
	ctx context.Context, endpoint, apiKey string,
) ([]ModelInfo, error) {
	if cached, ok := defaultCache.get(); ok {
		return cached, nil
	}
	models, err := fetchModels(ctx, endpoint, apiKey)
	if err != nil {
		return nil, err
	}
	defaultCache.set(models)
	return models, nil
}

// InvalidateModelCache clears the cached model list.
func InvalidateModelCache() {
	defaultCache.mu.Lock()
	defer defaultCache.mu.Unlock()
	defaultCache.models = nil
}

// fetchModels dispatches to the correct provider parser.
func fetchModels(
	ctx context.Context, endpoint, apiKey string,
) ([]ModelInfo, error) {
	if isGeminiEndpoint(endpoint) {
		return fetchGeminiModels(ctx, apiKey)
	}
	return fetchOpenAIModels(ctx, endpoint, apiKey)
}

func isGeminiEndpoint(endpoint string) bool {
	return strings.Contains(
		endpoint, "generativelanguage.googleapis.com")
}

// fetchGeminiModels calls the Gemini ListModels API.
func fetchGeminiModels(
	ctx context.Context, apiKey string,
) ([]ModelInfo, error) {
	url := "https://generativelanguage.googleapis.com/" +
		"v1beta/models?key=" + apiKey
	body, err := doModelRequest(ctx, url, "")
	if err != nil {
		return nil, fmt.Errorf("gemini list models: %w", err)
	}
	return parseGeminiModels(body)
}

// geminiListResponse is the Gemini API response shape.
type geminiListResponse struct {
	Models []geminiModel `json:"models"`
}

type geminiModel struct {
	Name                       string   `json:"name"`
	DisplayName                string   `json:"displayName"`
	Description                string   `json:"description"`
	InputTokenLimit            int      `json:"inputTokenLimit"`
	OutputTokenLimit           int      `json:"outputTokenLimit"`
	SupportedGenerationMethods []string `json:"supportedGenerationMethods"`
}

func parseGeminiModels(data []byte) ([]ModelInfo, error) {
	var resp geminiListResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("parse gemini response: %w", err)
	}
	var models []ModelInfo
	for _, m := range resp.Models {
		if !supportsGenerate(m.SupportedGenerationMethods) {
			continue
		}
		models = append(models, ModelInfo{
			ID:               stripModelsPrefix(m.Name),
			Name:             m.DisplayName,
			InputTokenLimit:  m.InputTokenLimit,
			OutputTokenLimit: m.OutputTokenLimit,
			Description:      m.Description,
		})
	}
	return models, nil
}

func supportsGenerate(methods []string) bool {
	for _, m := range methods {
		if m == "generateContent" {
			return true
		}
	}
	return false
}

func stripModelsPrefix(name string) string {
	return strings.TrimPrefix(name, "models/")
}

// fetchOpenAIModels calls the OpenAI-compatible /models endpoint.
func fetchOpenAIModels(
	ctx context.Context, endpoint, apiKey string,
) ([]ModelInfo, error) {
	base := strings.TrimRight(endpoint, "/")
	// Strip chat/completions suffixes to get the base URL.
	base = strings.TrimSuffix(base, "/chat/completions")
	base = strings.TrimSuffix(base, "/chat")
	url := base + "/models"
	body, err := doModelRequest(ctx, url, apiKey)
	if err != nil {
		return nil, fmt.Errorf("openai list models: %w", err)
	}
	return parseOpenAIModels(body)
}

// openAIListResponse is the OpenAI /models response shape.
type openAIListResponse struct {
	Data []openAIModel `json:"data"`
}

type openAIModel struct {
	ID      string `json:"id"`
	OwnedBy string `json:"owned_by"`
}

func parseOpenAIModels(data []byte) ([]ModelInfo, error) {
	var resp openAIListResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("parse openai response: %w", err)
	}
	models := make([]ModelInfo, 0, len(resp.Data))
	for _, m := range resp.Data {
		models = append(models, ModelInfo{
			ID:   m.ID,
			Name: m.ID,
		})
	}
	return models, nil
}

// doModelRequest performs a GET with a 10s timeout.
func doModelRequest(
	ctx context.Context, url, apiKey string,
) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	if apiKey != "" {
		req.Header.Set(
			"Authorization", "Bearer "+apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		slog.Warn("model list API error",
			"status", resp.StatusCode,
			"body", string(body))
		return nil, fmt.Errorf(
			"API returned %d: %s",
			resp.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
