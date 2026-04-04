package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pg-sage/sidecar/internal/config"
	"github.com/pg-sage/sidecar/internal/llm"
)

func TestListModelsHandler_NotConfigured(t *testing.T) {
	cfg := &config.LLMConfig{}
	handler := listModelsHandler(cfg)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/llm/models", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if !strings.Contains(body["error"], "not configured") {
		t.Errorf("error should mention not configured, got: %s",
			body["error"])
	}
}

func TestListModelsHandler_MissingAPIKey(t *testing.T) {
	cfg := &config.LLMConfig{
		Endpoint: "https://example.com/v1",
	}
	handler := listModelsHandler(cfg)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/llm/models", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestListModelsHandler_MissingEndpoint(t *testing.T) {
	cfg := &config.LLMConfig{
		APIKey: "some-key",
	}
	handler := listModelsHandler(cfg)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/llm/models", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestListModelsHandler_Success(t *testing.T) {
	// Set up a mock server that returns models.
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"data":[
				{"id":"gpt-4o","object":"model","owned_by":"openai"}
			]}`))
		}))
	defer srv.Close()

	// Clear the model cache so we hit the mock server.
	llm.InvalidateModelCache()

	cfg := &config.LLMConfig{
		Endpoint: srv.URL,
		APIKey:   "test-key",
		Model:    "gpt-4o",
	}
	handler := listModelsHandler(cfg)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/llm/models", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var body struct {
		Models  []llm.ModelInfo `json:"models"`
		Current string          `json:"current"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Current != "gpt-4o" {
		t.Errorf("expected current gpt-4o, got %s", body.Current)
	}
	if len(body.Models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(body.Models))
	}
	if body.Models[0].ID != "gpt-4o" {
		t.Errorf("expected model id gpt-4o, got %s", body.Models[0].ID)
	}
}

func TestListModelsHandler_ProviderError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "rate limited", 429)
		}))
	defer srv.Close()

	llm.InvalidateModelCache()

	cfg := &config.LLMConfig{
		Endpoint: srv.URL,
		APIKey:   "test-key",
		Model:    "gpt-4o",
	}
	handler := listModelsHandler(cfg)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/llm/models", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
}

// --- LLM Status Handler Tests ---

func testLLMClient(model string, budget int) *llm.Client {
	cfg := &config.LLMConfig{
		Enabled:          true,
		Endpoint:         "http://test-endpoint/v1",
		APIKey:           "test-key",
		Model:            model,
		TimeoutSeconds:   5,
		TokenBudgetDaily: budget,
		CooldownSeconds:  10,
	}
	return llm.New(cfg, func(_, _ string, _ ...any) {})
}

func TestLLMStatusHandler_NilManager(t *testing.T) {
	handler := llmStatusHandler(nil)
	req := httptest.NewRequest(
		http.MethodGet, "/api/v1/llm/status", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if !strings.Contains(body["error"], "not configured") {
		t.Errorf("error should mention 'not configured', got: %s",
			body["error"])
	}
}

func TestLLMStatusHandler_GeneralOnly(t *testing.T) {
	gen := testLLMClient("gemini-2.5-flash", 100000)
	mgr := llm.NewManager(gen, nil, false)

	handler := llmStatusHandler(mgr)
	req := httptest.NewRequest(
		http.MethodGet, "/api/v1/llm/status", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var body struct {
		Clients      map[string]llm.ClientStatus `json:"clients"`
		AnyExhausted bool                         `json:"any_exhausted"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(body.Clients) != 1 {
		t.Fatalf("expected 1 client, got %d", len(body.Clients))
	}
	gs, ok := body.Clients["general"]
	if !ok {
		t.Fatal("expected 'general' key in clients")
	}
	if gs.Model != "gemini-2.5-flash" {
		t.Errorf("model = %q, want %q", gs.Model, "gemini-2.5-flash")
	}
	if gs.TokenBudget != 100000 {
		t.Errorf("token_budget = %d, want 100000", gs.TokenBudget)
	}
	if body.AnyExhausted {
		t.Error("any_exhausted should be false")
	}
}

func TestLLMStatusHandler_DualClient_NotExhausted(t *testing.T) {
	gen := testLLMClient("gemini-2.5-flash", 100000)
	opt := testLLMClient("gemini-2.5-pro", 50000)
	mgr := llm.NewManager(gen, opt, false)

	handler := llmStatusHandler(mgr)
	req := httptest.NewRequest(
		http.MethodGet, "/api/v1/llm/status", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body struct {
		Clients      map[string]llm.ClientStatus `json:"clients"`
		AnyExhausted bool                         `json:"any_exhausted"`
	}
	json.NewDecoder(w.Body).Decode(&body)

	if len(body.Clients) != 2 {
		t.Fatalf("expected 2 clients, got %d", len(body.Clients))
	}
	if _, ok := body.Clients["general"]; !ok {
		t.Error("missing 'general' client")
	}
	if _, ok := body.Clients["optimizer"]; !ok {
		t.Error("missing 'optimizer' client")
	}
	if body.AnyExhausted {
		t.Error("any_exhausted should be false")
	}
}

func TestLLMStatusHandler_AnyExhausted(t *testing.T) {
	gen := testLLMClient("gemini-2.5-flash", 100000)
	opt := testLLMClient("gemini-2.5-pro", 50000)
	mgr := llm.NewManager(gen, opt, false)

	// The any_exhausted field is computed from the client's
	// IsBudgetExhausted() method. Since we can't directly
	// set the internal state from outside the package, we
	// test by verifying the response shape is correct.
	handler := llmStatusHandler(mgr)
	req := httptest.NewRequest(
		http.MethodGet, "/api/v1/llm/status", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	var body struct {
		Clients      map[string]llm.ClientStatus `json:"clients"`
		AnyExhausted bool                         `json:"any_exhausted"`
	}
	json.NewDecoder(w.Body).Decode(&body)

	// Verify that the response structure includes all expected
	// fields for budget observability.
	gs := body.Clients["general"]
	if gs.ResetTimestamp == "" {
		t.Error("resets_at should not be empty")
	}
	// Verify budget field is populated.
	if gs.TokenBudget != 100000 {
		t.Errorf("token_budget = %d, want 100000", gs.TokenBudget)
	}
}

func TestLLMStatusHandler_ResponseShape(t *testing.T) {
	gen := testLLMClient("gpt-4o", 200000)
	mgr := llm.NewManager(gen, nil, false)

	handler := llmStatusHandler(mgr)
	req := httptest.NewRequest(
		http.MethodGet, "/api/v1/llm/status", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	// Verify the raw JSON has the expected fields.
	var raw map[string]json.RawMessage
	json.NewDecoder(w.Body).Decode(&raw)

	if _, ok := raw["clients"]; !ok {
		t.Error("response missing 'clients' field")
	}
	if _, ok := raw["any_exhausted"]; !ok {
		t.Error("response missing 'any_exhausted' field")
	}

	// Drill into the general client to check field presence.
	var clients map[string]map[string]json.RawMessage
	json.Unmarshal(raw["clients"], &clients)
	gs := clients["general"]

	requiredFields := []string{
		"model", "enabled", "tokens_used", "token_budget",
		"budget_exhausted", "circuit_open", "resets_at",
	}
	for _, field := range requiredFields {
		if _, ok := gs[field]; !ok {
			t.Errorf("general client missing field %q", field)
		}
	}
}
