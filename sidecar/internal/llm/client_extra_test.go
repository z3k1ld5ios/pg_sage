package llm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pg-sage/sidecar/internal/config"
)

func TestChat_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	cfg := &config.LLMConfig{
		Enabled:          true,
		Endpoint:         srv.URL + "/",
		APIKey:           "k",
		Model:            "m",
		TimeoutSeconds:   1,
		TokenBudgetDaily: 100000,
		CooldownSeconds:  10,
	}
	client := New(cfg, noopLog)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, err := client.Chat(ctx, "s", "u", 100)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestChat_GarbageResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("this is not json"))
	}))
	defer srv.Close()

	cfg := &config.LLMConfig{
		Enabled:          true,
		Endpoint:         srv.URL + "/",
		APIKey:           "test-key",
		Model:            "test-model",
		TimeoutSeconds:   5,
		TokenBudgetDaily: 100000,
		CooldownSeconds:  10,
	}
	client := New(cfg, noopLog)

	_, _, err := client.Chat(context.Background(), "system", "user", 100)
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
	if !strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("expected error containing 'unmarshal', got: %v", err)
	}
}

func TestChat_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte("unauthorized"))
	}))
	defer srv.Close()

	cfg := &config.LLMConfig{
		Enabled:          true,
		Endpoint:         srv.URL + "/",
		APIKey:           "test-key",
		Model:            "test-model",
		TimeoutSeconds:   5,
		TokenBudgetDaily: 100000,
		CooldownSeconds:  10,
	}
	client := New(cfg, noopLog)

	_, _, err := client.Chat(context.Background(), "system", "user", 100)
	if err == nil {
		t.Fatal("expected error for 401 response")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("expected error containing '401', got: %v", err)
	}
}

func TestChat_LargeResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		big := make([]byte, 2<<20) // 2MB
		for i := range big {
			big[i] = 'x'
		}
		w.Write(big)
	}))
	defer srv.Close()

	cfg := &config.LLMConfig{
		Enabled:          true,
		Endpoint:         srv.URL + "/",
		APIKey:           "test-key",
		Model:            "test-model",
		TimeoutSeconds:   5,
		TokenBudgetDaily: 100000,
		CooldownSeconds:  10,
	}
	client := New(cfg, noopLog)

	_, _, err := client.Chat(context.Background(), "system", "user", 100)
	if err == nil {
		t.Fatal("expected error for large garbage response")
	}
	// The 1MB LimitReader truncates, then unmarshal fails on garbage.
	if !strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("expected error containing 'unmarshal', got: %v", err)
	}
}
