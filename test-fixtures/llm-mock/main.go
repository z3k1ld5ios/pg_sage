package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// OpenAI-compatible request/response types.

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	MaxTokens int           `json:"max_tokens"`
}

type chatChoice struct {
	Index        int         `json:"index"`
	Message      chatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type chatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Choices []chatChoice `json:"choices"`
	Usage   chatUsage    `json:"usage"`
}

// Request log entry.

type requestLog struct {
	Timestamp    string `json:"timestamp"`
	PromptPrefix string `json:"prompt_prefix"`
	MatchedRoute string `json:"matched_route"`
	Mode         string `json:"mode"`
}

// Server state.

type server struct {
	mu       sync.RWMutex
	mode     string
	logs     []requestLog
	logMu    sync.Mutex
	reqCount int
}

func newServer() *server {
	return &server{
		mode: "normal",
		logs: make([]requestLog, 0, 100),
	}
}

func (s *server) getMode() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mode
}

func (s *server) setMode(m string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = m
}

func (s *server) addLog(entry requestLog) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	s.reqCount++
	if len(s.logs) >= 100 {
		s.logs = s.logs[1:]
	}
	s.logs = append(s.logs, entry)
}

func (s *server) getLogs() []requestLog {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	cp := make([]requestLog, len(s.logs))
	copy(cp, s.logs)
	return cp
}

func (s *server) getReqCount() int {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	return s.reqCount
}

// Canned responses per route.

const respIndex = `[{"table":"public.orders","ddl":"CREATE INDEX CONCURRENTLY idx_orders_cust_status ON public.orders (customer_id, status)","drop_ddl":"DROP INDEX CONCURRENTLY IF EXISTS idx_orders_cust_status","rationale":"Composite index on customer_id and status improves JOIN and filter performance","severity":"warning","confidence":0.85,"index_type":"btree","category":"composite_index","affected_queries":["SELECT * FROM orders WHERE customer_id = $1 AND status = $2"],"estimated_improvement_pct":35.0,"validated":false,"action_level":"advisory"}]`

const respVacuum = `[{"object_identifier":"public.bloat_target","severity":"warning","rationale":"Dead tuple ratio exceeds 20%, autovacuum scale factor too high","recommended_sql":"ALTER TABLE public.bloat_target SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 10)","current_settings":{"autovacuum_vacuum_scale_factor":"0.1"},"recommended_settings":{"autovacuum_vacuum_scale_factor":"0.01"}}]`

const respMemory = `[{"object_identifier":"instance","severity":"info","rationale":"work_mem too low, sort operations spilling to disk","recommended_sql":"ALTER SYSTEM SET work_mem = '256MB'","current_settings":{"work_mem":"4MB"},"recommended_settings":{"work_mem":"256MB"}}]`

const respWAL = `[{"object_identifier":"instance","severity":"info","rationale":"Checkpoint frequency too high, consider increasing checkpoint_completion_target","recommended_sql":"ALTER SYSTEM SET checkpoint_completion_target = 0.9","current_settings":{"checkpoint_completion_target":"0.5"},"recommended_settings":{"checkpoint_completion_target":"0.9"}}]`

const respConn = `[{"object_identifier":"instance","severity":"info","rationale":"max_connections is 4x peak active usage","recommended_sql":"ALTER SYSTEM SET max_connections = 200","current_settings":{"max_connections":"800"},"recommended_settings":{"max_connections":"200"}}]`

const respHint = `[{"hint_directive":"Set(work_mem \"256MB\") HashJoin(o c)","rationale":"Sort spilling to disk and nested loop inefficient for large result sets","confidence":0.85}]`

const respBriefing = "## Database Health Briefing\n\n**Overall Status**: Good\n\n### Key Findings\n- 2 unused indexes detected\n- 1 table with high bloat\n- Query performance stable\n\n### Recommendations\n- Drop unused indexes to reduce write overhead\n- Schedule VACUUM FULL for bloat_target table"

const respRewrite = `[{"object_identifier":"public.orders","severity":"info","rationale":"Query can be rewritten to use EXISTS instead of IN for better performance","recommended_sql":"SELECT o.* FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tier = 'free')"}]`

const respDefault = `[]`

// Route matching: returns (content, routeName).
func matchRoute(prompt string) (string, string) {
	lower := strings.ToLower(prompt)

	if strings.Contains(lower, "index") &&
		(strings.Contains(lower, "recommendation") ||
			strings.Contains(lower, "create index") ||
			strings.Contains(lower, "analyze") ||
			strings.Contains(lower, "indexing rules")) {
		return respIndex, "index"
	}
	if strings.Contains(lower, "vacuum") ||
		strings.Contains(lower, "autovacuum") {
		return respVacuum, "vacuum"
	}
	if strings.Contains(lower, "work_mem") ||
		strings.Contains(lower, "memory") ||
		strings.Contains(lower, "shared_buffers") {
		return respMemory, "memory"
	}
	if strings.Contains(lower, "wal") ||
		strings.Contains(lower, "checkpoint") {
		return respWAL, "wal"
	}
	if strings.Contains(lower, "connection") ||
		strings.Contains(lower, "max_connections") {
		return respConn, "connection"
	}
	if strings.Contains(lower, "hint") ||
		strings.Contains(lower, "plan") ||
		strings.Contains(lower, "sort") ||
		strings.Contains(lower, "nested") ||
		strings.Contains(lower, "join") {
		return respHint, "hint"
	}
	if strings.Contains(lower, "briefing") ||
		strings.Contains(lower, "health") ||
		strings.Contains(lower, "summary") {
		return respBriefing, "briefing"
	}
	if strings.Contains(lower, "rewrite") ||
		strings.Contains(lower, "query optimization") {
		return respRewrite, "rewrite"
	}
	return respDefault, "default"
}

func buildChatResponse(content, prompt string) chatResponse {
	promptTokens := len(prompt) / 4
	completionTokens := len(content) / 4
	return chatResponse{
		ID:     fmt.Sprintf("mock-%d", time.Now().UnixNano()),
		Object: "chat.completion",
		Choices: []chatChoice{
			{
				Index:        0,
				Message:      chatMessage{Role: "assistant", Content: content},
				FinishReason: "stop",
			},
		},
		Usage: chatUsage{
			PromptTokens:     promptTokens,
			CompletionTokens: completionTokens,
			TotalTokens:      promptTokens + completionTokens,
		},
	}
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func (s *server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	mode := s.getMode()

	// Mode-based early returns.
	switch mode {
	case "error":
		http.Error(w, `{"error":{"message":"internal server error","type":"server_error"}}`, http.StatusInternalServerError)
		return
	case "rate-limit":
		w.Header().Set("Retry-After", "30")
		http.Error(w, `{"error":{"message":"rate limit exceeded","type":"rate_limit_error"}}`, http.StatusTooManyRequests)
		return
	case "timeout":
		time.Sleep(30 * time.Second)
		// fall through to normal processing after sleep
	}

	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":{"message":"invalid request body"}}`, http.StatusBadRequest)
		return
	}

	// Combine all message content for keyword matching.
	var combined strings.Builder
	for _, msg := range req.Messages {
		combined.WriteString(msg.Content)
		combined.WriteString(" ")
	}
	prompt := combined.String()

	content, route := matchRoute(prompt)

	// Apply mode transformations.
	switch mode {
	case "malformed":
		content = "```json\n" + content + "\n```"
	case "truncated":
		if len(content) > 20 {
			content = content[:len(content)/2]
		}
	case "thinking":
		content = "Let me analyze this carefully...\n" + content
	case "empty":
		content = ""
	}

	// Log the request.
	entry := requestLog{
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		PromptPrefix: truncateStr(prompt, 200),
		MatchedRoute: route,
		Mode:         mode,
	}
	s.addLog(entry)
	log.Printf("[%s] route=%s mode=%s prompt=%.80s",
		entry.Timestamp, route, mode, prompt)

	resp := buildChatResponse(content, prompt)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *server) handleModeSwitch(mode string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.setMode(mode)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"mode":"%s"}`, mode)
		log.Printf("Mode switched to: %s", mode)
	}
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	mode := s.getMode()
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","mode":"%s"}`, mode)
}

func (s *server) handleRequests(w http.ResponseWriter, r *http.Request) {
	logs := s.getLogs()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}

func (s *server) handleRequestCount(w http.ResponseWriter, r *http.Request) {
	count := s.getReqCount()
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"count":%d}`, count)
}

func main() {
	srv := newServer()

	mux := http.NewServeMux()

	// Chat completions endpoint.
	mux.HandleFunc("/v1/chat/completions", srv.handleChatCompletions)

	// Mode control endpoints.
	mux.HandleFunc("/v1/mode/normal", srv.handleModeSwitch("normal"))
	mux.HandleFunc("/v1/mode/error", srv.handleModeSwitch("error"))
	mux.HandleFunc("/v1/mode/timeout", srv.handleModeSwitch("timeout"))
	mux.HandleFunc("/v1/mode/malformed", srv.handleModeSwitch("malformed"))
	mux.HandleFunc("/v1/mode/truncated", srv.handleModeSwitch("truncated"))
	mux.HandleFunc("/v1/mode/thinking", srv.handleModeSwitch("thinking"))
	mux.HandleFunc("/v1/mode/empty", srv.handleModeSwitch("empty"))
	mux.HandleFunc("/v1/mode/rate-limit", srv.handleModeSwitch("rate-limit"))

	// Request logging endpoints.
	mux.HandleFunc("/v1/requests", srv.handleRequests)
	mux.HandleFunc("/v1/requests/count", srv.handleRequestCount)

	// Health endpoint.
	mux.HandleFunc("/health", srv.handleHealth)

	addr := ":11434"
	log.Printf("LLM mock server starting on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
