package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

const maxStoredRequests = 100

type RecordedRequest struct {
	Timestamp time.Time         `json:"timestamp"`
	Path      string            `json:"path"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	Body      string            `json:"body"`
}

type Server struct {
	mu       sync.RWMutex
	requests []RecordedRequest
	mode     string
}

func NewServer() *Server {
	return &Server{
		requests: make([]RecordedRequest, 0, maxStoredRequests),
		mode:     "normal",
	}
}

func (s *Server) recordRequest(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	headers := make(map[string]string, len(r.Header))
	for k, v := range r.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	rec := RecordedRequest{
		Timestamp: time.Now().UTC(),
		Path:      r.URL.Path,
		Method:    r.Method,
		Headers:   headers,
		Body:      string(body),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests = append(s.requests, rec)
	if len(s.requests) > maxStoredRequests {
		s.requests = s.requests[len(s.requests)-maxStoredRequests:]
	}
}

func (s *Server) getRequests() []RecordedRequest {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]RecordedRequest, len(s.requests))
	copy(out, s.requests)
	return out
}

func (s *Server) getRequestCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.requests)
}

func (s *Server) clearRequests() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = s.requests[:0]
}

func (s *Server) getMode() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mode
}

func (s *Server) setMode(mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = mode
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("error encoding response: %v", err)
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s %s", r.Method, r.URL.Path)

	switch {
	case r.URL.Path == "/health" && r.Method == http.MethodGet:
		s.handleHealth(w)

	case r.URL.Path == "/requests" && r.Method == http.MethodGet:
		s.handleGetRequests(w)

	case r.URL.Path == "/requests/count" && r.Method == http.MethodGet:
		s.handleGetRequestCount(w)

	case r.URL.Path == "/requests" && r.Method == http.MethodDelete:
		s.handleClearRequests(w)

	case r.URL.Path == "/mode/normal" && r.Method == http.MethodGet:
		s.handleSetMode(w, "normal")

	case r.URL.Path == "/mode/error" && r.Method == http.MethodGet:
		s.handleSetMode(w, "error")

	case r.URL.Path == "/mode/timeout" && r.Method == http.MethodGet:
		s.handleSetMode(w, "timeout")

	case r.URL.Path == "/mode/slow" && r.Method == http.MethodGet:
		s.handleSetMode(w, "slow")

	case r.Method == http.MethodPost:
		s.handleWebhook(w, r)

	default:
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "not found",
		})
	}
}

func (s *Server) handleHealth(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":            "ok",
		"mode":              s.getMode(),
		"requests_received": s.getRequestCount(),
	})
}

func (s *Server) handleGetRequests(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, s.getRequests())
}

func (s *Server) handleGetRequestCount(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]int{
		"count": s.getRequestCount(),
	})
}

func (s *Server) handleClearRequests(w http.ResponseWriter) {
	s.clearRequests()
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "cleared",
	})
}

func (s *Server) handleSetMode(w http.ResponseWriter, mode string) {
	s.setMode(mode)
	log.Printf("mode set to: %s", mode)
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "mode set",
		"mode":   mode,
	})
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	mode := s.getMode()

	switch mode {
	case "timeout":
		time.Sleep(15 * time.Second)
	case "slow":
		time.Sleep(5 * time.Second)
	}

	s.recordRequest(r)

	if mode == "error" {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "received",
	})
}

func main() {
	srv := NewServer()

	addr := ":9999"
	log.Printf("webhook-mock starting on %s", addr)

	server := &http.Server{
		Addr:              addr,
		Handler:           srv,
		ReadHeaderTimeout: 30 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		fmt.Printf("server error: %v\n", err)
	}
}
