package optimizer

import (
	"fmt"
	"sync"
	"time"
)

// CircuitState represents the state of a per-table circuit breaker.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

// TableCircuit tracks failure state for one table.
type TableCircuit struct {
	State        CircuitState
	Failures     int
	LastFailure  time.Time
	CooldownDays int // 1 for first open, 7 for repeat
}

// CircuitBreaker manages per-table circuit breakers.
type CircuitBreaker struct {
	mu       sync.Mutex
	circuits map[string]*TableCircuit
	maxFails int
}

// NewCircuitBreaker creates a CircuitBreaker with default maxFails=3.
func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{
		circuits: make(map[string]*TableCircuit),
		maxFails: 3,
	}
}

func tableKey(schema, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}

func (cb *CircuitBreaker) getOrCreate(key string) *TableCircuit {
	tc, ok := cb.circuits[key]
	if !ok {
		tc = &TableCircuit{State: CircuitClosed}
		cb.circuits[key] = tc
	}
	return tc
}

// ShouldSkip returns true if the table's circuit is open.
// Transitions expired open circuits to half_open.
func (cb *CircuitBreaker) ShouldSkip(schema, table string) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	tc := cb.getOrCreate(tableKey(schema, table))
	if tc.State == CircuitClosed || tc.State == CircuitHalfOpen {
		return false
	}
	cooldown := time.Duration(tc.CooldownDays) * 24 * time.Hour
	if time.Since(tc.LastFailure) >= cooldown {
		tc.State = CircuitHalfOpen
		return false
	}
	return true
}

// RecordSuccess resets the circuit to closed.
func (cb *CircuitBreaker) RecordSuccess(schema, table string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	tc := cb.getOrCreate(tableKey(schema, table))
	tc.State = CircuitClosed
	tc.Failures = 0
	tc.CooldownDays = 0
}

// RecordFailure records a failed recommendation.
// Opens the circuit after maxFails consecutive failures.
func (cb *CircuitBreaker) RecordFailure(schema, table string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	tc := cb.getOrCreate(tableKey(schema, table))
	tc.Failures++
	tc.LastFailure = time.Now()

	if tc.State == CircuitHalfOpen {
		tc.State = CircuitOpen
		tc.CooldownDays = 7
		return
	}
	if tc.Failures >= cb.maxFails {
		tc.State = CircuitOpen
		if tc.CooldownDays == 0 {
			tc.CooldownDays = 1
		}
	}
}

// GetState returns the current circuit state for a table.
func (cb *CircuitBreaker) GetState(schema, table string) CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	tc := cb.getOrCreate(tableKey(schema, table))
	return tc.State
}
