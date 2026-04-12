package main

import (
	"testing"
	"time"
)

// TestParseConfigRampStart covers the trust.ramp_start parser used by
// both single-db and fleet-mode bootstrap paths. Regression guard for
// latent bug #4 where fleet mode ignored the YAML value.
func TestParseConfigRampStart(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantZero bool
		wantTime string // canonical RFC3339 form for comparison
	}{
		{
			name:     "empty string returns zero",
			input:    "",
			wantZero: true,
		},
		{
			name:     "RFC3339 with offset",
			input:    "2025-11-03T14:30:00-05:00",
			wantTime: "2025-11-03T14:30:00-05:00",
		},
		{
			name:     "RFC3339 UTC",
			input:    "2025-11-03T14:30:00Z",
			wantTime: "2025-11-03T14:30:00Z",
		},
		{
			name:     "date only",
			input:    "2025-11-03",
			wantTime: "2025-11-03T00:00:00Z",
		},
		{
			name:     "date and time, no TZ",
			input:    "2025-11-03T14:30:00",
			wantTime: "2025-11-03T14:30:00Z",
		},
		{
			name:     "garbage returns zero",
			input:    "not a date",
			wantZero: true,
		},
		{
			name:     "partial date returns zero",
			input:    "2025-11",
			wantZero: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseConfigRampStart(tc.input)
			if tc.wantZero {
				if !got.IsZero() {
					t.Errorf("got %v, want zero time", got)
				}
				return
			}
			if got.IsZero() {
				t.Fatalf("got zero time, want parsed %s", tc.wantTime)
			}
			want, err := time.Parse(time.RFC3339, tc.wantTime)
			if err != nil {
				t.Fatalf("test fixture broken: %v", err)
			}
			if !got.Equal(want) {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

// TestParseConfigRampStart_FleetNonZeroWhenSet verifies the explicit
// guarantee we care about: if the YAML has a valid ramp_start, the
// parser must return a non-zero time so fleet bootstrap passes it
// through to PersistTrustRampStart rather than falling back to now().
func TestParseConfigRampStart_FleetNonZeroWhenSet(t *testing.T) {
	got := parseConfigRampStart("2024-01-15")
	if got.IsZero() {
		t.Fatal("valid YAML ramp_start should not round-trip to zero — " +
			"would regress latent bug #4 (fleet ignoring YAML)")
	}
	if got.Year() != 2024 || got.Month() != time.January || got.Day() != 15 {
		t.Errorf("got %v, want 2024-01-15", got)
	}
}
