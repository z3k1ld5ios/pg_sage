package analyzer

import (
	"fmt"

	"github.com/pg-sage/sidecar/internal/collector"
	"github.com/pg-sage/sidecar/internal/config"
)

// ruleSequenceExhaustion flags sequences approaching their max value.
// Handles both ascending and descending sequences.
// Warning at 75%, critical at 90%.
// INTEGER sequences (max ~2.1B) get extra emphasis.
func ruleSequenceExhaustion(
	current *collector.Snapshot,
	_ *collector.Snapshot,
	_ *config.Config,
	_ *RuleExtras,
) []Finding {
	var findings []Finding

	for _, seq := range current.Sequences {
		pct := seq.PctUsed
		if pct < 75.0 {
			continue
		}

		severity := "warning"
		if pct >= 90.0 {
			severity = "critical"
		}

		ident := seq.SchemaName + "." + seq.SequenceName

		detail := map[string]any{
			"current_value": seq.LastValue,
			"max_value":     seq.MaxValue,
			"increment":     seq.IncrementBy,
			"usage_pct":     pct,
			"data_type":     seq.DataType,
		}

		recommendation := fmt.Sprintf(
			"Sequence %s is %.1f%% consumed.",
			ident, pct,
		)
		if seq.DataType == "integer" {
			recommendation += " Consider migrating to bigint."
		} else {
			recommendation += " Plan for sequence reset or range expansion."
		}

		findings = append(findings, Finding{
			Category:         "sequence_exhaustion",
			Severity:         severity,
			ObjectType:       "sequence",
			ObjectIdentifier: ident,
			Title: fmt.Sprintf(
				"Sequence %s is %.1f%% consumed (%s)",
				ident, pct, seq.DataType,
			),
			Detail:         detail,
			Recommendation: recommendation,
			ActionRisk:     "safe",
		})
	}
	return findings
}
