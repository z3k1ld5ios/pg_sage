package analyzer

import (
	"fmt"

	"github.com/pg-sage/sidecar/internal/collector"
	"github.com/pg-sage/sidecar/internal/config"
)

// ruleTableBloat flags tables where the dead tuple ratio exceeds the
// configured threshold.
func ruleTableBloat(
	current *collector.Snapshot,
	_ *collector.Snapshot,
	cfg *config.Config,
	_ *RuleExtras,
) []Finding {
	threshold := float64(cfg.Analyzer.TableBloatDeadTuplePct) / 100.0
	var findings []Finding

	minRows := int64(cfg.Analyzer.TableBloatMinRows)

	for _, t := range current.Tables {
		total := t.NLiveTup + t.NDeadTup
		if total < minRows {
			continue
		}
		if total == 0 {
			total = 1
		}
		deadRatio := float64(t.NDeadTup) / float64(total)
		if deadRatio <= threshold {
			continue
		}

		ident := t.SchemaName + "." + t.RelName
		findings = append(findings, Finding{
			Category:         "table_bloat",
			Severity:         "warning",
			ObjectType:       "table",
			ObjectIdentifier: ident,
			Title: fmt.Sprintf(
				"Table %s has %.1f%% dead tuples",
				ident, deadRatio*100,
			),
			Detail: map[string]any{
				"n_live_tup":  t.NLiveTup,
				"n_dead_tup":  t.NDeadTup,
				"dead_ratio":  deadRatio,
				"last_vacuum": t.LastVacuum,
			},
			Recommendation: "Run VACUUM to reclaim dead tuple space.",
			RecommendedSQL: fmt.Sprintf("VACUUM %s;", ident),
			ActionRisk:     "safe",
		})
	}
	return findings
}

// ruleXIDWraparound flags databases approaching XID wraparound.
// xidAge is obtained from: SELECT age(datfrozenxid) FROM pg_database
// WHERE datname = current_database().
func ruleXIDWraparound(xidAge int64, cfg *config.Config) []Finding {
	var findings []Finding

	if xidAge < cfg.Analyzer.XIDWraparoundWarning {
		return findings
	}

	severity := "warning"
	if xidAge >= cfg.Analyzer.XIDWraparoundCritical {
		severity = "critical"
	}

	findings = append(findings, Finding{
		Category:         "xid_wraparound",
		Severity:         severity,
		ObjectType:       "database",
		ObjectIdentifier: "current_database",
		Title: fmt.Sprintf(
			"XID age is %d (wraparound risk)",
			xidAge,
		),
		Detail: map[string]any{
			"xid_age":           xidAge,
			"warning_threshold": cfg.Analyzer.XIDWraparoundWarning,
			"critical_threshold": cfg.Analyzer.XIDWraparoundCritical,
		},
		Recommendation: "Run VACUUM FREEZE on high-age tables.",
		RecommendedSQL: "VACUUM FREEZE;",
		ActionRisk:     "safe",
	})
	return findings
}
