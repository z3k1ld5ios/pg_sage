package analyzer

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/pg-sage/sidecar/internal/collector"
	"github.com/pg-sage/sidecar/internal/config"
)

// ruleReplicationLag flags replicas with high replay lag.
// Warning if > 30s, critical if > 5 minutes.
func ruleReplicationLag(
	current *collector.Snapshot,
	_ *collector.Snapshot,
	_ *config.Config,
	_ *RuleExtras,
) []Finding {
	if current.Replication == nil {
		return nil
	}

	var findings []Finding
	for i, r := range current.Replication.Replicas {
		if r.ReplayLag == nil || *r.ReplayLag == "" {
			continue
		}
		lag := parsePGInterval(*r.ReplayLag)
		if lag < 30*time.Second {
			continue
		}

		severity := "warning"
		if lag >= 5*time.Minute {
			severity = "critical"
		}

		clientAddr := "<unknown>"
		if r.ClientAddr != nil {
			clientAddr = *r.ClientAddr
		}

		ident := fmt.Sprintf("replica:%d:%s", i, clientAddr)
		writeLag := ""
		if r.WriteLag != nil {
			writeLag = *r.WriteLag
		}
		flushLag := ""
		if r.FlushLag != nil {
			flushLag = *r.FlushLag
		}

		findings = append(findings, Finding{
			Category:         "replication_lag",
			Severity:         severity,
			ObjectType:       "replication",
			ObjectIdentifier: ident,
			Title: fmt.Sprintf(
				"Replication lag %s on replica %s",
				*r.ReplayLag, clientAddr,
			),
			Detail: map[string]any{
				"client_addr": clientAddr,
				"replay_lag":  *r.ReplayLag,
				"lag_seconds": lag.Seconds(),
				"write_lag":   writeLag,
				"flush_lag":   flushLag,
				"state":       r.State,
			},
			Recommendation: "Investigate replica performance or network issues.",
			ActionRisk:     "safe",
		})
	}
	return findings
}

// ruleInactiveSlots flags replication slots that are not active.
func ruleInactiveSlots(
	current *collector.Snapshot,
	_ *collector.Snapshot,
	_ *config.Config,
	_ *RuleExtras,
) []Finding {
	if current.Replication == nil {
		return nil
	}

	var findings []Finding
	for _, s := range current.Replication.Slots {
		if s.Active {
			continue
		}

		ident := fmt.Sprintf("slot:%s", s.SlotName)
		findings = append(findings, Finding{
			Category:         "inactive_slot",
			Severity:         "warning",
			ObjectType:       "replication_slot",
			ObjectIdentifier: ident,
			Title: fmt.Sprintf(
				"Inactive replication slot %s (retaining %d bytes)",
				s.SlotName, s.RetainedBytes,
			),
			Detail: map[string]any{
				"slot_name":      s.SlotName,
				"slot_type":      s.SlotType,
				"retained_bytes": s.RetainedBytes,
				"active":         s.Active,
			},
			Recommendation: "Drop the inactive slot to free retained WAL.",
			RecommendedSQL: fmt.Sprintf(
				"SELECT pg_drop_replication_slot('%s');", s.SlotName,
			),
			ActionRisk: "safe",
		})
	}
	return findings
}

// parsePGInterval parses a PostgreSQL interval string like "00:01:23.456"
// or "1 day 02:03:04" into a time.Duration.
func parsePGInterval(s string) time.Duration {
	re := regexp.MustCompile(
		`(?:(\d+)\s+days?\s+)?(\d+):(\d+):(\d+(?:\.\d+)?)`,
	)
	m := re.FindStringSubmatch(s)
	if m == nil {
		return 0
	}

	var d time.Duration
	if m[1] != "" {
		days, _ := strconv.Atoi(m[1])
		d += time.Duration(days) * 24 * time.Hour
	}
	hours, _ := strconv.Atoi(m[2])
	mins, _ := strconv.Atoi(m[3])
	secs, _ := strconv.ParseFloat(m[4], 64)

	d += time.Duration(hours) * time.Hour
	d += time.Duration(mins) * time.Minute
	d += time.Duration(secs * float64(time.Second))

	return d
}
