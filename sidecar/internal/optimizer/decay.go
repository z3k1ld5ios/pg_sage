package optimizer

// DecayInfo holds decay analysis for one index.
type DecayInfo struct {
	IndexName    string
	CurrentScans int64
	PriorScans   int64
	DecayPct     float64
	IsDecaying   bool
}

// ComputeDecayPct computes the percentage decline from prior to current.
// Returns 0 if prior is 0 (no baseline). Negative means usage increased.
func ComputeDecayPct(priorScans, currentScans int64) float64 {
	if priorScans == 0 {
		return 0
	}
	return float64(priorScans-currentScans) / float64(priorScans) * 100
}

// AnalyzeDecay compares current index scans with historical scans
// to detect indexes whose usage is declining.
// threshold is the minimum decline percentage to flag (e.g., 50.0).
func AnalyzeDecay(
	current []IndexInfo,
	historical map[string]int64,
	threshold float64,
) []DecayInfo {
	var results []DecayInfo
	for _, idx := range current {
		prior, ok := historical[idx.Name]
		if !ok || prior == 0 {
			continue
		}
		pct := ComputeDecayPct(prior, idx.Scans)
		results = append(results, DecayInfo{
			IndexName:    idx.Name,
			CurrentScans: idx.Scans,
			PriorScans:   prior,
			DecayPct:     pct,
			IsDecaying:   pct > threshold,
		})
	}
	return results
}
