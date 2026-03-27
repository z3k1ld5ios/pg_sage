package forecaster

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pg-sage/sidecar/internal/analyzer"
)

// ForecasterConfig holds thresholds for workload forecasting.
type ForecasterConfig struct {
	Enabled              bool
	LookbackDays         int
	DiskWarnGrowthGBDay  float64
	ConnectionWarnPct    float64
	CacheWarnThreshold   float64
	SequenceWarnDays     int
	SequenceCriticalDays int
}

// Forecaster produces capacity forecast findings from historical
// snapshot data using statistical methods.
type Forecaster struct {
	pool  *pgxpool.Pool
	cfg   ForecasterConfig
	logFn func(string, string, ...any)
}

// New creates a new Forecaster.
func New(
	pool *pgxpool.Pool,
	cfg ForecasterConfig,
	logFn func(string, string, ...any),
) *Forecaster {
	return &Forecaster{pool: pool, cfg: cfg, logFn: logFn}
}

// Forecast runs all forecast rules and returns findings.
func (f *Forecaster) Forecast(
	ctx context.Context,
) ([]analyzer.Finding, error) {
	var all []analyzer.Finding

	sysAggs, err := QueryDailySystemAggs(
		ctx, f.pool, f.cfg.LookbackDays,
	)
	if err != nil {
		f.logFn("WARN", "forecaster: system aggs: %v", err)
	} else {
		all = append(all,
			forecastDiskGrowth(sysAggs, f.cfg)...)
		all = append(all,
			forecastConnectionSaturation(sysAggs, f.cfg)...)
		all = append(all,
			forecastCachePressure(sysAggs, f.cfg)...)
		all = append(all,
			forecastCheckpointPressure(sysAggs, f.cfg)...)
	}

	qAggs, err := QueryDailyQueryAggs(
		ctx, f.pool, f.cfg.LookbackDays,
	)
	if err != nil {
		f.logFn("WARN", "forecaster: query aggs: %v", err)
	} else {
		all = append(all,
			forecastQueryVolume(qAggs, f.cfg)...)
	}

	seqAggs, err := QueryDailySeqAggs(
		ctx, f.pool, f.cfg.LookbackDays,
	)
	if err != nil {
		f.logFn("WARN", "forecaster: seq aggs: %v", err)
	} else {
		all = append(all,
			forecastSequenceExhaustion(seqAggs, f.cfg)...)
	}

	return all, nil
}
