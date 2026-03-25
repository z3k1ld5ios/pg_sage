package config

import (
	"fmt"
	"log"
	"sync"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
	"os"
)

// Watcher monitors config.yaml for changes and calls onChange with
// the hot-reloadable values updated. Non-hot-reloadable values
// (postgres connection, listen addresses) are ignored on reload.
type Watcher struct {
	path     string
	mu       sync.RWMutex
	current  *Config
	onChange func(*Config)
	stop     chan struct{}
}

// NewWatcher creates a config file watcher. Call Start() to begin watching.
func NewWatcher(path string, current *Config, onChange func(*Config)) *Watcher {
	return &Watcher{
		path:     path,
		current:  current,
		onChange: onChange,
		stop:     make(chan struct{}),
	}
}

// Start begins watching the config file for changes.
func (w *Watcher) Start() error {
	if w.path == "" {
		return nil // no config file to watch
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("fsnotify: %w", err)
	}

	if err := watcher.Add(w.path); err != nil {
		watcher.Close()
		return fmt.Errorf("watch %s: %w", w.path, err)
	}

	go func() {
		defer watcher.Close()
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
					w.reload()
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("[WARN] [config-watcher] error: %v", err)
			case <-w.stop:
				return
			}
		}
	}()

	return nil
}

// Stop stops watching.
func (w *Watcher) Stop() {
	select {
	case w.stop <- struct{}{}:
	default:
	}
}

func (w *Watcher) reload() {
	data, err := os.ReadFile(w.path)
	if err != nil {
		log.Printf("[WARN] [config-watcher] read failed: %v", err)
		return
	}

	expanded := os.ExpandEnv(string(data))
	var fresh Config
	if err := yaml.Unmarshal([]byte(expanded), &fresh); err != nil {
		log.Printf("[WARN] [config-watcher] parse failed: %v", err)
		return
	}
	if err := fresh.validate(); err != nil {
		log.Printf("[WARN] [config-watcher] invalid config, keeping previous: %v", err)
		return
	}

	w.mu.Lock()
	old := w.current

	// Apply only hot-reloadable fields.
	changed := applyHotReload(old, &fresh)
	w.current = old
	w.mu.Unlock()

	if len(changed) > 0 {
		log.Printf("[INFO] [config-watcher] reloaded: %v", changed)
		if w.onChange != nil {
			w.onChange(old)
		}
	}
}

// applyHotReload copies hot-reloadable fields from fresh into target.
// Returns list of changed field names.
func applyHotReload(target, fresh *Config) []string {
	var changed []string

	if fresh.Collector.IntervalSeconds != 0 &&
		fresh.Collector.IntervalSeconds != target.Collector.IntervalSeconds {
		target.Collector.IntervalSeconds = fresh.Collector.IntervalSeconds
		changed = append(changed, "collector.interval_seconds")
	}
	if fresh.Collector.BatchSize != 0 &&
		fresh.Collector.BatchSize != target.Collector.BatchSize {
		target.Collector.BatchSize = fresh.Collector.BatchSize
		changed = append(changed, "collector.batch_size")
	}

	// Analyzer fields.
	a := &target.Analyzer
	fa := &fresh.Analyzer
	if fa.IntervalSeconds != 0 && fa.IntervalSeconds != a.IntervalSeconds {
		a.IntervalSeconds = fa.IntervalSeconds
		changed = append(changed, "analyzer.interval_seconds")
	}
	if fa.SlowQueryThresholdMs != 0 && fa.SlowQueryThresholdMs != a.SlowQueryThresholdMs {
		a.SlowQueryThresholdMs = fa.SlowQueryThresholdMs
		changed = append(changed, "analyzer.slow_query_threshold_ms")
	}

	// Safety fields.
	if fresh.Safety.CPUCeilingPct != 0 &&
		fresh.Safety.CPUCeilingPct != target.Safety.CPUCeilingPct {
		target.Safety.CPUCeilingPct = fresh.Safety.CPUCeilingPct
		changed = append(changed, "safety.cpu_ceiling_pct")
	}

	// Trust fields.
	if fresh.Trust.Level != "" && fresh.Trust.Level != target.Trust.Level {
		target.Trust.Level = fresh.Trust.Level
		changed = append(changed, "trust.level")
	}
	if fresh.Trust.MaintenanceWindow != target.Trust.MaintenanceWindow {
		target.Trust.MaintenanceWindow = fresh.Trust.MaintenanceWindow
		changed = append(changed, "trust.maintenance_window")
	}
	target.Trust.Tier3Safe = fresh.Trust.Tier3Safe
	target.Trust.Tier3Moderate = fresh.Trust.Tier3Moderate

	// LLM fields.
	if fresh.LLM.Enabled != target.LLM.Enabled {
		target.LLM.Enabled = fresh.LLM.Enabled
		changed = append(changed, "llm.enabled")
	}
	if fresh.LLM.Endpoint != "" && fresh.LLM.Endpoint != target.LLM.Endpoint {
		target.LLM.Endpoint = fresh.LLM.Endpoint
		changed = append(changed, "llm.endpoint")
	}
	if fresh.LLM.Model != "" && fresh.LLM.Model != target.LLM.Model {
		target.LLM.Model = fresh.LLM.Model
		changed = append(changed, "llm.model")
	}

	// Retention fields.
	if fresh.Retention.SnapshotsDays != 0 &&
		fresh.Retention.SnapshotsDays != target.Retention.SnapshotsDays {
		target.Retention.SnapshotsDays = fresh.Retention.SnapshotsDays
		changed = append(changed, "retention.snapshots_days")
	}

	return changed
}

// Current returns a read-locked copy of the current config.
func (w *Watcher) Current() *Config {
	w.mu.RLock()
	defer w.mu.RUnlock()
	c := *w.current
	return &c
}
