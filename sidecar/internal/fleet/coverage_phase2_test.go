package fleet

import (
	"testing"
	"time"

	"github.com/pg-sage/sidecar/internal/config"
)

// ---------------------------------------------------------------------------
// TestPhase2_AllPools — covers AllPools (was 0%)
// ---------------------------------------------------------------------------

func TestPhase2_AllPools_Empty(t *testing.T) {
	mgr := NewManager(&config.Config{Mode: "fleet"})
	pools := mgr.AllPools()
	if len(pools) != 0 {
		t.Errorf("expected 0 pools, got %d", len(pools))
	}
}

func TestPhase2_AllPools_NilPoolsSkipped(t *testing.T) {
	mgr := NewManager(&config.Config{Mode: "fleet"})
	mgr.RegisterInstance(&DatabaseInstance{
		Name:   "db1",
		Config: config.DatabaseConfig{Name: "db1"},
		Status: &InstanceStatus{Connected: true},
		Pool:   nil, // nil pool should be skipped
	})
	mgr.RegisterInstance(&DatabaseInstance{
		Name:   "db2",
		Config: config.DatabaseConfig{Name: "db2"},
		Status: &InstanceStatus{Connected: true},
		Pool:   nil,
	})
	pools := mgr.AllPools()
	if len(pools) != 0 {
		t.Errorf("expected 0 pools (all nil), got %d", len(pools))
	}
}

func TestPhase2_AllPools_MultipleInstances(t *testing.T) {
	// With nil pools but verifying the filter logic works.
	// We can't create real pgxpool.Pool without a DB, but we
	// confirm the method iterates all instances and returns
	// only non-nil pools.
	mgr := newTestManager("db1", "db2", "db3")
	pools := mgr.AllPools()
	// newTestManager creates instances with nil pools.
	if len(pools) != 0 {
		t.Errorf("expected 0 non-nil pools, got %d", len(pools))
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_ResolveDatabaseName — covers ResolveDatabaseName (was 0%)
// ---------------------------------------------------------------------------

func TestPhase2_ResolveDatabaseName_ExplicitName(t *testing.T) {
	mgr := newTestManager("db1", "db2")
	got := mgr.ResolveDatabaseName("db1")
	if got != "db1" {
		t.Errorf("expected 'db1', got %q", got)
	}
}

func TestPhase2_ResolveDatabaseName_ExplicitUnknown(t *testing.T) {
	mgr := newTestManager("db1")
	// Explicit name that doesn't match any instance is returned as-is.
	got := mgr.ResolveDatabaseName("nonexistent")
	if got != "nonexistent" {
		t.Errorf("expected 'nonexistent', got %q", got)
	}
}

func TestPhase2_ResolveDatabaseName_EmptyReturnsFirst(t *testing.T) {
	mgr := newTestManager("alpha")
	got := mgr.ResolveDatabaseName("")
	if got != "alpha" {
		t.Errorf("expected 'alpha', got %q", got)
	}
}

func TestPhase2_ResolveDatabaseName_AllReturnsFirst(t *testing.T) {
	mgr := newTestManager("beta")
	got := mgr.ResolveDatabaseName("all")
	if got != "beta" {
		t.Errorf("expected 'beta', got %q", got)
	}
}

func TestPhase2_ResolveDatabaseName_EmptyManagerReturnsOriginal(
	t *testing.T,
) {
	mgr := NewManager(&config.Config{Mode: "fleet"})
	got := mgr.ResolveDatabaseName("")
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestPhase2_ResolveDatabaseName_AllEmptyManager(t *testing.T) {
	mgr := NewManager(&config.Config{Mode: "fleet"})
	got := mgr.ResolveDatabaseName("all")
	if got != "all" {
		t.Errorf("expected 'all' (no instances), got %q", got)
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_Config — covers Config() accessor (was 0%)
// ---------------------------------------------------------------------------

func TestPhase2_Config_ReturnsConfigPtr(t *testing.T) {
	cfg := &config.Config{Mode: "standalone"}
	mgr := NewManager(cfg)
	got := mgr.Config()
	if got != cfg {
		t.Error("Config() should return the same pointer")
	}
}

func TestPhase2_Config_ModeValue(t *testing.T) {
	cfg := &config.Config{Mode: "fleet"}
	mgr := NewManager(cfg)
	if mgr.Config().Mode != "fleet" {
		t.Errorf("expected mode 'fleet', got %q",
			mgr.Config().Mode)
	}
}

func TestPhase2_Config_MutationReflected(t *testing.T) {
	cfg := &config.Config{Mode: "standalone"}
	mgr := NewManager(cfg)
	cfg.Mode = "fleet"
	if mgr.Config().Mode != "fleet" {
		t.Error("Config() should reflect mutations on original")
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_RemoveInstance — additional edge cases
// ---------------------------------------------------------------------------

func TestPhase2_RemoveInstance_NonExistent(t *testing.T) {
	mgr := newTestManager("db1")
	if mgr.RemoveInstance("nope") {
		t.Error("RemoveInstance should return false for unknown")
	}
	if mgr.InstanceCount() != 1 {
		t.Errorf("instance count should still be 1, got %d",
			mgr.InstanceCount())
	}
}

func TestPhase2_RemoveInstance_Existing(t *testing.T) {
	mgr := newTestManager("db1", "db2")
	if !mgr.RemoveInstance("db1") {
		t.Error("RemoveInstance should return true")
	}
	if mgr.InstanceCount() != 1 {
		t.Errorf("expected 1 remaining, got %d",
			mgr.InstanceCount())
	}
	if mgr.GetInstance("db1") != nil {
		t.Error("db1 should be nil after removal")
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_GetInstanceByDatabaseID — edge cases
// ---------------------------------------------------------------------------

func TestPhase2_GetInstanceByDatabaseID_Found(t *testing.T) {
	mgr := NewManager(&config.Config{Mode: "fleet"})
	mgr.RegisterInstance(&DatabaseInstance{
		Name:       "db1",
		DatabaseID: 42,
		Config:     config.DatabaseConfig{Name: "db1"},
		Status:     &InstanceStatus{Connected: true},
	})
	inst := mgr.GetInstanceByDatabaseID(42)
	if inst == nil {
		t.Fatal("expected instance with DatabaseID=42")
	}
	if inst.Name != "db1" {
		t.Errorf("expected 'db1', got %q", inst.Name)
	}
}

func TestPhase2_GetInstanceByDatabaseID_NotFound(t *testing.T) {
	mgr := newTestManager("db1")
	if mgr.GetInstanceByDatabaseID(999) != nil {
		t.Error("expected nil for non-matching DatabaseID")
	}
}

func TestPhase2_GetInstanceByDatabaseID_EmptyManager(t *testing.T) {
	mgr := NewManager(&config.Config{Mode: "fleet"})
	if mgr.GetInstanceByDatabaseID(0) != nil {
		t.Error("expected nil for empty manager")
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_FleetStatus_EmergencyStopped — verify flag propagation
// ---------------------------------------------------------------------------

func TestPhase2_FleetStatus_EmergencyStoppedFlag(t *testing.T) {
	mgr := newTestManager("db1", "db2")
	mgr.GetInstance("db1").Stopped = true
	status := mgr.FleetStatus()
	if !status.Summary.EmergencyStopped {
		t.Error("EmergencyStopped should be true when any stopped")
	}
}

func TestPhase2_FleetStatus_NotEmergencyStopped(t *testing.T) {
	mgr := newTestManager("db1")
	status := mgr.FleetStatus()
	if status.Summary.EmergencyStopped {
		t.Error("EmergencyStopped should be false when none stopped")
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_FleetStatus_ActionsTotals
// ---------------------------------------------------------------------------

func TestPhase2_FleetStatus_ActionsTotals(t *testing.T) {
	mgr := newTestManager("a", "b")
	mgr.GetInstance("a").Status.ActionsTotal = 10
	mgr.GetInstance("b").Status.ActionsTotal = 5
	status := mgr.FleetStatus()
	if status.Summary.TotalActions != 15 {
		t.Errorf("TotalActions: expected 15, got %d",
			status.Summary.TotalActions)
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_FleetStatus_DatabaseNamePopulated
// ---------------------------------------------------------------------------

func TestPhase2_FleetStatus_DatabaseNamePopulated(t *testing.T) {
	mgr := newTestManager("prod-01")
	status := mgr.FleetStatus()
	if len(status.Databases) != 1 {
		t.Fatalf("expected 1 database, got %d",
			len(status.Databases))
	}
	if status.Databases[0].Status.DatabaseName != "prod-01" {
		t.Errorf("DatabaseName: expected 'prod-01', got %q",
			status.Databases[0].Status.DatabaseName)
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_HealthScore edge cases
// ---------------------------------------------------------------------------

func TestPhase2_HealthScore_WarningsOnly(t *testing.T) {
	s := &InstanceStatus{
		Connected:       true,
		FindingsWarning: 10,
	}
	score := computeHealthScore(s)
	// 100 - 50 = 50
	if score != 50 {
		t.Errorf("expected 50, got %d", score)
	}
}

func TestPhase2_HealthScore_MixedFloorClamp(t *testing.T) {
	s := &InstanceStatus{
		Connected:        true,
		FindingsCritical: 2,
		FindingsWarning:  20,
	}
	// 100 - 50 - 100 = -50 -> clamped to 0
	score := computeHealthScore(s)
	if score != 0 {
		t.Errorf("expected 0 (floor), got %d", score)
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_Resume_All
// ---------------------------------------------------------------------------

func TestPhase2_Resume_All(t *testing.T) {
	mgr := newTestManager("a", "b", "c")
	mgr.EmergencyStop("")
	resumed := mgr.Resume("")
	if resumed != 3 {
		t.Errorf("expected 3 resumed, got %d", resumed)
	}
	for name, inst := range mgr.Instances() {
		if inst.Stopped {
			t.Errorf("%s should not be stopped", name)
		}
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_InstanceCount
// ---------------------------------------------------------------------------

func TestPhase2_InstanceCount_AfterRemoval(t *testing.T) {
	mgr := newTestManager("a", "b", "c")
	if mgr.InstanceCount() != 3 {
		t.Fatalf("initial: expected 3, got %d",
			mgr.InstanceCount())
	}
	mgr.RemoveInstance("b")
	if mgr.InstanceCount() != 2 {
		t.Errorf("after removal: expected 2, got %d",
			mgr.InstanceCount())
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_FleetStatus_HealthScoreComputedPerCall
// ---------------------------------------------------------------------------

func TestPhase2_FleetStatus_HealthScoreUpdatedEachCall(
	t *testing.T,
) {
	mgr := newTestManager("db1")
	s1 := mgr.FleetStatus()
	if s1.Databases[0].Status.HealthScore != 100 {
		t.Fatalf("initial: expected 100, got %d",
			s1.Databases[0].Status.HealthScore)
	}

	// Mutate the status and verify next call picks it up.
	mgr.GetInstance("db1").Status.FindingsCritical = 2
	s2 := mgr.FleetStatus()
	// 100 - 50 = 50
	if s2.Databases[0].Status.HealthScore != 50 {
		t.Errorf("after mutation: expected 50, got %d",
			s2.Databases[0].Status.HealthScore)
	}
}

// ---------------------------------------------------------------------------
// TestPhase2_FleetStatus_LastSeen
// ---------------------------------------------------------------------------

func TestPhase2_FleetStatus_LastSeen(t *testing.T) {
	mgr := NewManager(&config.Config{Mode: "fleet"})
	now := time.Now()
	mgr.RegisterInstance(&DatabaseInstance{
		Name:   "db1",
		Config: config.DatabaseConfig{Name: "db1"},
		Status: &InstanceStatus{
			Connected: true,
			LastSeen:  now,
		},
	})
	status := mgr.FleetStatus()
	if !status.Databases[0].Status.LastSeen.Equal(now) {
		t.Error("LastSeen should be preserved")
	}
}
