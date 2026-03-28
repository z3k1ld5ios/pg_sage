package tuner

import "testing"

func TestTickCooldowns_Decrements(t *testing.T) {
	tu := &Tuner{
		recentlyTuned: map[int64]int{42: 2},
	}
	tu.tickCooldowns()
	got, ok := tu.recentlyTuned[42]
	if !ok {
		t.Fatal("expected queryID 42 to still be present")
	}
	if got != 1 {
		t.Errorf("expected cooldown 1, got %d", got)
	}
}

func TestTickCooldowns_RemovesExpired(t *testing.T) {
	tu := &Tuner{
		recentlyTuned: map[int64]int{42: 1},
	}
	tu.tickCooldowns()
	if _, ok := tu.recentlyTuned[42]; ok {
		t.Error("expected queryID 42 to be removed after expiry")
	}
}

func TestCooldownDefault(t *testing.T) {
	tu := &Tuner{
		cfg: TunerConfig{CascadeCooldownCycles: 0},
	}
	got := tu.cooldownCycles()
	if got != 3 {
		t.Errorf("expected default cooldown 3, got %d", got)
	}
}

func TestCooldownConfigured(t *testing.T) {
	tu := &Tuner{
		cfg: TunerConfig{CascadeCooldownCycles: 5},
	}
	got := tu.cooldownCycles()
	if got != 5 {
		t.Errorf("expected configured cooldown 5, got %d", got)
	}
}
