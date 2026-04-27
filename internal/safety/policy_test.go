package safety

import (
	"testing"
	"time"

	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/model"
)

func TestEvaluateApplyRequiresSuccessfulDryRun(t *testing.T) {
	layer := New(config.Default())
	snap := model.ClusterSnapshot{ID: "snap", Health: model.ClusterHealth{Status: "green"}}
	plan := model.RebalancePlan{SnapshotID: "snap"}

	d := layer.EvaluateApply(snap, plan, nil, time.Now().UTC())
	if d.Allowed || d.Code != ReasonDryRunMissing {
		t.Fatalf("expected dry_run_missing, got %+v", d)
	}

	sim := &model.SimulationResult{SnapshotID: "snap", Succeeded: false}
	d = layer.EvaluateApply(snap, plan, sim, time.Now().UTC())
	if d.Allowed || d.Code != ReasonDryRunFailed {
		t.Fatalf("expected dry_run_failed, got %+v", d)
	}

	sim.Succeeded = true
	d = layer.EvaluateApply(snap, plan, sim, time.Now().UTC())
	if !d.Allowed {
		t.Fatalf("expected success, got %+v", d)
	}
}

func TestShouldStopOnDegrade(t *testing.T) {
	cfg := config.Default()
	cfg.Policy.StopOnHealthDegrade = true
	layer := New(cfg)
	baseline := model.ClusterSnapshot{Health: model.ClusterHealth{Status: "green"}}
	snap := model.ClusterSnapshot{Health: model.ClusterHealth{Status: "yellow"}}
	stop := layer.ShouldStopDetailed(snap, baseline)
	if !stop.Stop || stop.Code != ReasonHealthDegraded {
		t.Fatalf("expected health degrade stop, got %+v", stop)
	}
}

func TestShouldNotStopIfNodeAlreadyAboveCriticalInBaseline(t *testing.T) {
	cfg := config.Default()
	cfg.Policy.StopOnWatermarkBreach = true
	cfg.Policy.CriticalDiskPercent = 84
	layer := New(cfg)

	baseline := model.ClusterSnapshot{
		Nodes: map[string]model.Node{
			"n1": {ID: "n1", DiskTotalGB: 100, DiskUsedGB: 90},
		},
	}
	snap := model.ClusterSnapshot{
		Nodes: map[string]model.Node{
			"n1": {ID: "n1", DiskTotalGB: 100, DiskUsedGB: 89},
		},
	}
	stop := layer.ShouldStopDetailed(snap, baseline)
	if stop.Stop {
		t.Fatalf("expected no stop for already-over-threshold baseline, got %+v", stop)
	}
}

func TestShouldStopOnCriticalCrossing(t *testing.T) {
	cfg := config.Default()
	cfg.Policy.StopOnWatermarkBreach = true
	cfg.Policy.CriticalDiskPercent = 84
	layer := New(cfg)

	baseline := model.ClusterSnapshot{
		Nodes: map[string]model.Node{
			"n1": {ID: "n1", DiskTotalGB: 100, DiskUsedGB: 83},
		},
	}
	snap := model.ClusterSnapshot{
		Nodes: map[string]model.Node{
			"n1": {ID: "n1", DiskTotalGB: 100, DiskUsedGB: 85},
		},
	}
	stop := layer.ShouldStopDetailed(snap, baseline)
	if !stop.Stop || stop.Code != ReasonCriticalDiskExceeded {
		t.Fatalf("expected critical crossing stop, got %+v", stop)
	}
}
