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
	snap := model.ClusterSnapshot{Health: model.ClusterHealth{Status: "yellow"}}
	stop := layer.ShouldStopDetailed(snap, "green")
	if !stop.Stop || stop.Code != ReasonHealthDegraded {
		t.Fatalf("expected health degrade stop, got %+v", stop)
	}
}
