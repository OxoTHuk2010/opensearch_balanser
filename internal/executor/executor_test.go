package executor

import (
	"context"
	"testing"

	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/model"
	"opensearch-balanser/internal/safety"
)

type fakeAdapter struct {
	snapshot model.ClusterSnapshot
}

func (f *fakeAdapter) CollectSnapshot(ctx context.Context) (model.ClusterSnapshot, error) {
	return f.snapshot, nil
}

func (f *fakeAdapter) ValidateMove(ctx context.Context, step model.PlanStep) (model.AllocatorCheck, error) {
	return model.AllocatorCheck{Allowed: true}, nil
}

func (f *fakeAdapter) ExecuteMove(ctx context.Context, step model.PlanStep) error {
	return nil
}

func TestRunWithStopRequested(t *testing.T) {
	cfg := config.Default()
	cfg.Runtime.RequireManualApproval = false
	snap := model.ClusterSnapshot{Health: model.ClusterHealth{Status: "green"}, Nodes: map[string]model.Node{"a": {ID: "a", DiskTotalGB: 100, DiskUsedGB: 50}}, Watermarks: model.Watermarks{HighPercent: 90}}
	ad := &fakeAdapter{snapshot: snap}
	plan := model.RebalancePlan{Steps: []model.PlanStep{{Index: "i", ShardID: 0, FromNode: "a", ToNode: "b", EstimatedCost: model.EstimatedCost{NetworkGB: 1}}}}
	res, err := RunWithOptions(context.Background(), cfg, ad, safety.New(cfg), snap, plan, RunOptions{StopRequested: func() bool { return true }})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Stopped || res.StopReasonCode != "stop_requested" {
		t.Fatalf("expected stop requested, got %+v", res)
	}
}
