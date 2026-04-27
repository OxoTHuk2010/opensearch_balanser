package planner

import (
	"testing"

	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/model"
)

func TestBuildBlockedOnRedCluster(t *testing.T) {
	p := New(config.Default())
	_, err := p.Build(model.ClusterSnapshot{Health: model.ClusterHealth{Status: "red"}}, model.AnalysisResult{})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestBuildProducesMoves(t *testing.T) {
	cfg := config.Default()
	cfg.Planner.MaxMovesPerPlan = 5
	p := New(cfg)
	snap := model.ClusterSnapshot{
		ID:     "s1",
		Health: model.ClusterHealth{Status: "green"},
		Nodes: map[string]model.Node{
			"a": {ID: "a", Zone: "z1", DiskTotalGB: 100, DiskUsedGB: 90},
			"b": {ID: "b", Zone: "z2", DiskTotalGB: 100, DiskUsedGB: 20},
		},
		Shards: []model.Shard{
			{Index: "i", ShardID: 0, Primary: false, NodeID: "a", SizeGB: 10, State: "STARTED"},
			{Index: "i", ShardID: 1, Primary: false, NodeID: "a", SizeGB: 10, State: "STARTED"},
		},
		Watermarks: model.Watermarks{HighPercent: 90},
	}
	pl, err := p.Build(snap, model.AnalysisResult{Score: model.Score{DiskSkewPct: 70, ShardSkewPct: 100}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pl.Steps) == 0 {
		t.Fatalf("expected at least one move")
	}
}
