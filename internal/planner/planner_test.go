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

func TestBuildPrefersSmallShardMovesUnderSevereShardImbalance(t *testing.T) {
	cfg := config.Default()
	cfg.Planner.MaxMovesPerPlan = 1
	p := New(cfg)

	shards := make([]model.Shard, 0, 120)
	for i := 0; i < 100; i++ {
		shards = append(shards, model.Shard{
			Index:   "small",
			ShardID: i,
			Primary: false,
			NodeID:  "a",
			SizeGB:  1,
			State:   "STARTED",
		})
	}
	shards = append(shards,
		model.Shard{Index: "big", ShardID: 0, Primary: false, NodeID: "b", SizeGB: 50, State: "STARTED"},
		model.Shard{Index: "big", ShardID: 1, Primary: false, NodeID: "b", SizeGB: 50, State: "STARTED"},
	)

	snap := model.ClusterSnapshot{
		ID:     "s2",
		Health: model.ClusterHealth{Status: "green"},
		Nodes: map[string]model.Node{
			"a": {ID: "a", Zone: "z1", DiskTotalGB: 200, DiskUsedGB: 100},
			"b": {ID: "b", Zone: "z2", DiskTotalGB: 200, DiskUsedGB: 100},
			"c": {ID: "c", Zone: "z3", DiskTotalGB: 200, DiskUsedGB: 20},
		},
		Shards:     shards,
		Watermarks: model.Watermarks{HighPercent: 95},
	}

	pl, err := p.Build(snap, model.AnalysisResult{Score: model.Score{DiskSkewPct: 100, ShardSkewPct: 100}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pl.Steps) == 0 {
		t.Fatalf("expected at least one move")
	}
	if pl.Steps[0].FromNode != "a" {
		t.Fatalf("expected move from shard-heavy node a, got %+v", pl.Steps[0])
	}
	if pl.Steps[0].EstimatedCost.NetworkGB > 5 {
		t.Fatalf("expected small-shard move, got %.2f GB", pl.Steps[0].EstimatedCost.NetworkGB)
	}
}

func TestBuildSkipsTinyShards(t *testing.T) {
	cfg := config.Default()
	cfg.Planner.MaxMovesPerPlan = 5
	cfg.Planner.MinMoveShardSizeGB = 0.01
	p := New(cfg)
	snap := model.ClusterSnapshot{
		ID:     "s3",
		Health: model.ClusterHealth{Status: "green"},
		Nodes: map[string]model.Node{
			"a": {ID: "a", Zone: "z1", DiskTotalGB: 100, DiskUsedGB: 80},
			"b": {ID: "b", Zone: "z2", DiskTotalGB: 100, DiskUsedGB: 40},
		},
		Shards: []model.Shard{
			{Index: "tiny", ShardID: 0, Primary: false, NodeID: "a", SizeGB: 0.00001, State: "STARTED"},
			{Index: "normal", ShardID: 1, Primary: false, NodeID: "a", SizeGB: 1, State: "STARTED"},
		},
		Watermarks: model.Watermarks{HighPercent: 95},
	}
	pl, err := p.Build(snap, model.AnalysisResult{Score: model.Score{DiskSkewPct: 100, ShardSkewPct: 100}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pl.Steps) == 0 {
		t.Fatalf("expected non-empty plan")
	}
	if pl.Steps[0].Index == "tiny" {
		t.Fatalf("expected tiny shard to be skipped")
	}
}

func TestBuildAvoidsTargetsAboveLowWatermark(t *testing.T) {
	cfg := config.Default()
	cfg.Planner.MaxMovesPerPlan = 2
	p := New(cfg)
	snap := model.ClusterSnapshot{
		ID:     "s4",
		Health: model.ClusterHealth{Status: "green"},
		Nodes: map[string]model.Node{
			"a": {ID: "a", Zone: "z1", DiskTotalGB: 100, DiskUsedGB: 90},
			"b": {ID: "b", Zone: "z2", DiskTotalGB: 100, DiskUsedGB: 86},
			"c": {ID: "c", Zone: "z3", DiskTotalGB: 100, DiskUsedGB: 40},
		},
		Shards: []model.Shard{
			{Index: "i", ShardID: 0, Primary: false, NodeID: "a", SizeGB: 2, State: "STARTED"},
			{Index: "i", ShardID: 1, Primary: false, NodeID: "a", SizeGB: 2, State: "STARTED"},
		},
		Watermarks: model.Watermarks{LowPercent: 85, HighPercent: 90},
	}

	pl, err := p.Build(snap, model.AnalysisResult{Score: model.Score{DiskSkewPct: 100, ShardSkewPct: 100}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pl.Steps) == 0 {
		t.Fatalf("expected non-empty plan")
	}
	for _, st := range pl.Steps {
		if st.ToNode == "b" {
			t.Fatalf("expected planner to avoid low-watermark saturated node b")
		}
	}
}

func TestBuildPrefersLargerMoveWhenSourceUnderTargetFreePressure(t *testing.T) {
	cfg := config.Default()
	cfg.Planner.MaxMovesPerPlan = 1
	cfg.Planner.TargetFreeGBPerNode = 40
	cfg.Planner.PressureMinShardSizeGB = 0.5
	cfg.Planner.MoveScorePressureSizeReward = 6
	p := New(cfg)
	snap := model.ClusterSnapshot{
		ID:     "s5",
		Health: model.ClusterHealth{Status: "green"},
		Nodes: map[string]model.Node{
			"a": {ID: "a", Zone: "z1", DiskTotalGB: 100, DiskUsedGB: 90}, // free=10, needs +30GB
			"b": {ID: "b", Zone: "z2", DiskTotalGB: 100, DiskUsedGB: 30},
		},
		Shards: []model.Shard{
			{Index: "small", ShardID: 0, Primary: false, NodeID: "a", SizeGB: 1, State: "STARTED"},
			{Index: "big", ShardID: 1, Primary: false, NodeID: "a", SizeGB: 20, State: "STARTED"},
		},
		Watermarks: model.Watermarks{LowPercent: 85, HighPercent: 90},
	}

	pl, err := p.Build(snap, model.AnalysisResult{Score: model.Score{DiskSkewPct: 100, ShardSkewPct: 100}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pl.Steps) == 0 {
		t.Fatalf("expected non-empty plan")
	}
	if pl.Steps[0].Index != "big" {
		t.Fatalf("expected pressure-aware larger move, got %+v", pl.Steps[0])
	}
}

func TestBuildContinuesWhileReducingFreeSpaceDeficit(t *testing.T) {
	cfg := config.Default()
	cfg.Planner.MaxMovesPerPlan = 3
	cfg.Planner.TargetFreeGBPerNode = 50
	cfg.Planner.NodeBalanceWeightPressure = 3
	cfg.Planner.PressureMinShardSizeGB = 0.1
	p := New(cfg)

	snap := model.ClusterSnapshot{
		ID:     "s6",
		Health: model.ClusterHealth{Status: "green"},
		Nodes: map[string]model.Node{
			"a": {ID: "a", Zone: "z1", DiskTotalGB: 100, DiskUsedGB: 95}, // free=5
			"b": {ID: "b", Zone: "z2", DiskTotalGB: 100, DiskUsedGB: 20},
		},
		Shards: []model.Shard{
			{Index: "x", ShardID: 0, Primary: false, NodeID: "a", SizeGB: 5, State: "STARTED"},
			{Index: "x", ShardID: 1, Primary: false, NodeID: "a", SizeGB: 5, State: "STARTED"},
			{Index: "x", ShardID: 2, Primary: false, NodeID: "a", SizeGB: 5, State: "STARTED"},
		},
		Watermarks: model.Watermarks{LowPercent: 95, HighPercent: 97},
	}

	pl, err := p.Build(snap, model.AnalysisResult{Score: model.Score{DiskSkewPct: 100, ShardSkewPct: 100}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pl.Steps) < 2 {
		t.Fatalf("expected planner to continue reducing free-space deficit, got %d steps", len(pl.Steps))
	}
}

func TestBuildRespectsLowWatermarkSafetyMargin(t *testing.T) {
	cfg := config.Default()
	cfg.Planner.MaxMovesPerPlan = 1
	cfg.Planner.LowWatermarkSafetyMarginPercent = 0.5
	p := New(cfg)
	snap := model.ClusterSnapshot{
		ID:     "s7",
		Health: model.ClusterHealth{Status: "green"},
		Nodes: map[string]model.Node{
			"a": {ID: "a", Zone: "z1", DiskTotalGB: 100, DiskUsedGB: 90},
			"b": {ID: "b", Zone: "z2", DiskTotalGB: 100, DiskUsedGB: 84.7}, // close to low=85
			"c": {ID: "c", Zone: "z3", DiskTotalGB: 100, DiskUsedGB: 40},
		},
		Shards: []model.Shard{
			{Index: "i", ShardID: 0, Primary: false, NodeID: "a", SizeGB: 0.6, State: "STARTED"},
		},
		Watermarks: model.Watermarks{LowPercent: 85, HighPercent: 90},
	}
	pl, err := p.Build(snap, model.AnalysisResult{Score: model.Score{DiskSkewPct: 100, ShardSkewPct: 100}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, st := range pl.Steps {
		if st.ToNode == "b" {
			t.Fatalf("expected safety margin to avoid node b as target")
		}
	}
}
