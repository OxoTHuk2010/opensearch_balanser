package model

import "testing"

func TestComputeIDIgnoresActiveOperations(t *testing.T) {
	base := ClusterSnapshot{
		Health:     ClusterHealth{Status: "green"},
		Watermarks: Watermarks{LowPercent: 85, HighPercent: 90, FloodStagePercent: 95},
		Nodes: map[string]Node{
			"n1": {ID: "n1", Host: "h1", IP: "10.0.0.1", Status: "online", Roles: []string{"d", "m"}, DiskTotalGB: 100, DiskUsedGB: 50},
		},
		Shards: []Shard{
			{Index: "idx", ShardID: 0, Primary: true, State: "STARTED", NodeID: "n1", SizeGB: 10, ReplicaID: "idx/0"},
		},
	}

	s1 := base
	s1.ActiveOperations = 0
	id1, err := s1.ComputeID()
	if err != nil {
		t.Fatalf("ComputeID s1 error: %v", err)
	}

	s2 := base
	s2.ActiveOperations = 999
	id2, err := s2.ComputeID()
	if err != nil {
		t.Fatalf("ComputeID s2 error: %v", err)
	}

	if id1 != id2 {
		t.Fatalf("expected same ID when only active_operations differ, got %s vs %s", id1, id2)
	}
}

func TestComputeIDIgnoresVolatileFields(t *testing.T) {
	base := ClusterSnapshot{
		Health:     ClusterHealth{Status: "green"},
		Watermarks: Watermarks{LowPercent: 85, HighPercent: 90, FloodStagePercent: 95},
		Nodes: map[string]Node{
			"n1": {ID: "n1", Host: "h1", IP: "10.0.0.1", Status: "online", Roles: []string{"d", "m"}, DiskTotalGB: 100, DiskUsedGB: 50},
			"n2": {ID: "n2", Host: "h2", IP: "10.0.0.2", Status: "online", Roles: []string{"d"}, DiskTotalGB: 100, DiskUsedGB: 10},
		},
		Shards: []Shard{
			{Index: "idx", ShardID: 0, Primary: true, State: "STARTED", NodeID: "n1", SizeGB: 10, ReplicaID: "idx/0"},
		},
	}

	id1, err := base.ComputeID()
	if err != nil {
		t.Fatalf("ComputeID base error: %v", err)
	}

	changed := base
	changed.Nodes = CloneSnapshot(base).Nodes
	n1 := changed.Nodes["n1"]
	n1.DiskUsedGB = 73
	changed.Nodes["n1"] = n1
	changed.Shards = append([]Shard(nil), base.Shards...)
	changed.Shards[0].State = "RELOCATING"
	changed.Shards[0].SizeGB = 17.2

	id2, err := changed.ComputeID()
	if err != nil {
		t.Fatalf("ComputeID changed error: %v", err)
	}

	if id1 != id2 {
		t.Fatalf("expected same ID for volatile-only changes, got %s vs %s", id1, id2)
	}
}

func TestComputeIDChangesOnShardPlacementChange(t *testing.T) {
	base := ClusterSnapshot{
		Health:     ClusterHealth{Status: "green"},
		Watermarks: Watermarks{LowPercent: 85, HighPercent: 90, FloodStagePercent: 95},
		Nodes: map[string]Node{
			"n1": {ID: "n1", Host: "h1", IP: "10.0.0.1", Status: "online", Roles: []string{"d"}, DiskTotalGB: 100},
			"n2": {ID: "n2", Host: "h2", IP: "10.0.0.2", Status: "online", Roles: []string{"d"}, DiskTotalGB: 100},
		},
		Shards: []Shard{
			{Index: "idx", ShardID: 0, Primary: true, NodeID: "n1"},
		},
	}

	id1, err := base.ComputeID()
	if err != nil {
		t.Fatalf("ComputeID base error: %v", err)
	}

	moved := base
	moved.Shards = append([]Shard(nil), base.Shards...)
	moved.Shards[0].NodeID = "n2"
	id2, err := moved.ComputeID()
	if err != nil {
		t.Fatalf("ComputeID moved error: %v", err)
	}

	if id1 == id2 {
		t.Fatalf("expected different ID when shard placement changes")
	}
}
