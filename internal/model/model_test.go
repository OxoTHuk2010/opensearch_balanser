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
