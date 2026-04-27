package analyzer

import (
	"testing"

	"opensearch-balanser/internal/model"
)

func TestAnalyzeDetectsDiskSkewAndFaultDomainViolation(t *testing.T) {
	snap := model.ClusterSnapshot{
		Health: model.ClusterHealth{Status: "green"},
		Nodes: map[string]model.Node{
			"n1": {ID: "n1", Zone: "a", DiskTotalGB: 100, DiskUsedGB: 95},
			"n2": {ID: "n2", Zone: "a", DiskTotalGB: 100, DiskUsedGB: 30},
		},
		Shards: []model.Shard{
			{Index: "i", ShardID: 0, Primary: true, NodeID: "n1", ReplicaID: "i/0"},
			{Index: "i", ShardID: 0, Primary: false, NodeID: "n2", ReplicaID: "i/0"},
		},
	}

	res := Analyze(snap, Options{CriticalDiskPercent: 92})
	if len(res.Findings) == 0 {
		t.Fatalf("expected findings")
	}
	var hasSkew, hasFault bool
	for _, f := range res.Findings {
		if f.Type == "disk_skew" {
			hasSkew = true
		}
		if f.Type == "fault_domain_violation" {
			hasFault = true
		}
	}
	if !hasSkew {
		t.Fatalf("expected disk skew finding")
	}
	if !hasFault {
		t.Fatalf("expected fault domain finding")
	}
}
