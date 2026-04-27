package analyzer

import (
	"fmt"
	"math"
	"sort"
	"time"

	"opensearch-balanser/internal/model"
)

type Options struct {
	DiskSkewThresholdPct  float64
	ShardSkewThresholdPct float64
	CriticalDiskPercent   float64
}

func Analyze(snapshot model.ClusterSnapshot, opt Options) model.AnalysisResult {
	if opt.DiskSkewThresholdPct <= 0 {
		opt.DiskSkewThresholdPct = 10
	}
	if opt.ShardSkewThresholdPct <= 0 {
		opt.ShardSkewThresholdPct = 15
	}
	if opt.CriticalDiskPercent <= 0 {
		opt.CriticalDiskPercent = 92
	}

	diskPcts := make([]float64, 0, len(snapshot.Nodes))
	shardsPerNode := map[string]int{}
	for id, n := range snapshot.Nodes {
		diskPcts = append(diskPcts, n.DiskUsedPercent())
		shardsPerNode[id] = 0
	}
	for _, s := range snapshot.Shards {
		shardsPerNode[s.NodeID]++
	}

	findings := make([]model.Finding, 0)

	diskSkew := pctSkew(diskPcts)
	if diskSkew > opt.DiskSkewThresholdPct {
		findings = append(findings, model.Finding{
			Type:     "disk_skew",
			Severity: "major",
			Message:  fmt.Sprintf("disk skew is %.2f%%, above threshold %.2f%%", diskSkew, opt.DiskSkewThresholdPct),
		})
	}

	shardCounts := make([]float64, 0, len(shardsPerNode))
	for _, c := range shardsPerNode {
		shardCounts = append(shardCounts, float64(c))
	}
	shardSkew := pctSkew(shardCounts)
	if shardSkew > opt.ShardSkewThresholdPct {
		findings = append(findings, model.Finding{
			Type:     "shard_skew",
			Severity: "major",
			Message:  fmt.Sprintf("shard skew is %.2f%%, above threshold %.2f%%", shardSkew, opt.ShardSkewThresholdPct),
		})
	}

	for id, n := range snapshot.Nodes {
		if n.DiskUsedPercent() >= opt.CriticalDiskPercent {
			findings = append(findings, model.Finding{
				Type:     "overflow_risk",
				Severity: "critical",
				NodeID:   id,
				Message:  fmt.Sprintf("node %s at %.2f%% disk usage", id, n.DiskUsedPercent()),
			})
		}
	}

	byReplica := map[string][]model.Shard{}
	for _, s := range snapshot.Shards {
		byReplica[s.ReplicaID] = append(byReplica[s.ReplicaID], s)
	}
	for replicaID, copies := range byReplica {
		zoneSet := map[string]bool{}
		for _, cp := range copies {
			n, ok := snapshot.Nodes[cp.NodeID]
			if !ok {
				continue
			}
			zone := n.Zone
			if zone == "" {
				zone = "unknown"
			}
			zoneSet[zone] = true
		}
		if len(copies) > 1 && len(zoneSet) == 1 {
			findings = append(findings, model.Finding{
				Type:       "fault_domain_violation",
				Severity:   "critical",
				ShardRef:   replicaID,
				Constraint: "replicas must span multiple fault domains",
				Message:    fmt.Sprintf("all replicas for %s are in one zone", replicaID),
			})
		}
	}

	riskPenalty := 0.0
	for _, f := range findings {
		switch f.Severity {
		case "critical":
			riskPenalty += 2
		case "major":
			riskPenalty += 1
		case "minor":
			riskPenalty += 0.2
		}
	}

	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Severity == findings[j].Severity {
			return findings[i].Type < findings[j].Type
		}
		order := map[string]int{"critical": 0, "major": 1, "minor": 2}
		return order[findings[i].Severity] < order[findings[j].Severity]
	})

	return model.AnalysisResult{
		GeneratedAt: time.Now().UTC(),
		Score: model.Score{
			DiskSkewPct:  diskSkew,
			ShardSkewPct: shardSkew,
			RiskPenalty:  riskPenalty,
		},
		Findings: findings,
	}
}

func pctSkew(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	minV := math.MaxFloat64
	maxV := -math.MaxFloat64
	for _, v := range values {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	if maxV <= 0 {
		return 0
	}
	return ((maxV - minV) / maxV) * 100
}
