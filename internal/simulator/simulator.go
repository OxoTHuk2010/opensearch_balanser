package simulator

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"opensearch-balanser/internal/collector"
	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/model"
)

func Run(ctx context.Context, cfg config.Config, adapter collector.Adapter, snapshot model.ClusterSnapshot, plan model.RebalancePlan) (model.SimulationResult, error) {
	if snapshot.ID != plan.SnapshotID {
		return model.SimulationResult{}, fmt.Errorf("plan snapshot mismatch: plan=%s snapshot=%s", plan.SnapshotID, snapshot.ID)
	}
	work := model.CloneSnapshot(snapshot)
	conflicts := make([]string, 0)
	checks := make([]model.AllocatorCheck, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		if !containsShard(work, step) {
			conflicts = append(conflicts, fmt.Sprintf("step %s/%d missing source shard on %s", step.Index, step.ShardID, step.FromNode))
			continue
		}
		if cfg.Runtime.CheckAllocator && adapter != nil {
			check, err := adapter.ValidateMove(ctx, step)
			if err != nil {
				conflicts = append(conflicts, fmt.Sprintf("allocator check failed for %s/%d: %v", step.Index, step.ShardID, err))
				continue
			}
			checks = append(checks, check)
			if !check.Allowed {
				conflicts = append(conflicts, fmt.Sprintf("allocator denied %s", check.StepRef))
				continue
			}
		}
		if !respectsWatermark(work, step) {
			conflicts = append(conflicts, fmt.Sprintf("step %s/%d breaches watermark on %s", step.Index, step.ShardID, step.ToNode))
			continue
		}
		applyMove(&work, step)
	}

	score := computeScore(work)
	succeeded := len(conflicts) == 0
	summary := "dry-run completed successfully"
	if !succeeded {
		summary = fmt.Sprintf("dry-run completed with %d conflict(s)", len(conflicts))
	}
	conflictSummary := summarizeConflicts(conflicts)

	return model.SimulationResult{
		RunAt:           time.Now().UTC(),
		SnapshotID:      snapshot.ID,
		Succeeded:       succeeded,
		Conflicts:       conflicts,
		ConflictSummary: conflictSummary,
		AllocatorChecks: checks,
		ExpectedScore:   score,
		Summary:         summary,
	}, nil
}

func containsShard(snapshot model.ClusterSnapshot, step model.PlanStep) bool {
	for _, s := range snapshot.Shards {
		if s.Index == step.Index && s.ShardID == step.ShardID && s.Primary == step.Primary && s.NodeID == step.FromNode {
			return true
		}
	}
	return false
}

func respectsWatermark(snapshot model.ClusterSnapshot, step model.PlanStep) bool {
	toNode, ok := snapshot.Nodes[step.ToNode]
	if !ok || toNode.DiskTotalGB <= 0 {
		return false
	}
	size := shardSize(snapshot, step)
	after := ((toNode.DiskUsedGB + size) / toNode.DiskTotalGB) * 100
	return after < snapshot.Watermarks.HighPercent
}

func shardSize(snapshot model.ClusterSnapshot, step model.PlanStep) float64 {
	for _, s := range snapshot.Shards {
		if s.Index == step.Index && s.ShardID == step.ShardID && s.Primary == step.Primary && s.NodeID == step.FromNode {
			return s.SizeGB
		}
	}
	return 0
}

func applyMove(snapshot *model.ClusterSnapshot, step model.PlanStep) {
	size := shardSize(*snapshot, step)
	for i := range snapshot.Shards {
		s := snapshot.Shards[i]
		if s.Index == step.Index && s.ShardID == step.ShardID && s.Primary == step.Primary && s.NodeID == step.FromNode {
			snapshot.Shards[i].NodeID = step.ToNode
			break
		}
	}
	from := snapshot.Nodes[step.FromNode]
	to := snapshot.Nodes[step.ToNode]
	from.DiskUsedGB = math.Max(0, from.DiskUsedGB-size)
	to.DiskUsedGB += size
	snapshot.Nodes[step.FromNode] = from
	snapshot.Nodes[step.ToNode] = to
}

func computeScore(snapshot model.ClusterSnapshot) model.Score {
	if len(snapshot.Nodes) == 0 {
		return model.Score{}
	}
	counts := map[string]float64{}
	diskMin, diskMax := math.MaxFloat64, -math.MaxFloat64
	for id := range snapshot.Nodes {
		counts[id] = 0
	}
	for _, s := range snapshot.Shards {
		counts[s.NodeID]++
	}
	shardMin, shardMax := math.MaxFloat64, -math.MaxFloat64
	for id, node := range snapshot.Nodes {
		dp := node.DiskUsedPercent()
		diskMin = math.Min(diskMin, dp)
		diskMax = math.Max(diskMax, dp)
		shardMin = math.Min(shardMin, counts[id])
		shardMax = math.Max(shardMax, counts[id])
	}
	diskSkew := 0.0
	if diskMax > 0 {
		diskSkew = ((diskMax - diskMin) / diskMax) * 100
	}
	shardSkew := 0.0
	if shardMax > 0 {
		shardSkew = ((shardMax - shardMin) / shardMax) * 100
	}
	return model.Score{DiskSkewPct: diskSkew, ShardSkewPct: shardSkew}
}

func summarizeConflicts(conflicts []string) map[string]int {
	if len(conflicts) == 0 {
		return nil
	}
	out := map[string]int{}
	for _, c := range conflicts {
		k := "other"
		switch {
		case strings.Contains(c, "allocator check failed"):
			k = "allocator_check_failed"
			if strings.Contains(c, "low watermark") {
				k = "allocator_low_watermark"
			}
		case strings.Contains(c, "allocator denied"):
			k = "allocator_denied"
		case strings.Contains(c, "breaches watermark"):
			k = "simulated_watermark_breach"
		case strings.Contains(c, "missing source shard"):
			k = "missing_source_shard"
		}
		out[k]++
	}
	return out
}
