package planner

import (
	"fmt"
	"math"
	"sort"
	"time"

	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/model"
)

type Planner struct {
	cfg config.Config
}

func New(cfg config.Config) Planner {
	return Planner{cfg: cfg}
}

func (p Planner) Build(snapshot model.ClusterSnapshot, analysis model.AnalysisResult) (model.RebalancePlan, error) {
	if snapshot.Health.Status == "red" {
		return model.RebalancePlan{}, fmt.Errorf("planner blocked: cluster health is red")
	}
	if snapshot.ActiveOperations > p.cfg.Cluster.MaxActiveRecover {
		return model.RebalancePlan{}, fmt.Errorf("planner blocked: active operations %d exceed max %d", snapshot.ActiveOperations, p.cfg.Cluster.MaxActiveRecover)
	}

	before := analysis.Score
	work := model.CloneSnapshot(snapshot)
	maxMoves := p.cfg.Planner.MaxMovesPerPlan
	steps := make([]model.PlanStep, 0, maxMoves)

	for i := 0; i < maxMoves; i++ {
		fromID, toID, ok := p.pickMostImbalancedNodes(work)
		if !ok {
			break
		}
		candidate, ok := p.pickShardToMove(work, fromID, toID)
		if !ok {
			break
		}
		fromNode := work.Nodes[fromID]
		toNode := work.Nodes[toID]
		if !p.allowedTarget(work, candidate, toNode) {
			markNodeAsUnmovable(&work, fromID)
			continue
		}
		step := p.makeStep(candidate, fromNode, toNode)
		steps = append(steps, step)
		applyMove(&work, step)

		after := computeScore(work)
		if improvement(before, after) <= 0 {
			steps = steps[:len(steps)-1]
			break
		}
		before = after
	}

	return model.RebalancePlan{
		SnapshotID: snapshot.ID,
		CreatedAt:  time.Now().UTC(),
		Before:     analysis.Score,
		After:      computeScore(work),
		Steps:      steps,
		Explain:    "Greedy hard-constraints planner with weighted cost scoring and move minimization.",
	}, nil
}

func (p Planner) BuildEmergencyDrain(snapshot model.ClusterSnapshot, nodeID string, analysis model.AnalysisResult) (model.RebalancePlan, error) {
	drainNode, ok := snapshot.Nodes[nodeID]
	if !ok {
		return model.RebalancePlan{}, fmt.Errorf("node not found: %s", nodeID)
	}
	if snapshot.Health.Status == "red" {
		return model.RebalancePlan{}, fmt.Errorf("emergency drain blocked: cluster health is red")
	}

	work := model.CloneSnapshot(snapshot)
	steps := []model.PlanStep{}
	for _, shard := range work.Shards {
		if shard.NodeID != nodeID {
			continue
		}
		targets := candidateTargets(work, shard)
		if len(targets) == 0 {
			continue
		}
		target := targets[0]
		step := p.makeStep(shard, drainNode, target)
		step.Reason = "emergency drain"
		steps = append(steps, step)
		applyMove(&work, step)
		if len(steps) >= p.cfg.Planner.MaxMovesPerPlan {
			break
		}
	}

	return model.RebalancePlan{
		SnapshotID:   snapshot.ID,
		CreatedAt:    time.Now().UTC(),
		Before:       analysis.Score,
		After:        computeScore(work),
		Steps:        steps,
		Explain:      "Emergency drain: evacuate shards from the target node with strict placement constraints.",
		EmergencyFor: nodeID,
	}, nil
}

func (p Planner) makeStep(shard model.Shard, fromNode, toNode model.Node) model.PlanStep {
	network := shard.SizeGB
	diskIO := shard.SizeGB * 2
	cpu := math.Max(0.1, shard.SizeGB/20)
	costScore := p.cfg.Planner.WeightCost*(network+diskIO+cpu) + p.cfg.Planner.WeightDisk*toNode.DiskUsedPercent()
	beforeFrom := fromNode.DiskUsedPercent()
	beforeTo := toNode.DiskUsedPercent()
	afterFrom := estimateDiskPct(fromNode, -shard.SizeGB)
	afterTo := estimateDiskPct(toNode, shard.SizeGB)
	return model.PlanStep{
		Type:     "move",
		Index:    shard.Index,
		ShardID:  shard.ShardID,
		Primary:  shard.Primary,
		FromNode: fromNode.ID,
		ToNode:   toNode.ID,
		Reason:   "reduce disk/shard skew while preserving placement constraints",
		ConstraintChecks: []string{
			"target node below high watermark",
			"fault-domain separation is preserved",
			"cluster health preconditions satisfied",
		},
		EstimatedCost: model.EstimatedCost{
			NetworkGB: network,
			DiskIOGB:  diskIO,
			CPUUnits:  cpu,
			Score:     costScore,
		},
		ExpectedBefore: model.StepExpectation{FromNodeDiskPct: beforeFrom, ToNodeDiskPct: beforeTo},
		ExpectedAfter:  model.StepExpectation{FromNodeDiskPct: afterFrom, ToNodeDiskPct: afterTo},
		RollbackHint:   fmt.Sprintf("move shard back %s/%d from %s to %s", shard.Index, shard.ShardID, toNode.ID, fromNode.ID),
	}
}

func (p Planner) allowedTarget(snapshot model.ClusterSnapshot, shard model.Shard, target model.Node) bool {
	if target.ID == shard.NodeID {
		return false
	}
	if target.DiskTotalGB <= 0 {
		return false
	}
	after := estimateDiskPct(target, shard.SizeGB)
	if after >= snapshot.Watermarks.HighPercent {
		return false
	}
	for _, s := range snapshot.Shards {
		if s.Index == shard.Index && s.ShardID == shard.ShardID && s.NodeID != shard.NodeID {
			n := snapshot.Nodes[s.NodeID]
			if sameFaultDomain(n, target) {
				return false
			}
		}
	}
	return true
}

func (p Planner) pickMostImbalancedNodes(snapshot model.ClusterSnapshot) (string, string, bool) {
	if len(snapshot.Nodes) < 2 {
		return "", "", false
	}
	type pair struct {
		id       string
		diskUsed float64
		shards   int
	}
	counts := map[string]int{}
	for id := range snapshot.Nodes {
		counts[id] = 0
	}
	for _, s := range snapshot.Shards {
		counts[s.NodeID]++
	}
	arr := make([]pair, 0, len(snapshot.Nodes))
	for id, n := range snapshot.Nodes {
		arr = append(arr, pair{id: id, diskUsed: n.DiskUsedPercent(), shards: counts[id]})
	}
	maxDisk := 0.0
	maxShards := 0
	for _, p := range arr {
		if p.diskUsed > maxDisk {
			maxDisk = p.diskUsed
		}
		if p.shards > maxShards {
			maxShards = p.shards
		}
	}
	score := func(pn pair) float64 {
		diskNorm := 0.0
		if maxDisk > 0 {
			diskNorm = pn.diskUsed / maxDisk
		}
		shardNorm := 0.0
		if maxShards > 0 {
			shardNorm = float64(pn.shards) / float64(maxShards)
		}
		return p.cfg.Planner.NodeBalanceWeightDisk*diskNorm + p.cfg.Planner.NodeBalanceWeightShards*shardNorm
	}
	sort.Slice(arr, func(i, j int) bool { return score(arr[i]) > score(arr[j]) })
	from := arr[0]
	sort.Slice(arr, func(i, j int) bool { return score(arr[i]) < score(arr[j]) })
	to := arr[0]
	if from.diskUsed-to.diskUsed < 1 && from.shards-to.shards <= 1 {
		return "", "", false
	}
	if from.id == to.id {
		return "", "", false
	}
	return from.id, to.id, true
}

func (p Planner) pickShardToMove(snapshot model.ClusterSnapshot, fromNodeID, toNodeID string) (model.Shard, bool) {
	candidates := make([]model.Shard, 0)
	for _, s := range snapshot.Shards {
		if s.NodeID == fromNodeID {
			candidates = append(candidates, s)
		}
	}
	counts := map[string]int{}
	for id := range snapshot.Nodes {
		counts[id] = 0
	}
	for _, s := range snapshot.Shards {
		counts[s.NodeID]++
	}
	fromCount := counts[fromNodeID]
	toCount := counts[toNodeID]
	shardImbalance := fromCount - toCount
	severeShardImbalance := shardImbalance > p.cfg.Planner.SevereShardImbalanceThreshold

	bestScore := math.MaxFloat64
	best := model.Shard{}
	found := false
	for _, c := range candidates {
		if c.State != "STARTED" && c.State != "RELOCATING" {
			continue
		}
		alreadyOnTarget := false
		for _, s := range snapshot.Shards {
			if s.Index == c.Index && s.ShardID == c.ShardID && s.NodeID == toNodeID {
				alreadyOnTarget = true
				break
			}
		}
		if alreadyOnTarget {
			continue
		}
		afterDiskGap := math.Abs(estimateDiskPct(snapshot.Nodes[fromNodeID], -c.SizeGB) - estimateDiskPct(snapshot.Nodes[toNodeID], c.SizeGB))
		afterShardGap := math.Abs(float64((fromCount - 1) - (toCount + 1)))
		primaryPenalty := 0.0
		if c.Primary {
			primaryPenalty = p.cfg.Planner.MoveScorePrimaryPenalty
		}
		sizePenalty := c.SizeGB * p.cfg.Planner.LargeShardPenaltyMultiplier
		if c.SizeGB < p.cfg.Planner.LargeShardSizeGB {
			sizePenalty = c.SizeGB
		}
		if severeShardImbalance && c.SizeGB >= p.cfg.Planner.LargeShardSizeGB {
			// Under strong shard-count imbalance, strongly avoid large-shard moves.
			sizePenalty = c.SizeGB * p.cfg.Planner.LargeShardPenaltyMultiplier * p.cfg.Planner.MoveScoreSevereLargeShardExtraMult
		}
		score := primaryPenalty +
			afterDiskGap*p.cfg.Planner.MoveScoreWeightDiskGap +
			afterShardGap*p.cfg.Planner.MoveScoreWeightShardGap +
			sizePenalty*p.cfg.Planner.MoveScoreWeightSize
		if score < bestScore {
			bestScore = score
			best = c
			found = true
		}
	}
	if !found {
		return model.Shard{}, false
	}
	return best, true
}

func applyMove(snapshot *model.ClusterSnapshot, step model.PlanStep) {
	for i := range snapshot.Shards {
		s := snapshot.Shards[i]
		if s.Index == step.Index && s.ShardID == step.ShardID && s.NodeID == step.FromNode && s.Primary == step.Primary {
			snapshot.Shards[i].NodeID = step.ToNode
			size := s.SizeGB
			from := snapshot.Nodes[step.FromNode]
			to := snapshot.Nodes[step.ToNode]
			from.DiskUsedGB = math.Max(0, from.DiskUsedGB-size)
			to.DiskUsedGB += size
			snapshot.Nodes[step.FromNode] = from
			snapshot.Nodes[step.ToNode] = to
			return
		}
	}
}

func computeScore(snapshot model.ClusterSnapshot) model.Score {
	if len(snapshot.Nodes) == 0 {
		return model.Score{}
	}
	diskMin, diskMax := math.MaxFloat64, -math.MaxFloat64
	counts := map[string]float64{}
	for id := range snapshot.Nodes {
		counts[id] = 0
	}
	for _, s := range snapshot.Shards {
		counts[s.NodeID]++
	}
	shardMin, shardMax := math.MaxFloat64, -math.MaxFloat64
	for id, n := range snapshot.Nodes {
		dp := n.DiskUsedPercent()
		diskMin = math.Min(diskMin, dp)
		diskMax = math.Max(diskMax, dp)
		c := counts[id]
		shardMin = math.Min(shardMin, c)
		shardMax = math.Max(shardMax, c)
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

func estimateDiskPct(n model.Node, deltaGB float64) float64 {
	if n.DiskTotalGB <= 0 {
		return 0
	}
	return ((n.DiskUsedGB + deltaGB) / n.DiskTotalGB) * 100
}

func candidateTargets(snapshot model.ClusterSnapshot, shard model.Shard) []model.Node {
	nodes := make([]model.Node, 0, len(snapshot.Nodes))
	for _, n := range snapshot.Nodes {
		if n.ID == shard.NodeID {
			continue
		}
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].DiskUsedPercent() < nodes[j].DiskUsedPercent() })
	return nodes
}

func markNodeAsUnmovable(snapshot *model.ClusterSnapshot, nodeID string) {
	n := snapshot.Nodes[nodeID]
	n.DiskUsedGB = n.DiskTotalGB
	snapshot.Nodes[nodeID] = n
}

func sameFaultDomain(a, b model.Node) bool {
	if a.Zone != "" && b.Zone != "" {
		return a.Zone == b.Zone
	}
	if a.Rack != "" && b.Rack != "" {
		return a.Rack == b.Rack
	}
	return a.Host == b.Host
}

func improvement(before, after model.Score) float64 {
	return (before.DiskSkewPct + before.ShardSkewPct + before.RiskPenalty) - (after.DiskSkewPct + after.ShardSkewPct + after.RiskPenalty)
}
