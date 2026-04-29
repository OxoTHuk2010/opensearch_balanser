package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type ClusterSnapshot struct {
	ID               string            `json:"id"`
	CollectedAt      time.Time         `json:"collected_at"`
	Health           ClusterHealth     `json:"health"`
	Nodes            map[string]Node   `json:"nodes"`
	Shards           []Shard           `json:"shards"`
	ActiveOperations int               `json:"active_operations"`
	Watermarks       Watermarks        `json:"watermarks"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

type ClusterHealth struct {
	Status string `json:"status"`
}

type Node struct {
	ID          string   `json:"id"`
	Host        string   `json:"host"`
	IP          string   `json:"ip"`
	Zone        string   `json:"zone"`
	Rack        string   `json:"rack"`
	Status      string   `json:"status"`
	Roles       []string `json:"roles"`
	DiskTotalGB float64  `json:"disk_total_gb"`
	DiskUsedGB  float64  `json:"disk_used_gb"`
}

type Shard struct {
	Index     string  `json:"index"`
	ShardID   int     `json:"shard_id"`
	Primary   bool    `json:"primary"`
	State     string  `json:"state"`
	NodeID    string  `json:"node_id"`
	SizeGB    float64 `json:"size_gb"`
	ReplicaID string  `json:"replica_id"`
}

type Watermarks struct {
	LowPercent        float64 `json:"low_percent"`
	HighPercent       float64 `json:"high_percent"`
	FloodStagePercent float64 `json:"flood_stage_percent"`
}

type AnalysisResult struct {
	GeneratedAt time.Time `json:"generated_at"`
	Score       Score     `json:"score"`
	Findings    []Finding `json:"findings"`
}

type Finding struct {
	Type       string `json:"type"`
	Severity   string `json:"severity"`
	Message    string `json:"message"`
	NodeID     string `json:"node_id,omitempty"`
	ShardRef   string `json:"shard_ref,omitempty"`
	Constraint string `json:"constraint,omitempty"`
}

type Score struct {
	DiskSkewPct  float64 `json:"disk_skew_pct"`
	ShardSkewPct float64 `json:"shard_skew_pct"`
	RiskPenalty  float64 `json:"risk_penalty"`
}

type RebalancePlan struct {
	SnapshotID   string     `json:"snapshot_id"`
	CreatedAt    time.Time  `json:"created_at"`
	Before       Score      `json:"before"`
	After        Score      `json:"after"`
	Steps        []PlanStep `json:"steps"`
	Explain      string     `json:"explain"`
	EmergencyFor string     `json:"emergency_for,omitempty"`
}

type PlanStep struct {
	Type             string          `json:"type"`
	Index            string          `json:"index"`
	ShardID          int             `json:"shard_id"`
	Primary          bool            `json:"primary"`
	FromNode         string          `json:"from_node"`
	ToNode           string          `json:"to_node"`
	Reason           string          `json:"reason"`
	ConstraintChecks []string        `json:"constraint_checks"`
	EstimatedCost    EstimatedCost   `json:"estimated_cost"`
	ExpectedBefore   StepExpectation `json:"expected_before"`
	ExpectedAfter    StepExpectation `json:"expected_after"`
	RollbackHint     string          `json:"rollback_hint"`
}

type StepExpectation struct {
	FromNodeDiskPct float64 `json:"from_node_disk_pct"`
	ToNodeDiskPct   float64 `json:"to_node_disk_pct"`
}

type EstimatedCost struct {
	NetworkGB float64 `json:"network_gb"`
	DiskIOGB  float64 `json:"disk_io_gb"`
	CPUUnits  float64 `json:"cpu_units"`
	Score     float64 `json:"score"`
}

type AllocatorCheck struct {
	StepRef   string `json:"step_ref"`
	Allowed   bool   `json:"allowed"`
	Reason    string `json:"reason,omitempty"`
	Allocator string `json:"allocator,omitempty"`
}

type SimulationResult struct {
	RunAt           time.Time        `json:"run_at"`
	SnapshotID      string           `json:"snapshot_id"`
	Succeeded       bool             `json:"succeeded"`
	Conflicts       []string         `json:"conflicts"`
	ConflictSummary map[string]int   `json:"conflict_summary,omitempty"`
	AllocatorChecks []AllocatorCheck `json:"allocator_checks,omitempty"`
	ExpectedScore   Score            `json:"expected_score"`
	Summary         string           `json:"summary"`
}

type PlanBundle struct {
	Snapshot   ClusterSnapshot   `json:"snapshot"`
	Analysis   AnalysisResult    `json:"analysis"`
	Plan       RebalancePlan     `json:"plan"`
	Simulation *SimulationResult `json:"simulation,omitempty"`
}

type ExecutionStatus string

const (
	ExecutionPending   ExecutionStatus = "pending"
	ExecutionRunning   ExecutionStatus = "running"
	ExecutionStopped   ExecutionStatus = "stopped"
	ExecutionFailed    ExecutionStatus = "failed"
	ExecutionCompleted ExecutionStatus = "completed"
)

type Execution struct {
	ID             string            `json:"id"`
	CorrelationID  string            `json:"correlation_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	Mode           string            `json:"mode"`
	Status         ExecutionStatus   `json:"status"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	SnapshotID     string            `json:"snapshot_id"`
	Plan           RebalancePlan     `json:"plan"`
	Simulation     *SimulationResult `json:"simulation,omitempty"`
	CurrentBatch   int               `json:"current_batch"`
	TotalBatches   int               `json:"total_batches"`
	AppliedSteps   int               `json:"applied_steps"`
	StopRequested  bool              `json:"stop_requested"`
	StopReasonCode string            `json:"stop_reason_code,omitempty"`
	StopReason     string            `json:"stop_reason,omitempty"`
	Errors         []string          `json:"errors,omitempty"`
	AuditTrail     []AuditRecord     `json:"audit_trail,omitempty"`
}

type AuditRecord struct {
	At          time.Time `json:"at"`
	ExecutionID string    `json:"execution_id"`
	Batch       int       `json:"batch"`
	Action      string    `json:"action"`
	StepRef     string    `json:"step_ref,omitempty"`
	Result      string    `json:"result"`
	Reason      string    `json:"reason,omitempty"`
}

func (s ClusterSnapshot) ComputeID() (string, error) {
	type stableNode struct {
		ID          string   `json:"id"`
		Host        string   `json:"host"`
		IP          string   `json:"ip"`
		Zone        string   `json:"zone"`
		Rack        string   `json:"rack"`
		Status      string   `json:"status"`
		Roles       []string `json:"roles"`
		DiskTotalGB float64  `json:"disk_total_gb"`
	}
	type stableShard struct {
		Index   string `json:"index"`
		ShardID int    `json:"shard_id"`
		Primary bool   `json:"primary"`
		NodeID  string `json:"node_id"`
	}
	stable := struct {
		Health     ClusterHealth `json:"health"`
		Watermarks Watermarks    `json:"watermarks"`
		Nodes      []stableNode  `json:"nodes"`
		Shards     []stableShard `json:"shards"`
	}{
		Health:     s.Health,
		Watermarks: s.Watermarks,
		Shards:     make([]stableShard, 0, len(s.Shards)),
	}

	for _, n := range s.Nodes {
		roles := append([]string(nil), n.Roles...)
		sort.Strings(roles)
		stable.Nodes = append(stable.Nodes, stableNode{
			ID:          n.ID,
			Host:        n.Host,
			IP:          n.IP,
			Zone:        n.Zone,
			Rack:        n.Rack,
			Status:      n.Status,
			Roles:       roles,
			DiskTotalGB: n.DiskTotalGB,
		})
	}
	for _, sh := range s.Shards {
		stable.Shards = append(stable.Shards, stableShard{
			Index:   sh.Index,
			ShardID: sh.ShardID,
			Primary: sh.Primary,
			NodeID:  sh.NodeID,
		})
	}

	sort.Slice(stable.Nodes, func(i, j int) bool { return stable.Nodes[i].ID < stable.Nodes[j].ID })
	sort.Slice(stable.Shards, func(i, j int) bool {
		a, b := stable.Shards[i], stable.Shards[j]
		if a.Index != b.Index {
			return a.Index < b.Index
		}
		if a.ShardID != b.ShardID {
			return a.ShardID < b.ShardID
		}
		if a.Primary != b.Primary {
			return a.Primary
		}
		return a.NodeID < b.NodeID
	})

	body, err := json.Marshal(stable)
	if err != nil {
		return "", fmt.Errorf("marshal stable snapshot: %w", err)
	}
	d := sha256.Sum256(body)
	return hex.EncodeToString(d[:]), nil
}

func (n Node) DiskUsedPercent() float64 {
	if n.DiskTotalGB <= 0 {
		return 0
	}
	return (n.DiskUsedGB / n.DiskTotalGB) * 100
}

func CloneSnapshot(in ClusterSnapshot) ClusterSnapshot {
	out := in
	out.Nodes = make(map[string]Node, len(in.Nodes))
	for k, v := range in.Nodes {
		vv := v
		vv.Roles = append([]string(nil), v.Roles...)
		out.Nodes[k] = vv
	}
	out.Shards = append([]Shard(nil), in.Shards...)
	if in.Metadata != nil {
		out.Metadata = make(map[string]string, len(in.Metadata))
		for k, v := range in.Metadata {
			out.Metadata[k] = v
		}
	}
	return out
}
