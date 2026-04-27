package safety

import (
	"fmt"
	"strings"
	"time"

	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/model"
)

type ReasonCode string

const (
	ReasonOK                     ReasonCode = "ok"
	ReasonDryRunMissing          ReasonCode = "dry_run_missing"
	ReasonDryRunFailed           ReasonCode = "dry_run_failed"
	ReasonSnapshotMismatch       ReasonCode = "snapshot_mismatch"
	ReasonHealthRed              ReasonCode = "health_red"
	ReasonHealthYellowNotAllowed ReasonCode = "health_yellow_not_allowed"
	ReasonActiveOpsExceeded      ReasonCode = "active_operations_exceeded"
	ReasonExecutionWindowClosed  ReasonCode = "execution_window_closed"
	ReasonHealthDegraded         ReasonCode = "health_degraded"
	ReasonCriticalDiskExceeded   ReasonCode = "critical_disk_exceeded"
)

type PreflightDecision struct {
	Allowed bool       `json:"allowed"`
	Code    ReasonCode `json:"code"`
	Message string     `json:"message"`
}

type StopDecision struct {
	Stop    bool       `json:"stop"`
	Code    ReasonCode `json:"code"`
	Message string     `json:"message"`
}

type Layer struct {
	cfg config.Config
}

func New(cfg config.Config) Layer {
	return Layer{cfg: cfg}
}

func (s Layer) EvaluateApply(snapshot model.ClusterSnapshot, plan model.RebalancePlan, sim *model.SimulationResult, now time.Time) PreflightDecision {
	if sim == nil {
		return PreflightDecision{Allowed: false, Code: ReasonDryRunMissing, Message: "apply blocked: missing dry-run result"}
	}
	if !sim.Succeeded {
		return PreflightDecision{Allowed: false, Code: ReasonDryRunFailed, Message: "apply blocked: dry-run has conflicts"}
	}
	if sim.SnapshotID != plan.SnapshotID || sim.SnapshotID != snapshot.ID {
		return PreflightDecision{Allowed: false, Code: ReasonSnapshotMismatch, Message: "apply blocked: snapshot mismatch between apply request and dry-run"}
	}
	if strings.EqualFold(snapshot.Health.Status, "red") {
		return PreflightDecision{Allowed: false, Code: ReasonHealthRed, Message: "apply blocked: cluster health is red"}
	}
	if strings.EqualFold(snapshot.Health.Status, "yellow") && !s.cfg.Policy.AllowYellow {
		return PreflightDecision{Allowed: false, Code: ReasonHealthYellowNotAllowed, Message: "apply blocked: cluster health is yellow and policy disallows apply"}
	}
	if snapshot.ActiveOperations > s.cfg.Cluster.MaxActiveRecover {
		return PreflightDecision{Allowed: false, Code: ReasonActiveOpsExceeded, Message: fmt.Sprintf("apply blocked: active operations %d exceed limit %d", snapshot.ActiveOperations, s.cfg.Cluster.MaxActiveRecover)}
	}
	if s.cfg.Policy.EnforceExecutionWindow {
		h := now.UTC().Hour()
		if h < s.cfg.Policy.ExecutionWindowStartUTC || h >= s.cfg.Policy.ExecutionWindowEndUTC {
			return PreflightDecision{Allowed: false, Code: ReasonExecutionWindowClosed, Message: "apply blocked: current time is outside execution window"}
		}
	}
	return PreflightDecision{Allowed: true, Code: ReasonOK, Message: "preflight checks passed"}
}

func (s Layer) ValidateApply(snapshot model.ClusterSnapshot, plan model.RebalancePlan, sim *model.SimulationResult) error {
	dec := s.EvaluateApply(snapshot, plan, sim, time.Now().UTC())
	if !dec.Allowed {
		return fmt.Errorf("%s: %s", dec.Code, dec.Message)
	}
	return nil
}

func (s Layer) ShouldStopDetailed(snapshot model.ClusterSnapshot, baselineHealth string) StopDecision {
	if s.cfg.Policy.StopOnHealthDegrade {
		if healthSeverity(snapshot.Health.Status) > healthSeverity(baselineHealth) {
			return StopDecision{Stop: true, Code: ReasonHealthDegraded, Message: fmt.Sprintf("health degraded from %s to %s", baselineHealth, snapshot.Health.Status)}
		}
	}
	if s.cfg.Policy.StopOnWatermarkBreach {
		for _, n := range snapshot.Nodes {
			if n.DiskUsedPercent() >= s.cfg.Policy.CriticalDiskPercent {
				return StopDecision{Stop: true, Code: ReasonCriticalDiskExceeded, Message: fmt.Sprintf("node %s crossed critical disk threshold %.2f%%", n.ID, s.cfg.Policy.CriticalDiskPercent)}
			}
		}
	}
	if snapshot.ActiveOperations > s.cfg.Cluster.MaxActiveRecover {
		return StopDecision{Stop: true, Code: ReasonActiveOpsExceeded, Message: fmt.Sprintf("active operations %d exceed limit %d", snapshot.ActiveOperations, s.cfg.Cluster.MaxActiveRecover)}
	}
	return StopDecision{Stop: false, Code: ReasonOK, Message: "cluster state is acceptable"}
}

func (s Layer) ShouldStop(snapshot model.ClusterSnapshot, baselineHealth string) (bool, string) {
	d := s.ShouldStopDetailed(snapshot, baselineHealth)
	return d.Stop, d.Message
}

func healthSeverity(status string) int {
	switch strings.ToLower(status) {
	case "red":
		return 3
	case "yellow":
		return 2
	default:
		return 1
	}
}
