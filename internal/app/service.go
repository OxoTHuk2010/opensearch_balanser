package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"opensearch-balanser/internal/analyzer"
	"opensearch-balanser/internal/collector"
	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/executor"
	"opensearch-balanser/internal/model"
	"opensearch-balanser/internal/planner"
	"opensearch-balanser/internal/safety"
	"opensearch-balanser/internal/simulator"
)

type Service struct {
	cfg      config.Config
	adapter  collector.Adapter
	safety   safety.Layer
	planner  planner.Planner
	analyzer analyzer.Options
	store    ExecutionStore
	rt       Runtime
}

func NewService(cfg config.Config, adapter collector.Adapter, store ExecutionStore, rt Runtime) Service {
	return Service{
		cfg:      cfg,
		adapter:  adapter,
		safety:   safety.New(cfg),
		planner:  planner.New(cfg),
		analyzer: analyzer.Options{CriticalDiskPercent: cfg.Policy.CriticalDiskPercent},
		store:    store,
		rt:       rt,
	}
}

func (s Service) Audit(ctx context.Context) (model.PlanBundle, error) {
	start := time.Now()
	snap, err := s.adapter.CollectSnapshot(ctx)
	if err != nil {
		s.rt.Metrics.Inc("collector_snapshot_errors_total", nil)
		return model.PlanBundle{}, err
	}
	analysis := analyzer.Analyze(snap, s.analyzer)
	s.rt.Metrics.Inc("audit_runs_total", nil)
	s.rt.Metrics.SetGauge("audit_last_duration_seconds", time.Since(start).Seconds(), nil)
	return model.PlanBundle{Snapshot: snap, Analysis: analysis}, nil
}

func (s Service) Plan(ctx context.Context) (model.PlanBundle, error) {
	bundle, err := s.Audit(ctx)
	if err != nil {
		return model.PlanBundle{}, err
	}
	pl, err := s.planner.Build(bundle.Snapshot, bundle.Analysis)
	if err != nil {
		s.rt.Metrics.Inc("planner_errors_total", nil)
		return model.PlanBundle{}, err
	}
	bundle.Plan = pl
	s.rt.Metrics.Inc("planner_runs_total", nil)
	s.rt.Metrics.SetGauge("planner_last_steps", float64(len(pl.Steps)), nil)
	return bundle, nil
}

func (s Service) EmergencyPlan(ctx context.Context, nodeID string) (model.PlanBundle, error) {
	bundle, err := s.Audit(ctx)
	if err != nil {
		return model.PlanBundle{}, err
	}
	pl, err := s.planner.BuildEmergencyDrain(bundle.Snapshot, nodeID, bundle.Analysis)
	if err != nil {
		return model.PlanBundle{}, err
	}
	bundle.Plan = pl
	s.rt.Metrics.Inc("planner_emergency_runs_total", nil)
	return bundle, nil
}

func (s Service) DryRun(ctx context.Context, bundle model.PlanBundle) (model.PlanBundle, error) {
	fresh, err := s.adapter.CollectSnapshot(ctx)
	if err != nil {
		return model.PlanBundle{}, err
	}
	if fresh.ID != bundle.Plan.SnapshotID {
		return model.PlanBundle{}, fmt.Errorf("dry-run blocked: current snapshot %s differs from plan snapshot %s", fresh.ID, bundle.Plan.SnapshotID)
	}
	sim, err := simulator.Run(ctx, s.cfg, s.adapter, fresh, bundle.Plan)
	if err != nil {
		s.rt.Metrics.Inc("simulator_errors_total", nil)
		return model.PlanBundle{}, err
	}
	bundle.Snapshot = fresh
	bundle.Simulation = &sim
	if sim.Succeeded {
		s.rt.Metrics.Inc("simulator_runs_total", map[string]string{"result": "success"})
	} else {
		s.rt.Metrics.Inc("simulator_runs_total", map[string]string{"result": "conflict"})
	}
	return bundle, nil
}

func (s Service) Apply(ctx context.Context, bundle model.PlanBundle) (executor.Result, error) {
	fresh, err := s.adapter.CollectSnapshot(ctx)
	if err != nil {
		return executor.Result{}, err
	}
	preflight := s.safety.EvaluateApply(fresh, bundle.Plan, bundle.Simulation, time.Now().UTC())
	if !preflight.Allowed {
		return executor.Result{}, fmt.Errorf("%s: %s", preflight.Code, preflight.Message)
	}

	execID := NewID("exec")
	corrID := NewID("corr")
	exec := model.Execution{
		ID:            execID,
		CorrelationID: corrID,
		Mode:          "apply-cli",
		Status:        model.ExecutionRunning,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
		SnapshotID:    bundle.Plan.SnapshotID,
		Plan:          bundle.Plan,
		Simulation:    bundle.Simulation,
	}
	persistEnabled := true
	if err := s.store.Create(exec); err != nil {
		persistEnabled = false
		s.rt.Logger.Error("cli_execution_store_create_failed", map[string]any{"error": err.Error()})
	}
	s.rt.Logger.Info("cli_apply_started", map[string]any{
		"execution_id":     execID,
		"correlation_id":   corrID,
		"total_steps":      len(bundle.Plan.Steps),
		"plan_snapshot_id": bundle.Plan.SnapshotID,
	})

	confirm := func(batch int, total int, steps []model.PlanStep) (bool, error) {
		remainingAfterCurrent := total - batch
		fmt.Printf("\nBatch %d/%d: %d step(s)\n", batch, total, len(steps))
		for _, step := range steps {
			role := "replica"
			if step.Primary {
				role = "primary"
			}
			fmt.Printf("  - %s/%d (%s) %s -> %s, size=%.2fGB\n", step.Index, step.ShardID, role, step.FromNode, step.ToNode, step.EstimatedCost.NetworkGB)
		}
		fmt.Printf("After this batch: remaining batches = %d\n", remainingAfterCurrent)
		fmt.Printf("Approve batch %d/%d (%d step(s))? [y/N]: ", batch, total, len(steps))
		in := bufio.NewReader(os.Stdin)
		line, err := in.ReadString('\n')
		if err != nil {
			return false, err
		}
		v := strings.TrimSpace(strings.ToLower(line))
		return v == "y" || v == "yes", nil
	}
	if !s.cfg.Runtime.RequireManualApproval {
		confirm = nil
	}
	hooks := executor.RunOptions{
		Confirm: confirm,
		OnEvent: func(ev executor.Event) {
			fields := map[string]any{
				"execution_id": execID,
				"event":        ev.Type,
				"batch":        ev.Batch,
				"total":        ev.Total,
				"message":      ev.Message,
				"reason_code":  ev.ReasonCode,
			}
			if ev.Step != nil {
				fields["step_ref"] = fmt.Sprintf("%s/%d:%s->%s", ev.Step.Index, ev.Step.ShardID, ev.Step.FromNode, ev.Step.ToNode)
			}
			s.rt.Logger.Info("cli_apply_event", fields)
			if !persistEnabled {
				return
			}
			current, ok, err := s.store.Get(execID)
			if err != nil || !ok {
				return
			}
			current.CurrentBatch = ev.Batch
			current.TotalBatches = ev.Total
			if ev.Type == "step_done" {
				current.AppliedSteps++
			}
			if ev.Type == "stopped" {
				current.StopReasonCode = ev.ReasonCode
				current.StopReason = ev.Message
			}
			current.AuditTrail = append(current.AuditTrail, model.AuditRecord{
				At:          time.Now().UTC(),
				ExecutionID: current.ID,
				Batch:       ev.Batch,
				Action:      ev.Type,
				Result:      ev.Type,
				Reason:      ev.Message,
			})
			if ev.Step != nil {
				last := len(current.AuditTrail) - 1
				current.AuditTrail[last].StepRef = fmt.Sprintf("%s/%d:%s->%s", ev.Step.Index, ev.Step.ShardID, ev.Step.FromNode, ev.Step.ToNode)
			}
			_ = s.store.Update(current)
		},
	}
	res, err := executor.RunWithOptions(ctx, s.cfg, s.adapter, s.safety, fresh, bundle.Plan, hooks)
	if err != nil {
		if persistEnabled {
			exec.Status = model.ExecutionFailed
			exec.StopReasonCode = "executor_run_error"
			exec.StopReason = err.Error()
			exec.Errors = append(exec.Errors, err.Error())
			_ = s.store.Update(exec)
		}
		return executor.Result{}, err
	}
	res.ExecutionID = execID
	if res.Stopped {
		s.rt.Metrics.Inc("executor_runs_total", map[string]string{"result": "stopped"})
	} else {
		s.rt.Metrics.Inc("executor_runs_total", map[string]string{"result": "completed"})
	}
	if persistEnabled {
		current, ok, err := s.store.Get(execID)
		if err == nil && ok {
			current.AppliedSteps = res.AppliedSteps
			current.CurrentBatch = res.CompletedBatches
			current.TotalBatches = res.TotalBatches
			current.StopReason = res.StopReason
			current.StopReasonCode = res.StopReasonCode
			current.Errors = append(current.Errors, res.Errors...)
			if res.Stopped {
				current.Status = model.ExecutionStopped
			} else {
				current.Status = model.ExecutionCompleted
			}
			_ = s.store.Update(current)
		}
	}
	s.rt.Logger.Info("cli_apply_finished", map[string]any{
		"execution_id":      execID,
		"correlation_id":    corrID,
		"applied_steps":     res.AppliedSteps,
		"total_steps":       res.TotalSteps,
		"remaining_steps":   res.RemainingSteps,
		"completed_batches": res.CompletedBatches,
		"total_batches":     res.TotalBatches,
		"remaining_batches": res.RemainingBatches,
		"stopped":           res.Stopped,
		"stop_reason_code":  res.StopReasonCode,
	})
	return res, nil
}

func SaveBundle(path string, bundle model.PlanBundle) error {
	b, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func LoadBundle(path string) (model.PlanBundle, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return model.PlanBundle{}, err
	}
	var bundle model.PlanBundle
	if err := json.Unmarshal(b, &bundle); err != nil {
		return model.PlanBundle{}, err
	}
	return bundle, nil
}
