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
	confirm := func(batch int, total int, steps []model.PlanStep) (bool, error) {
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
	res, err := executor.Run(ctx, s.cfg, s.adapter, s.safety, fresh, bundle.Plan, confirm)
	if err != nil {
		return executor.Result{}, err
	}
	if res.Stopped {
		s.rt.Metrics.Inc("executor_runs_total", map[string]string{"result": "stopped"})
	} else {
		s.rt.Metrics.Inc("executor_runs_total", map[string]string{"result": "completed"})
	}
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
