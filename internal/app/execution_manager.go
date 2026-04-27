package app

import (
	"context"
	"fmt"
	"time"

	"opensearch-balanser/internal/executor"
	"opensearch-balanser/internal/model"
)

func (s Service) StartExecution(ctx context.Context, bundle model.PlanBundle, idempotencyKey, correlationID string) (model.Execution, error) {
	if idempotencyKey != "" {
		if existing, ok, err := s.store.FindByIdempotency(idempotencyKey); err == nil && ok {
			s.rt.Metrics.Inc("executor_idempotency_hit_total", nil)
			return existing, nil
		} else if err != nil {
			return model.Execution{}, err
		}
	}

	if correlationID == "" {
		correlationID = NewID("corr")
	}
	exec := model.Execution{
		ID:             NewID("exec"),
		CorrelationID:  correlationID,
		IdempotencyKey: idempotencyKey,
		Mode:           "apply-controlled",
		Status:         model.ExecutionPending,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
		SnapshotID:     bundle.Plan.SnapshotID,
		Plan:           bundle.Plan,
		Simulation:     bundle.Simulation,
	}
	if err := s.store.Create(exec); err != nil {
		return model.Execution{}, err
	}
	s.rt.Metrics.Inc("executor_started_total", nil)
	s.rt.Logger.Info("execution_created", map[string]any{"execution_id": exec.ID, "correlation_id": correlationID})

	go s.runExecution(context.Background(), exec.ID, bundle)
	return exec, nil
}

func (s Service) runExecution(ctx context.Context, executionID string, bundle model.PlanBundle) {
	exec, ok, err := s.store.Get(executionID)
	if err != nil || !ok {
		return
	}
	exec.Status = model.ExecutionRunning
	exec.UpdatedAt = time.Now().UTC()
	_ = s.store.Update(exec)

	fresh, err := s.adapter.CollectSnapshot(ctx)
	if err != nil {
		s.failExecution(executionID, "snapshot_collect_failed", err.Error())
		return
	}
	preflight := s.safety.EvaluateApply(fresh, bundle.Plan, bundle.Simulation, time.Now().UTC())
	if !preflight.Allowed {
		s.failExecution(executionID, string(preflight.Code), preflight.Message)
		return
	}

	confirm := func(batch int, total int, steps []model.PlanStep) (bool, error) {
		if !s.cfg.Runtime.RequireManualApproval {
			return true, nil
		}
		exec, ok, err := s.store.Get(executionID)
		if err != nil || !ok {
			return false, fmt.Errorf("execution not available")
		}
		if exec.StopRequested {
			return false, nil
		}
		// API mode defaults to automatic approve unless stop requested.
		return true, nil
	}

	hooks := executor.RunOptions{
		Confirm: confirm,
		StopRequested: func() bool {
			exec, ok, err := s.store.Get(executionID)
			if err != nil || !ok {
				return true
			}
			return exec.StopRequested
		},
		OnEvent: func(ev executor.Event) {
			exec, ok, err := s.store.Get(executionID)
			if err != nil || !ok {
				return
			}
			exec.CurrentBatch = ev.Batch
			exec.TotalBatches = ev.Total
			if ev.Type == "step_done" {
				exec.AppliedSteps++
			}
			if ev.Type == "stopped" {
				exec.StopReasonCode = ev.ReasonCode
				exec.StopReason = ev.Message
			}
			if ev.Step != nil {
				exec.AuditTrail = append(exec.AuditTrail, model.AuditRecord{
					At:          time.Now().UTC(),
					ExecutionID: exec.ID,
					Batch:       ev.Batch,
					Action:      ev.Type,
					StepRef:     fmt.Sprintf("%s/%d:%s->%s", ev.Step.Index, ev.Step.ShardID, ev.Step.FromNode, ev.Step.ToNode),
					Result:      ev.Type,
					Reason:      ev.Message,
				})
			} else {
				exec.AuditTrail = append(exec.AuditTrail, model.AuditRecord{
					At:          time.Now().UTC(),
					ExecutionID: exec.ID,
					Batch:       ev.Batch,
					Action:      ev.Type,
					Result:      ev.Type,
					Reason:      ev.Message,
				})
			}
			exec.UpdatedAt = time.Now().UTC()
			_ = s.store.Update(exec)
		},
	}

	res, err := executor.RunWithOptions(ctx, s.cfg, s.adapter, s.safety, fresh, bundle.Plan, hooks)
	if err != nil {
		s.failExecution(executionID, "executor_run_error", err.Error())
		return
	}

	exec, ok, err = s.store.Get(executionID)
	if err != nil || !ok {
		return
	}
	exec.Errors = append(exec.Errors, res.Errors...)
	exec.AppliedSteps = res.AppliedSteps
	exec.CurrentBatch = res.CompletedBatches
	exec.StopReason = res.StopReason
	exec.StopReasonCode = res.StopReasonCode
	if res.Stopped {
		exec.Status = model.ExecutionStopped
		s.rt.Metrics.Inc("executor_stopped_total", map[string]string{"reason_code": res.StopReasonCode})
	} else {
		exec.Status = model.ExecutionCompleted
		s.rt.Metrics.Inc("executor_completed_total", nil)
	}
	exec.UpdatedAt = time.Now().UTC()
	_ = s.store.Update(exec)
	s.rt.Logger.Info("execution_finished", map[string]any{"execution_id": exec.ID, "status": exec.Status, "reason_code": exec.StopReasonCode, "correlation_id": exec.CorrelationID})
}

func (s Service) failExecution(executionID, code, message string) {
	exec, ok, err := s.store.Get(executionID)
	if err != nil || !ok {
		return
	}
	exec.Status = model.ExecutionFailed
	exec.StopReasonCode = code
	exec.StopReason = message
	exec.Errors = append(exec.Errors, message)
	exec.UpdatedAt = time.Now().UTC()
	_ = s.store.Update(exec)
	s.rt.Metrics.Inc("executor_failed_total", map[string]string{"reason_code": code})
	s.rt.Logger.Error("execution_failed", map[string]any{"execution_id": executionID, "reason_code": code, "error": message, "correlation_id": exec.CorrelationID})
}

func (s Service) GetExecution(id string) (model.Execution, bool, error) {
	return s.store.Get(id)
}

func (s Service) RequestStop(id string) error {
	if err := s.store.RequestStop(id); err != nil {
		return err
	}
	s.rt.Metrics.Inc("executor_stop_requested_total", nil)
	return nil
}
