package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"opensearch-balanser/internal/collector"
	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/model"
	"opensearch-balanser/internal/safety"
)

type ConfirmBatchFunc func(batch int, total int, steps []model.PlanStep) (bool, error)

type Event struct {
	Type       string
	Batch      int
	Total      int
	Step       *model.PlanStep
	ReasonCode string
	Message    string
}

type RunOptions struct {
	Confirm       ConfirmBatchFunc
	StopRequested func() bool
	OnEvent       func(Event)
}

type Result struct {
	CompletedBatches int      `json:"completed_batches"`
	AppliedSteps     int      `json:"applied_steps"`
	Stopped          bool     `json:"stopped"`
	StopReason       string   `json:"stop_reason,omitempty"`
	StopReasonCode   string   `json:"stop_reason_code,omitempty"`
	Errors           []string `json:"errors,omitempty"`
}

func Run(ctx context.Context, cfg config.Config, adapter collector.Adapter, policy safety.Layer, baseline model.ClusterSnapshot, plan model.RebalancePlan, confirm ConfirmBatchFunc) (Result, error) {
	return RunWithOptions(ctx, cfg, adapter, policy, baseline, plan, RunOptions{Confirm: confirm})
}

func RunWithOptions(ctx context.Context, cfg config.Config, adapter collector.Adapter, policy safety.Layer, baseline model.ClusterSnapshot, plan model.RebalancePlan, opts RunOptions) (Result, error) {
	batches := buildBatches(plan.Steps, cfg.Limits.MaxConcurrentMoves, cfg.Limits.MaxDataGBPerBatch)
	result := Result{}

	for i, batch := range batches {
		if opts.StopRequested != nil && opts.StopRequested() {
			result.Stopped = true
			result.StopReason = "stop requested"
			result.StopReasonCode = "stop_requested"
			emit(opts, Event{Type: "stopped", Batch: i + 1, Total: len(batches), ReasonCode: result.StopReasonCode, Message: result.StopReason})
			return result, nil
		}

		emit(opts, Event{Type: "batch_start", Batch: i + 1, Total: len(batches)})

		if opts.Confirm != nil && cfg.Runtime.RequireManualApproval {
			ok, err := opts.Confirm(i+1, len(batches), batch)
			if err != nil {
				return result, fmt.Errorf("batch confirmation failed: %w", err)
			}
			if !ok {
				result.Stopped = true
				result.StopReason = "operator rejected batch"
				result.StopReasonCode = "operator_rejected"
				emit(opts, Event{Type: "stopped", Batch: i + 1, Total: len(batches), ReasonCode: result.StopReasonCode, Message: result.StopReason})
				return result, nil
			}
		}

		errList := runBatch(ctx, adapter, batch, cfg.Limits.MaxConcurrentMoves, opts)
		if len(errList) > 0 {
			result.Errors = append(result.Errors, errList...)
			result.Stopped = true
			result.StopReason = "execution errors"
			result.StopReasonCode = "execution_errors"
			emit(opts, Event{Type: "stopped", Batch: i + 1, Total: len(batches), ReasonCode: result.StopReasonCode, Message: result.StopReason})
			return result, nil
		}
		result.CompletedBatches++
		result.AppliedSteps += len(batch)
		emit(opts, Event{Type: "batch_done", Batch: i + 1, Total: len(batches)})

		snap, err := adapter.CollectSnapshot(ctx)
		if err != nil {
			result.Stopped = true
			result.StopReason = fmt.Sprintf("post-batch snapshot failed: %v", err)
			result.StopReasonCode = "post_batch_snapshot_failed"
			emit(opts, Event{Type: "stopped", Batch: i + 1, Total: len(batches), ReasonCode: result.StopReasonCode, Message: result.StopReason})
			return result, nil
		}
		stop := policy.ShouldStopDetailed(snap, baseline)
		if stop.Stop {
			result.Stopped = true
			result.StopReason = stop.Message
			result.StopReasonCode = string(stop.Code)
			emit(opts, Event{Type: "stopped", Batch: i + 1, Total: len(batches), ReasonCode: result.StopReasonCode, Message: result.StopReason})
			return result, nil
		}
		if i < len(batches)-1 && cfg.Limits.CooldownSeconds > 0 {
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case <-time.After(time.Duration(cfg.Limits.CooldownSeconds) * time.Second):
			}
		}
	}
	return result, nil
}

func buildBatches(steps []model.PlanStep, maxConcurrent int, maxDataGB float64) [][]model.PlanStep {
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	if maxDataGB <= 0 {
		maxDataGB = 1
	}
	batches := make([][]model.PlanStep, 0)
	cur := make([]model.PlanStep, 0, maxConcurrent)
	curData := 0.0
	for _, step := range steps {
		size := step.EstimatedCost.NetworkGB
		if len(cur) > 0 && (len(cur) >= maxConcurrent || curData+size > maxDataGB) {
			batches = append(batches, cur)
			cur = nil
			curData = 0
		}
		cur = append(cur, step)
		curData += size
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches
}

func runBatch(ctx context.Context, adapter collector.Adapter, steps []model.PlanStep, concurrency int, opts RunOptions) []string {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	errCh := make(chan string, len(steps))
	for _, step := range steps {
		step := step
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				errCh <- ctx.Err().Error()
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()
			if err := adapter.ExecuteMove(ctx, step); err != nil {
				errCh <- fmt.Sprintf("step %s/%d %s->%s failed: %v", step.Index, step.ShardID, step.FromNode, step.ToNode, err)
				emit(opts, Event{Type: "step_failed", Step: &step, Message: err.Error()})
				return
			}
			emit(opts, Event{Type: "step_done", Step: &step})
		}()
	}
	wg.Wait()
	close(errCh)
	errs := make([]string, 0)
	for err := range errCh {
		errs = append(errs, err)
	}
	return errs
}

func emit(opts RunOptions, event Event) {
	if opts.OnEvent != nil {
		opts.OnEvent(event)
	}
}
