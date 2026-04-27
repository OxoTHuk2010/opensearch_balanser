package collector

import (
	"context"

	"opensearch-balanser/internal/model"
)

type Adapter interface {
	CollectSnapshot(ctx context.Context) (model.ClusterSnapshot, error)
	ValidateMove(ctx context.Context, step model.PlanStep) (model.AllocatorCheck, error)
	ExecuteMove(ctx context.Context, step model.PlanStep) error
}
