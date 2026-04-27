package app

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/observability"
)

type Runtime struct {
	Logger  *observability.Logger
	Metrics *observability.Metrics
}

func NewRuntime(cfg config.Config) (Runtime, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.Observability.AuditSinkPath), 0o700); err != nil {
		return Runtime{}, fmt.Errorf("create observability dir: %w", err)
	}
	logger, err := observability.NewLogger(cfg.Observability.AuditSinkPath, cfg.Observability.LogLevel)
	if err != nil {
		return Runtime{}, fmt.Errorf("init logger: %w", err)
	}
	return Runtime{Logger: logger, Metrics: observability.NewMetrics()}, nil
}

func NewID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b))
}
