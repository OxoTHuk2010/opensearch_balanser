package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Cluster       ClusterConfig       `yaml:"cluster"`
	Planner       PlannerConfig       `yaml:"planner"`
	Limits        LimitsConfig        `yaml:"limits"`
	Policy        PolicyConfig        `yaml:"policy"`
	Runtime       RuntimeConfig       `yaml:"runtime"`
	API           APIConfig           `yaml:"api"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type ClusterConfig struct {
	Backend          string        `yaml:"backend"`
	Endpoint         string        `yaml:"endpoint"`
	Username         string        `yaml:"username"`
	PasswordEnv      string        `yaml:"password_env"`
	TLSEnabled       bool          `yaml:"tls_enabled"`
	SkipTLSVerify    bool          `yaml:"skip_tls_verify"`
	CAFile           string        `yaml:"ca_file"`
	RequestTimeout   time.Duration `yaml:"request_timeout"`
	MaxActiveRecover int           `yaml:"max_active_recoveries"`
}

type PlannerConfig struct {
	MaxMovesPerPlan                    int     `yaml:"max_moves_per_plan"`
	WeightDisk                         float64 `yaml:"weight_disk"`
	WeightShards                       float64 `yaml:"weight_shards"`
	WeightRisk                         float64 `yaml:"weight_risk"`
	WeightCost                         float64 `yaml:"weight_cost"`
	SevereShardImbalanceThreshold      int     `yaml:"severe_shard_imbalance_threshold"`
	LargeShardSizeGB                   float64 `yaml:"large_shard_size_gb"`
	LargeShardPenaltyMultiplier        float64 `yaml:"large_shard_penalty_multiplier"`
	NodeBalanceWeightDisk              float64 `yaml:"node_balance_weight_disk"`
	NodeBalanceWeightShards            float64 `yaml:"node_balance_weight_shards"`
	MoveScoreWeightDiskGap             float64 `yaml:"move_score_weight_disk_gap"`
	MoveScoreWeightShardGap            float64 `yaml:"move_score_weight_shard_gap"`
	MoveScoreWeightSize                float64 `yaml:"move_score_weight_size"`
	MoveScorePrimaryPenalty            float64 `yaml:"move_score_primary_penalty"`
	MoveScoreSevereLargeShardExtraMult float64 `yaml:"move_score_severe_large_shard_extra_mult"`
}

type LimitsConfig struct {
	MaxConcurrentMoves int     `yaml:"max_concurrent_moves"`
	MaxDataGBPerBatch  float64 `yaml:"max_data_gb_per_batch"`
	CooldownSeconds    int     `yaml:"cooldown_seconds"`
	MaxChurnPerRun     int     `yaml:"max_churn_per_run"`
}

type PolicyConfig struct {
	AllowYellow             bool    `yaml:"allow_yellow"`
	CriticalDiskPercent     float64 `yaml:"critical_disk_percent"`
	StopOnHealthDegrade     bool    `yaml:"stop_on_health_degrade"`
	StopOnWatermarkBreach   bool    `yaml:"stop_on_watermark_breach"`
	EnforceExecutionWindow  bool    `yaml:"enforce_execution_window"`
	ExecutionWindowStartUTC int     `yaml:"execution_window_start_utc"`
	ExecutionWindowEndUTC   int     `yaml:"execution_window_end_utc"`
}

type RuntimeConfig struct {
	RequireManualApproval bool   `yaml:"require_manual_approval"`
	CheckAllocator        bool   `yaml:"check_allocator"`
	DataDir               string `yaml:"data_dir"`
}

type APIConfig struct {
	Enabled      bool          `yaml:"enabled"`
	Listen       string        `yaml:"listen"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type ObservabilityConfig struct {
	AuditSinkPath string `yaml:"audit_sink_path"`
	LogLevel      string `yaml:"log_level"`
}

func Default() Config {
	return Config{
		Cluster: ClusterConfig{
			Backend:          "opensearch",
			RequestTimeout:   15 * time.Second,
			MaxActiveRecover: 20,
			TLSEnabled:       true,
		},
		Planner: PlannerConfig{
			MaxMovesPerPlan:                    50,
			WeightDisk:                         0.45,
			WeightShards:                       0.35,
			WeightRisk:                         0.15,
			WeightCost:                         0.05,
			SevereShardImbalanceThreshold:      50,
			LargeShardSizeGB:                   20,
			LargeShardPenaltyMultiplier:        3,
			NodeBalanceWeightDisk:              0.6,
			NodeBalanceWeightShards:            0.4,
			MoveScoreWeightDiskGap:             1.0,
			MoveScoreWeightShardGap:            2.0,
			MoveScoreWeightSize:                0.5,
			MoveScorePrimaryPenalty:            1000,
			MoveScoreSevereLargeShardExtraMult: 4.0,
		},
		Limits: LimitsConfig{
			MaxConcurrentMoves: 2,
			MaxDataGBPerBatch:  100,
			CooldownSeconds:    10,
			MaxChurnPerRun:     200,
		},
		Policy: PolicyConfig{
			AllowYellow:             false,
			CriticalDiskPercent:     92,
			StopOnHealthDegrade:     true,
			StopOnWatermarkBreach:   true,
			EnforceExecutionWindow:  false,
			ExecutionWindowStartUTC: 0,
			ExecutionWindowEndUTC:   24,
		},
		Runtime: RuntimeConfig{
			RequireManualApproval: true,
			CheckAllocator:        true,
			DataDir:               "./data",
		},
		API: APIConfig{
			Enabled:      false,
			Listen:       ":8080",
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 60 * time.Second,
		},
		Observability: ObservabilityConfig{
			AuditSinkPath: "./data/audit.log",
			LogLevel:      "info",
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Cluster.RequestTimeout <= 0 {
		cfg.Cluster.RequestTimeout = 15 * time.Second
	}
	if cfg.Cluster.Endpoint == "" {
		return Config{}, fmt.Errorf("cluster.endpoint is required")
	}
	if cfg.Planner.MaxMovesPerPlan <= 0 {
		cfg.Planner.MaxMovesPerPlan = 50
	}
	if cfg.Planner.SevereShardImbalanceThreshold <= 0 {
		cfg.Planner.SevereShardImbalanceThreshold = 50
	}
	if cfg.Planner.LargeShardSizeGB <= 0 {
		cfg.Planner.LargeShardSizeGB = 20
	}
	if cfg.Planner.LargeShardPenaltyMultiplier <= 0 {
		cfg.Planner.LargeShardPenaltyMultiplier = 3
	}
	if cfg.Planner.NodeBalanceWeightDisk < 0 {
		cfg.Planner.NodeBalanceWeightDisk = 0
	}
	if cfg.Planner.NodeBalanceWeightShards < 0 {
		cfg.Planner.NodeBalanceWeightShards = 0
	}
	if cfg.Planner.NodeBalanceWeightDisk == 0 && cfg.Planner.NodeBalanceWeightShards == 0 {
		cfg.Planner.NodeBalanceWeightDisk = 0.6
		cfg.Planner.NodeBalanceWeightShards = 0.4
	}
	if cfg.Planner.MoveScoreWeightDiskGap < 0 {
		cfg.Planner.MoveScoreWeightDiskGap = 1.0
	}
	if cfg.Planner.MoveScoreWeightShardGap < 0 {
		cfg.Planner.MoveScoreWeightShardGap = 2.0
	}
	if cfg.Planner.MoveScoreWeightSize < 0 {
		cfg.Planner.MoveScoreWeightSize = 0.5
	}
	if cfg.Planner.MoveScorePrimaryPenalty <= 0 {
		cfg.Planner.MoveScorePrimaryPenalty = 1000
	}
	if cfg.Planner.MoveScoreSevereLargeShardExtraMult <= 0 {
		cfg.Planner.MoveScoreSevereLargeShardExtraMult = 4.0
	}
	if cfg.Limits.MaxConcurrentMoves <= 0 {
		cfg.Limits.MaxConcurrentMoves = 1
	}
	if cfg.Limits.MaxDataGBPerBatch <= 0 {
		cfg.Limits.MaxDataGBPerBatch = 20
	}
	if cfg.Limits.MaxChurnPerRun <= 0 {
		cfg.Limits.MaxChurnPerRun = cfg.Planner.MaxMovesPerPlan
	}
	if cfg.Policy.ExecutionWindowStartUTC < 0 || cfg.Policy.ExecutionWindowStartUTC > 23 {
		return Config{}, fmt.Errorf("policy.execution_window_start_utc must be in 0..23")
	}
	if cfg.Policy.ExecutionWindowEndUTC <= 0 || cfg.Policy.ExecutionWindowEndUTC > 24 {
		return Config{}, fmt.Errorf("policy.execution_window_end_utc must be in 1..24")
	}
	if cfg.API.ReadTimeout <= 0 {
		cfg.API.ReadTimeout = 15 * time.Second
	}
	if cfg.API.WriteTimeout <= 0 {
		cfg.API.WriteTimeout = 60 * time.Second
	}
	if cfg.API.Listen == "" {
		cfg.API.Listen = ":8080"
	}
	if cfg.Runtime.DataDir == "" {
		cfg.Runtime.DataDir = "./data"
	}
	if cfg.Observability.AuditSinkPath == "" {
		cfg.Observability.AuditSinkPath = "./data/audit.log"
	}
	if cfg.Observability.LogLevel == "" {
		cfg.Observability.LogLevel = "info"
	}
	return cfg, nil
}

func ResolvePassword(envName string) string {
	if envName == "" {
		return ""
	}
	return os.Getenv(envName)
}
