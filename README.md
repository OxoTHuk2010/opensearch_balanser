# OpenSearch Balanser

Safety-first rebalancer for distributed OpenSearch clusters with CLI + HTTP API.

## What It Does

- Collects cluster state (nodes, shards, watermarks, health).
- Detects imbalance and risk conditions.
- Builds a rebalance plan with explainability.
- Runs dry-run simulation before apply.
- Applies plan in controlled batches with stop conditions.
- Exposes execution lifecycle via API (`pending/running/stopped/failed/completed`).

## Documentation Gate

- Local check: `go run ./cmd/docs-gate`
- CI check order: `docs-gate` must pass before `test` job.
- Mandatory docs covered by the gate: `README.md`, `API.md`, `OPERATIONS.md`, `SECURITY.md`, `CHANGELOG.md`, `docs/adr/0001-safety-first-gates.md`.

## Prerequisites

- Go `1.26+`.
- Reachable OpenSearch endpoint with TLS/access configured.
- `config.yaml` file (copy from `config.example.yaml`).

## Quick Start (CLI)

1. Copy config template:
   ```bash
   cp config.example.yaml config.yaml
   ```
2. Fill required values in `config.yaml`:
- `cluster.endpoint`
- `cluster.username`
- `cluster.password_env` (and export env variable)
3. Build binary:
   ```bash
   go build -o balancer ./cmd/balancer
   ```
4. Run safe workflow:
   ```bash
   ./balancer plan --config config.yaml --out plan-bundle.json
   ./balancer dry-run --config config.yaml --plan plan-bundle.json --out plan-bundle.json
   ./balancer apply --config config.yaml --plan plan-bundle.json
   ```

## CLI Commands

- `balancer audit --config config.yaml`
- `balancer plan --config config.yaml --out plan-bundle.json`
- `balancer dry-run --config config.yaml --plan plan-bundle.json --out plan-bundle.json`
- `balancer apply --config config.yaml --plan plan-bundle.json`
- `balancer force --config config.yaml --out plan-bundle.json`
- `balancer emergency-drain --config config.yaml --node data-01 --out emergency-plan-bundle.json`
- `balancer serve-api --config config.yaml`

`force` runs `plan -> dry-run -> apply` in one command without interactive batch confirmations, intended for cron/automation.

## API Enablement

- API starts only if `api.enabled: true` in `config.yaml`.
- Server listen address is `api.listen` (default `:8080`).

## HTTP API

- `POST /v1/plan`
- `POST /v1/simulate`
- `POST /v1/apply`
- `POST /v1/stop`
- `GET /v1/executions/{id}`
- `GET /metrics`
- `GET /healthz`

### API Request Payloads

- `POST /v1/plan`: empty body.
- `POST /v1/simulate`:
  ```json
  { "plan_bundle": { "...": "..." } }
  ```
- `POST /v1/apply`:
  ```json
  {
    "plan_bundle": { "...": "..." },
    "idempotency_key": "optional-dedup-key",
    "correlation_id": "optional-correlation-id"
  }
  ```
- `POST /v1/stop`:
  ```json
  { "execution_id": "exec-..." }
  ```

### API Workflow

1. `POST /v1/plan`
2. `POST /v1/simulate` with returned `plan_bundle`
3. `POST /v1/apply` with simulated `plan_bundle`
4. Poll `GET /v1/executions/{id}` until terminal status
5. `POST /v1/stop` to request graceful stop

## Safety Model

- Apply blocked without successful dry-run on the same snapshot.
- Apply blocked on policy-violating cluster health.
- Apply blocked when active operations exceed configured limit.
- Optional UTC execution window enforcement.
- Batch limits + cooldown + runtime stop checks.
- CLI apply keeps manual approval by default.
- API apply is persisted and can be stopped via `POST /v1/stop`.

## Execution and Audit Artifacts

- `plan-bundle.json`: snapshot + analysis + plan + dry-run output.
- `data/executions.json`: persisted execution state and audit trail.
- `observability.audit_sink_path`: structured runtime logs.

## Config Notes

- `cluster.endpoint` is required.
- `runtime.data_dir` stores execution persistence files.
- `policy.execution_window_start_utc` must be `0..23`.
- `policy.execution_window_end_utc` must be `1..24`.
- `api.enabled` controls API startup guard.
- `planner.large_shard_size_gb` and `planner.large_shard_penalty_multiplier` tune large-shard move avoidance.
- `planner.severe_shard_imbalance_threshold` and `planner.move_score_weight_shard_gap` tune shard-count balancing pressure.
- `planner.min_move_shard_size_gb` skips negligible shard moves that do not materially improve balance.
- `planner.target_free_gb_per_node` sets desired free disk per node (for example `300` GB).
- `planner.node_balance_weight_pressure`, `planner.pressure_min_shard_size_gb`, and `planner.move_score_pressure_size_reward` tune aggressive drain behavior for overloaded nodes.
- Set `policy.allow_yellow: true` to permit apply in yellow health; red remains blocked.

## Observability

- `GET /metrics` exposes Prometheus-style counters/gauges.
- API requests include `X-Correlation-ID` (generated if absent).
- Execution lifecycle and failures are logged in structured JSON.

## Least-Privilege Guidance

- Use read-only credentials for `audit/plan/dry-run` where possible.
- Use narrowly scoped write credentials only for controlled apply.
- Do not store plaintext secrets in config; use `password_env`.

## Additional Documents

- [API](API.md)
- [Operations Runbook](OPERATIONS.md)
- [Security](SECURITY.md)
- [Changelog](CHANGELOG.md)
- [ADR 0001](docs/adr/0001-safety-first-gates.md)
