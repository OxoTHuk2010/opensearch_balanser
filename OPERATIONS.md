# OPERATIONS

## Runbook

Safe execution path:
1. `plan`
2. `dry-run`
3. `apply`
4. monitor `/metrics` and execution status endpoint

For API mode:
1. `POST /v1/plan`
2. `POST /v1/simulate`
3. `POST /v1/apply`
4. poll `GET /v1/executions/{id}`

## Incident Response

If cluster health degrades or errors rise:
1. Request stop: `POST /v1/stop`.
2. Verify execution status becomes `stopped` or `failed`.
3. Inspect `data/executions.json` and audit log.
4. Re-run `audit` before any new plan.

## Rollback and Stop

- Manual stop is graceful and batch-aware.
- Each plan step includes `rollback_hint` in plan artifacts.
- Do not restart apply until a new dry-run succeeds on the current snapshot.

## Monitoring Checklist

- `executor_failed_total`
- `executor_stopped_total`
- `simulator_runs_total{result="conflict"}`
- HTTP latency/volume on API endpoints
