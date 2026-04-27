# API

## Endpoints

- `POST /v1/plan`
- `POST /v1/simulate`
- `POST /v1/apply`
- `POST /v1/stop`
- `GET /v1/executions/{id}`
- `GET /metrics`
- `GET /healthz`

## Request/Response Examples

### POST /v1/plan
Request body: empty.

Response (200):
```json
{ "snapshot": { "id": "..." }, "analysis": {}, "plan": {} }
```

### POST /v1/simulate
Request:
```json
{ "plan_bundle": { "snapshot": {}, "analysis": {}, "plan": {} } }
```
Response (200): updated `plan_bundle` with `simulation`.

### POST /v1/apply
Request:
```json
{
  "plan_bundle": { "snapshot": {}, "analysis": {}, "plan": {}, "simulation": {} },
  "idempotency_key": "optional-key",
  "correlation_id": "optional-correlation"
}
```
Response (202): persisted execution object.

### GET /v1/executions/{id}
Response (200): execution with status and audit trail.

### POST /v1/stop
Request:
```json
{ "execution_id": "exec-..." }
```
Response (200): `{ "ok": true }`.

## Error Model

All API errors return:
```json
{ "error": "reason_code", "message": "details" }
```

Common reason codes:
- `invalid_json`
- `execution_id_required`
- `execution_not_found`
- `plan_failed`
- `simulate_failed`
- `apply_start_failed`
