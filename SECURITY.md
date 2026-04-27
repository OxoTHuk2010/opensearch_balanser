# SECURITY

## Access Model

- Read-only credentials for `audit/plan/dry-run`.
- Separate limited write credentials for controlled apply only.
- Enforce least privilege at OpenSearch API scope.

## Secrets

- Do not store plaintext passwords in config.
- Use `cluster.password_env` and environment-based secret injection.
- Restrict filesystem permissions for `config.yaml` and runtime data.

## TLS and Transport Security

- Enable TLS for cluster communication.
- Pin CA via `cluster.ca_file`.
- Keep `skip_tls_verify=false` in production.

## Auditability

- Preserve `data/executions.json` as execution evidence.
- Keep structured audit logs (`observability.audit_sink_path`).
- Retain correlation IDs for cross-system tracing.
