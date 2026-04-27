# ADR 0001: Safety-first gates before testing and apply

## Context

The project is production-critical and requires safety > stability > functionality.
Documentation drift was identified as a recurring risk that can invalidate test expectations and operating procedures.

## Decision

Introduce a mandatory documentation gate that validates required docs and section headers before the test stage in CI.
Test stage is explicitly dependent on successful docs gate completion.

## Consequences

Positive:
- Operators and engineers get minimum guaranteed runbook/API/security coverage.
- Test results are interpreted against up-to-date docs.

Tradeoff:
- CI may fail on documentation changes until required sections are restored.
