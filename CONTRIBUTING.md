# Contributing

Use Go 1.26.0+, Bash, Make, and Python 3. Run `make check` for formatting,
vet, and race tests. Run `make collector`, `make collector-validate`, and
`make smoke` after Collector changes. Smoke tests use local synthetic OTLP and
mock inference; no API credentials are needed.

Keep Collector API versions and the Builder manifest compatible. Do not commit
private telemetry or credentials. Describe the behavior changed and how it was
verified; use representative regression tests for inference, caching, preservation,
and lifecycle changes. Published model-accuracy claims require separate evaluation.

Contributions are licensed under Apache-2.0; see LICENSE.
