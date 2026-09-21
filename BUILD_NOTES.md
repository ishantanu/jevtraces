# Build notes

Verified in the standalone project on 2026-09-21 with Go 1.26.5:

- `make check`: formatting, vet, and race tests passed (93.8% statement coverage).
- `go mod tidy -diff`: module files are tidy.
- `make collector`: built `_build/otelcol-jevtraces` using OCB v0.161.0.
- `make collector-validate`: the standalone trace configuration passed.
- `make smoke`: a running Collector received synthetic OTLP and exported pending,
  cached, and protected annotations. Low-scoring spans were retained. Matching
  operations reused one request to a local mock Jev endpoint.
- CI workflow passed actionlint v1.7.7; the build script passed `bash -n`.

The project has no dependency on the jevmetrics checkout. Its root Go module is
`github.com/ishantanu/jevtraces`; the processor package is `jevtracesprocessor`.
Generated build output is ignored by Git. No remote repository, release, or
hosted CI run has been created for this standalone project.

Tests validate software behavior using synthetic telemetry and mock inference.
They do not establish live Jev accuracy or safe trace-sampling thresholds.

Prometheus endpoint verification: the example now exposes internal telemetry on
port 8888. The smoke test scrapes a temporary equivalent endpoint and verifies
processed, annotated, protected, and inference-request counters alongside OTLP
metric passthrough. Configuration validation and the extended smoke test passed.
