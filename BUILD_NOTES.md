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
Generated build output is ignored by Git. These notes record local checks;
they do not assert the status of a release or hosted CI run.

Tests validate software behavior using synthetic telemetry and mock inference.
They do not establish live Jev accuracy or safe trace-sampling thresholds.

Prometheus endpoint verification: the example now exposes internal telemetry on
port 8888. The smoke test scrapes a temporary equivalent endpoint and verifies
processed, annotated, protected, and inference-request counters alongside OTLP
metric passthrough. Configuration validation and the extended smoke test passed.

## Adaptive sampling experiment — 2026-09-22

- Added tail sampling and OTLP/HTTP export to the OCB distribution (v0.161.0).
- Collector rebuild, all three example configuration validations, and the existing
  smoke test passed.
- `make experiment` passed: 20/200 low-scored routine traces retained, both spans
  present per retained trace; 10/10 subsequent scored traces retained after refresh.
- Protection, pending/skipped fallback, provider outage, split traces, cached late
  drops, and archive isolation passed. The experiment uses mock inference.

The full-input and sampled outputs in the example are debug exporters. The 90%
synthetic routine-volume reduction is not a measured production saving. Clustered
sampling requires trace-ID routing; see `docs/adaptive-sampling.md`.
