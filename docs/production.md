# Production pilot guidance

Status remains alpha. Start with annotation-only pilots. The optional
[adaptive experiment](adaptive-sampling.md) uses a separate tail-sampling pipeline
and requires additional routing and buffering checks. Scores describe operation metadata, not whole traces or current
service health. Validate usefulness against engineer-labeled operations before
using annotations to guide retention decisions.

## Deployment

Use `examples/otelcol/config-production.yaml` as a starting point. Set
`JEV_API_KEY`, `OTLP_BACKEND_ENDPOINT` (gRPC host:port), and
`OTLP_BACKEND_AUTHORIZATION` (the backend's authorization header value).
The backend connection verifies TLS. The receivers and internal metrics bind to
loopback for a local application or agent. For a gateway deployment, configure
receiver TLS/authentication and network access controls before exposing listeners.
Never expose an unauthenticated public OTLP receiver.

The exporter uses bounded in-memory queuing and finite retries. It does not provide
durable delivery across restarts or prolonged backend outages. For persistence,
build a distribution containing the file-storage extension and configure exporter
queue storage. Load test backend outage behavior for your delivery requirements.

Deploy jevtraces at one tier only: a downstream jevtraces instance removes the
upstream instance's annotations. Keep normalized names and attribute allow-lists;
redact sensitive values before this processor. Prefer a pinned model version when
available; `jev-latest` can change and existing cached scores expire independently.

## Replicas and budgets

No Redis or trace-ID affinity is needed for annotation-only processing. Every
processor instance has independent cache, queue, cooldown, and rate state. A new
replica starts cold. Scores can differ across replicas; first-seen spans remain
pending and are never retroactively updated. Shared API credentials do not imply
shared cache or rate limits.

`min_inference_interval: 200ms` spaces admission of inference requests by at least
200ms across workers in one instance (5 admissions/second, with one immediate
initial admission). Waiting is cancellable and does not block trace delivery.
The pending queue plus worker count bounds outstanding jobs; a full queue rejects
new assessments while forwarding spans. `jevtraces.inference.rate_limited` counts
jobs that waited for this gate, not dropped spans. Actual network sends may be
delayed by scheduling; this is a local admission limit, not a provider-side quota.

For N replicas, allow approximately N times the per-instance rate, plus cold-start
admissions. Include rolling-update surge replicas and multiple processor instances
in capacity planning. Restarts reset rate state. A hard global budget needs external
coordination or an inference gateway. Autoscaling can increase duplicate inference
and cost. Worker count separately bounds concurrent requests.

Monitor processed/annotated/protected/skipped spans, cache misses, queue rejections,
rate-limited jobs, inference failures/latency, Collector memory, and exporter queue
and failure telemetry. Pending does not guarantee an assessment is queued.

## Checks before broader rollout

- Run `make check`, `make collector`, `make collector-validate`, `make smoke`, and `make experiment`.
- Validate the pilot config with test environment values using the built binary.
- Run `go test -run '^$' -bench BenchmarkConsumeTraces -benchmem ./...`.
  This measures 100-span batches with repeated descriptors, warm/cold caches,
  no inference network I/O, and a no-op downstream. It is not a throughput SLA.
- Exercise representative attribute sizes and high-cardinality operations under
  sustained load; measure peak memory, CPU, latency, cache coverage, and cost.
- Exercise multiple deployed replicas, rolling restarts, provider 429/5xx/timeouts,
  and backend outages. Unit tests cover replica isolation and cold restarts but
  are not a deployment soak test.
- Evaluate score quality on labeled operations. Mock tests do not validate Jev's
  accuracy or probability calibration. No production-readiness claim follows
  solely from passing tests.

Metrics in the same Collector remain a separate pipeline. Correlated metric
windows would require additional state, freshness rules, routing, and assessment
semantics; they are not part of this hardening change.
