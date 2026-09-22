# Experimental inference-guided adaptive sampling

Run the deterministic experiment first:

```sh
make collector
make experiment
```

It uses a local mock Jev server, the compiled Collector's real tail sampler, and
local OTLP HTTP sinks. No API key or paid inference is needed. CI runs it too.
The script derives its policy from `examples/otelcol/config-adaptive.yaml` and only
changes endpoints, credentials, and timing for the test.

The first verified run retained 20/200 routine traces (40/400 spans). Every retained
routine trace contained both spans. After the mock score changed from 0.05 to 0.9
and the cache expired, 10/10 subsequent scored traces were retained. Cold operations,
errors, slow spans, protected operations, oversized/skipped metadata, uncached
operations during an outage, and a split trace containing an error were retained.
A late span on a rejected trace remained rejected using the decision cache. The
archive branch retained the input without Jev annotations.

These are synthetic pipeline results, not evidence of safe real-world reduction
or inference accuracy. The baseline uses hashed trace IDs, not a per-batch quota.

## Policy

`jevtraces` annotates spans, then `tail_sampling` buffers them by trace ID. Top-level
policies are ORed. Retain a trace if any received span has an error, protection,
missing/pending/skipped assessment, missing probability, or any of the three
probabilities >= 0.2. Also retain traces with overall duration >= 1 second.
Only traces where every received span has three valid scores below 0.2 are eligible
for reduction to a 10% trace-ID-based baseline sample.

The 0.2 threshold and 10% baseline are illustrative settings, not evaluated safety
thresholds. A Jev keep probability is an operation judgment, not a sampling rate.
The policy does not multiply these probabilities or interpret them as independent.
Adaptation comes from new/refreshed operation assessments changing which traces
qualify for full retention; this is not a feedback controller for volume or cost.

The first span of an uncached operation remains pending, even when its assessment
completes before the sampling timer. Inference does not modify already-buffered
spans. Fresh cached scores remain usable during provider outages; once expired,
unavailable assessments cause conservative retention. Model changes can shift
scores. Review operation labels and retention outcomes before changing thresholds.

## Live shadow pilot

With `JEV_API_KEY` exported:

```sh
./_build/otelcol-jevtraces --config examples/otelcol/config-adaptive.yaml
```

Send OTLP HTTP traces to localhost:4318. The example uses named debug exporters
for archive and sampled branches; these are local inspection outputs, not durable
storage. For a real pilot replace them with separately identifiable backend
exporters. Keep the full archive until important-trace retention has been measured.
Generate business metrics before sampling or from the archive: sampled traces
produce biased counts and cannot be used as an unadjusted traffic denominator.

For Docker Desktop clients, use a local copy with the receiver listening on
`0.0.0.0:4318` and send to `http://host.docker.internal:4318`. Restrict access to
trusted clients. If the application also exports metrics, add a metrics pipeline
using the OTLP receiver, memory limiter, batch processor, and archive exporter.
Files named `*.local.yaml` are ignored; do not commit private pilot configuration.
Use normalized operation names or allowlisted route templates so distinct business
operations do not collapse into one assessment.

The shipped receiver only binds loopback. Gateway deployments need appropriate transport
security and access controls. Existing annotation-only configurations remain valid.

## Cluster and trace completeness limits

All spans of each trace must reach the same sampling replica. Place trace-ID-aware
routing (for example OTel's load-balancing exporter) before this pipeline; an
ordinary random load balancer is insufficient. That router is not included in this
example distribution. Independent Jev caches still cause duplicated inference and
possibly different operation scores across replicas. Redis would not replace
trace-ID routing or the tail sampler's trace buffers.

`decision_wait: 10s` is a bounded collection window, not proof a trace is complete.
Choose it from real arrival delays. The sampler cannot recover spans dropped by an
upstream SDK. Errors arriving after a cached drop decision do not rescue the trace.
Decision cache eviction, full buffers, shutdown, and routing changes during rolling
updates can cause loss or incomplete traces. Size the trace buffer and decision
caches for measured traffic and late arrivals; monitor tail-sampling eviction and
late-span metrics. No guarantee of complete traces is made under these conditions.

Reference: upstream v0.161.0 [tail sampling processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.161.0/processor/tailsamplingprocessor).

Before enabling sampled-only delivery, run real shadow traffic, independently
review important operations/traces, test late spans and rolling updates, and
measure memory, retained-trace coverage, and backend outage behavior.
