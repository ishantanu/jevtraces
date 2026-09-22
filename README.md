# jevtraces

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/jevtraces-flat-dark.png" />
  <img src="docs/assets/jevtraces.png" alt="jevtraces logo" width="420" />
</picture>

**Jev inference over operation metadata, delivered as OpenTelemetry span annotations.**

`jevtraces` is an alpha OTel trace processor. It asks Jev whether representative
traces containing an operation are likely useful for diagnosis, business-critical,
and worth retaining. It caches those judgments and annotates later matching spans.
**The jevtraces processor retains every span, regardless of the returned probabilities.**

An opt-in [adaptive sampling experiment](docs/adaptive-sampling.md) now combines
these annotations with OTel tail sampling in a separate configuration. It retains
a full archive branch and samples the comparison branch.

The jevtraces processor assesses operation metadata, not a complete trace or a specific
request's outcome. The processor does not buffer traces, change sampling flags, rewrite IDs,
sample spans, or perform whole-trace retention. A trace can contain spans with
different assessments. Do not interpret a low operation score as permission to
discard that span or its trace.

## Run it

From the repository root, with Go 1.26.0+ and Collector Builder v0.161.0:

```bash
go install go.opentelemetry.io/collector/cmd/builder@v0.161.0
OCB_BIN="$(go env GOPATH)/bin/builder" make collector
export JEV_API_KEY='your-key'
./_build/otelcol-jevtraces --config examples/otelcol/config.yaml
```

The example accepts OTLP traces and metrics on ports 4317/4318. It exports
annotated traces and unchanged metrics to the debug exporter through separate
pipelines. Metrics do not trigger Jev inference. This standalone distribution includes the `jevtraces` processor, OTLP receiver,
batch, memory-limiter, and tail-sampling processors, and debug/OTLP/OTLP-HTTP exporters. It has no dependency
on the jevmetrics project.

To validate the full pipeline without external credentials or inference:

```bash
python3 scripts/smoke-traces.py
```

The smoke test starts a temporary local Jev stub and Collector, sends OTLP traces,
checks cached annotations, protected spans, and metric passthrough, and stops both processes.

For a local one-process test flow, send OTLP traces to the Collector on
`http://localhost:4318` and use the debug exporter or your preferred backend exporter.
Configure a backend for persistent storage when needed.

## Experimental adaptive trace sampling

Run `make experiment` to test the bundled Jev-guided tail-sampling policy against
synthetic traces and mock inference. For a live shadow pilot, export `JEV_API_KEY`
and run:

```sh
./_build/otelcol-jevtraces --config examples/otelcol/config-adaptive.yaml
```

The sampled branch retains protected or uncertain traces and keeps a 10% baseline
of traces whose received spans all have low operation scores. A full-input branch
allows comparison. Both outputs use debug exporters in this example; configure
backend exporters for durable storage. This remains experimental, with explicit
[policy, clustering, and late-span limits](docs/adaptive-sampling.md).

## Prometheus: what Jev has processed

The example exposes the processor's internal metrics at
**http://localhost:8888/metrics**. Restart the Collector after changing its YAML;
no binary rebuild is required for this endpoint. Open the URL in a browser for
raw samples, or configure Prometheus to scrape it:

```yaml
scrape_configs:
  - job_name: jevtraces
    static_configs:
      - targets: ["localhost:8888"]
```

If Prometheus runs in Docker Desktop while the Collector runs on the host, use
`host.docker.internal:8888` as the target instead.

| Metric name at the scrape endpoint | Meaning |
| --- | --- |
| `jevtraces_spans_processed` | All spans inspected by the processor |
| `jevtraces_spans_annotated` | Spans that received cached Jev operation probabilities |
| `jevtraces_spans_protected` | Spans protected by local error, latency, or name rules; inference bypassed |
| `jevtraces_spans_skipped` | Spans whose metadata could not be assessed |
| `jevtraces_inference_requests` | Actual assessment attempts, including attempts that fail |
| `jevtraces_inference_failures` | Failed attempts, excluding shutdown cancellation |
| `jevtraces_cache_hits`, `jevtraces_cache_misses` | Local assessment-cache use |
| `jevtraces_queue_enqueued`, `jevtraces_queue_rejected` | Queue admission and pressure |
| `jevtraces_inference_duration` | Histogram of assessment latency in seconds (`_bucket`, `_sum`, `_count`) |

For example, `rate(jevtraces_spans_processed[5m])` shows spans inspected per second.
Some series appear only after the corresponding event occurs. These names reflect
the example's internal Prometheus exporter, which omits the `_total` suffix.

This endpoint comes from `service.telemetry.metrics.readers`, not the incoming
metrics pipeline. The counters describe span processing and operation-level
inference, not unique whole traces or individual span scores. View per-span Jev
probabilities in trace output or a tracing backend; the counters do not contain
trace IDs or names.

## Processing and annotations

1. Copy the incoming batch so other receiver-sharing pipelines keep their input.
2. Mark protected spans using local rules, without calling Jev.
3. Construct a bounded, allow-listed operation descriptor for other spans.
4. On a cache miss, forward the span with `pending` and queue background work.
5. Validate three typed probabilities and cache successful assessments.
6. Attach the cached assessment to subsequent matching spans until expiry/eviction.

Already-exported spans are never updated retrospectively. Workers do not emit
additional spans. A one-shot operation may therefore only appear as `pending`.

| Span attribute | Meaning |
| --- | --- |
| `jevtraces.assessment.state` | `protected`, `pending`, `scored`, or `skipped` |
| `jevtraces.protection.reason` | Present when protected: `error`, `slow`, `protected_operation`, or `invalid_timing` |
| `jevtraces.model` | Configured model identifier, present when scored |
| `jevtraces.operation.diagnostic_value` | Probability that representative traces containing this operation aid diagnosis |
| `jevtraces.operation.business_criticality` | Probability that this operation participates in a critical workflow |
| `jevtraces.operation.keep_probability` | Probability favoring retention of representative traces for this operation |

The three probability attributes occur only with `scored` and are in `[0, 1]`.
They are independent judgments, not an overall confidence value. Questions are
sent together using TypeSafe's [`noul` primitive](https://docs.typesafe.ai/primitives/noul)
through the [HTTP API](https://docs.typesafe.ai/api).

The `jevtraces.*` span-attribute namespace is reserved. Existing attributes in that
namespace are replaced to avoid forwarding stale scores from another processor.
Other span fields and attributes, resources, scopes, events, links, trace state,
and trace/span IDs remain unchanged. Annotations add storage and indexing volume;
this version provides no volume reduction.

## Configuration

```yaml
processors:
  jevtraces:
    api_key: ${env:JEV_API_KEY}
    base_url: https://api.typesafe.ai
    model: jev-latest
    mode: annotate
    timeout: 3s
    min_inference_interval: 200ms
    score_ttl: 15m
    slow_span_threshold: 1s
    queue_size: 256
    workers: 2
    cache_size: 10000
    max_state_bytes: 16384
    include_span_name: true
    protected_operations: []
    context_attributes:
      - service.name
      - service.namespace
      - deployment.environment.name
    span_attributes:
      - http.request.method
      - http.route
      - rpc.system
      - rpc.service
      - rpc.method
      - db.system.name
      - db.operation.name
      - messaging.system
      - messaging.operation.type

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, jevtraces, batch]
      exporters: [debug]
```

All values above are defaults except the required API key. See the
[complete example](examples/otelcol/config.yaml) for receiver/exporter
definitions and an example protected operation.

Only `annotate` is supported; `reduce` and other modes fail validation. Errors
mean spans whose OTel status code is `Error`. Slow means the span duration is at
least `slow_span_threshold`. These rules mark individual spans; they do not know
whether another span in the same trace failed. Protected operation names match
exactly. Missing start times or reversed timestamps are marked `invalid_timing`.
Local protection always takes precedence over a cached model assessment.

## Metadata privacy and cache identity

The request contains the span name (unless disabled), span kind/status code,
scope name/version, and values of explicitly allowed resource/span attributes.
Use normalized names and route templates. Names, scope information, and allowed
attribute values can contain sensitive data: redact them upstream and configure
allow-lists for your application. `include_span_name: false` excludes names from
inference, while local exact-name protection still works.

Unlisted attributes, URLs, query text, status messages, event names/payloads,
links, IDs, trace state, timestamps, and raw durations are not sent. URLs and
queries could be sent if an operator explicitly adds their attribute keys to an
allow-list. Only scalar attribute values are accepted. Individual string fields
are limited to 512 bytes; oversized or unsupported metadata is marked `skipped`
without an API request. `max_state_bytes` bounds the serialized state, excluding
the fixed question definitions. Each allow-list has a maximum of 64 keys.

The key hashes the exact request state. Two spans with identical descriptors can
share an assessment despite different trace IDs, instance IDs, or timestamps.
Changes to allowed context, names, kind/status, or scope identity produce a new
key. Resource fields not on the allow-list intentionally do not partition this
cache. Use separate processor instances for independent tenants or policies, or
include the required tenant attribute in the allow-list after considering privacy.

The bounded LRU cache expires assessments after `score_ttl`. Pending jobs contain
only bounded state and a hash, not complete spans. Worker count bounds concurrent
requests. `min_inference_interval` (default `200ms`, minimum `1ms`) spaces request
admissions across all workers of one instance, without bursts. Workers wait while
spans continue downstream. This is not a cluster-wide or monetary budget. High-cardinality
operation names can still cause cache churn and frequent requests.

See [production pilot guidance](docs/production.md) for deployment configuration,
cluster budgeting, benchmark commands, and remaining release gates.

## Failures and multiple replicas

API calls run only in background workers. Queue saturation, cache expiry, API
failure, and invalid responses never drop spans. `pending` means no usable local
assessment; it does not guarantee a request is currently queued. API failures
cause a per-instance cooldown of 1, 2, 4, 8, 16, then 32 seconds. Later batches
retry after cooldown. Fresh cached assessments remain usable during an outage.
Shutdown cancels in-flight inference. Downstream delivery errors are propagated.

Each replica has independent cache/queue state. **Redis is not required or
implemented for jevtraces.** Source affinity can reduce duplicate assessment
requests, but arbitrary routing is valid for this annotation-only component.
Cold replicas can show `pending` while others show `scored`, and independently
obtained scores can differ. Jev consistency does not guarantee identical output.
There is no cluster-wide API budget or shared-cache coordination in this version.

For the optional adaptive pipeline, OTel tail sampling owns trace buffering and
decisions. It requires trace-ID routing and has late-span, eviction, and restart
limits described in [adaptive sampling](docs/adaptive-sampling.md). The jevtraces
processor itself never drops individual low-scoring spans.

## Development and evaluation

The independent Go module is
`github.com/ishantanu/jevtraces`, with `NewFactory()` registering
an alpha trace processor named `jevtraces`. The OCB manifest uses a local module
replacement. A future module release would use `vX.Y.Z` tags;
this implementation does not publish a module release.

`make check` runs formatting, vet, and race tests for this project. The trace suite covers API
validation, metadata privacy, cache identity/expiry/eviction, queue deduplication
and pressure, cancellation, cooldown recovery, protection, and payload preservation.
CI also runs the compiled Collector smoke test and adaptive sampling experiment
with synthetic OTLP and mock Jev.

Internal telemetry uses `jevtraces.spans.*`, `jevtraces.cache.*`,
`jevtraces.queue.*`, and `jevtraces.inference.*` counters, plus the
`jevtraces.inference.duration` histogram in seconds. It does not attach operation
names or trace IDs as metric dimensions.

Evaluate annotations against engineer-labeled operations and measure API cost,
latency, cache reuse, and added telemetry volume. Mock tests establish software
behavior, not model accuracy, rare-failure detection, or safe sampling thresholds.

Apache-2.0; see [LICENSE](LICENSE).

### Small inference evaluation

See [eval/README.md](eval/README.md) for a 10-operation synthetic evaluation.
With `JEV_API_KEY` exported, `make eval` runs 20 paid requests using the processor's
actual questions and saves probabilities and agreement against provisional labels.
This is an exploratory evaluation, not a production-quality certification.
