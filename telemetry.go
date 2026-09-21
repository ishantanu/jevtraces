package jevtracesprocessor

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type telemetry struct {
	processed, annotated, protected, skipped metric.Int64Counter
	hits, misses, enqueued, rejected         metric.Int64Counter
	requests, failures, rateLimited          metric.Int64Counter
	latency                                  metric.Float64Histogram
}

func newTelemetry(provider metric.MeterProvider) (telemetry, error) {
	if provider == nil {
		provider = noop.NewMeterProvider()
	}
	meter := provider.Meter("github.com/ishantanu/jevtraces")
	t := telemetry{}
	for _, item := range []struct {
		name, description string
		target            *metric.Int64Counter
	}{
		{"spans.processed", "Input spans processed; all are retained.", &t.processed},
		{"spans.annotated", "Spans annotated with cached operation probabilities.", &t.annotated},
		{"spans.protected", "Spans marked by deterministic protection rules.", &t.protected},
		{"spans.skipped", "Spans whose metadata could not be assessed.", &t.skipped},
		{"cache.hits", "Operation assessments reused from the local cache.", &t.hits},
		{"cache.misses", "Spans without a fresh local operation assessment.", &t.misses},
		{"queue.enqueued", "Operation assessment jobs accepted by the queue.", &t.enqueued},
		{"queue.rejected", "New jobs rejected due to capacity, cooldown, or shutdown.", &t.rejected},
		{"inference.rate_limited", "Assessment jobs delayed by the per-instance request rate limit.", &t.rateLimited},
		{"inference.requests", "Jev assessment attempts started by workers.", &t.requests},
		{"inference.failures", "Failed Jev assessments excluding shutdown cancellation.", &t.failures},
	} {
		counter, err := meter.Int64Counter("jevtraces."+item.name, metric.WithDescription(item.description))
		if err != nil {
			return telemetry{}, fmt.Errorf("create telemetry %s: %w", item.name, err)
		}
		*item.target = counter
	}
	var err error
	t.latency, err = meter.Float64Histogram("jevtraces.inference.duration", metric.WithUnit("s"), metric.WithDescription("Duration of Jev assessment attempts, including failures."))
	return t, err
}
