package jevtracesprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

var Type = component.MustNewType("jevtraces")

func NewFactory() processor.Factory {
	return processor.NewFactory(Type, createDefaultConfig, processor.WithTraces(createTraces, component.StabilityLevelAlpha))
}

func createDefaultConfig() component.Config {
	return &Config{
		BaseURL: "https://api.typesafe.ai", Model: "jev-latest", Mode: "annotate",
		MinInferenceInterval: 200 * time.Millisecond, Timeout: 3 * time.Second, ScoreTTL: 15 * time.Minute, SlowSpanThreshold: time.Second,
		QueueSize: 256, Workers: 2, CacheSize: 10000, MaxStateBytes: 16384,
		IncludeSpanName:   true,
		ContextAttributes: []string{"service.name", "service.namespace", "deployment.environment.name"},
		SpanAttributes:    []string{"http.request.method", "http.route", "rpc.system", "rpc.service", "rpc.method", "db.system.name", "db.operation.name", "messaging.system", "messaging.operation.type"},
	}
}

func createTraces(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return newProcessor(set, cfg.(*Config), next)
}
