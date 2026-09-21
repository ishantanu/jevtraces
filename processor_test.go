package jevtracesprocessor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

const validResponse = `{"answers":{"diagnostic_value":{"type":"noul","noul":0.2},"business_criticality":{"type":"noul","noul":0.3},"keep":{"type":"noul","noul":0.1}}}`

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.APIKey = "test-key"
	return cfg
}

func tracesInput() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	rs.SetSchemaUrl("resource-schema")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("shop.http")
	ss.Scope().SetVersion("1.0")
	ss.SetSchemaUrl("scope-schema")
	span := ss.Spans().AppendEmpty()
	span.SetName("GET /products/{id}")
	span.SetKind(ptrace.SpanKindServer)
	span.SetTraceID(pcommon.TraceID{1})
	span.SetSpanID(pcommon.SpanID{2})
	span.SetParentSpanID(pcommon.SpanID{3})
	span.SetFlags(1)
	span.TraceState().FromRaw("vendor=opaque")
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(100, 0)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(100, 100000000)))
	span.Attributes().PutStr("http.request.method", "GET")
	span.Attributes().PutStr("http.route", "/products/{id}")
	span.Attributes().PutStr("url.full", "https://shop/private?token=secret")
	span.Events().AppendEmpty().SetName("private-event")
	span.Links().AppendEmpty().SetTraceID(pcommon.TraceID{4})
	return td
}

func firstSpan(td ptrace.Traces) ptrace.Span {
	return td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
}

func inputKey(t *testing.T, td ptrace.Traces, cfg *Config) string {
	t.Helper()
	rs := td.ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	key, _, err := summarize(ss.Spans().At(0), rs.Resource(), ss.Scope(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func testProcessor(t *testing.T, cfg *Config) (*tracesProcessor, chan ptrace.Traces) {
	t.Helper()
	outputs := make(chan ptrace.Traces, 32)
	next, err := consumer.NewTraces(func(_ context.Context, td ptrace.Traces) error { outputs <- td; return nil })
	if err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}, cfg, next)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return p, outputs
}

func await(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func expectState(t *testing.T, td ptrace.Traces, state string) {
	t.Helper()
	value, ok := firstSpan(td).Attributes().Get("jevtraces.assessment.state")
	if !ok || value.Str() != state {
		t.Fatalf("state=%v, want %s", value.AsRaw(), state)
	}
}

func assertPreserved(t *testing.T, input, output ptrace.Traces) {
	t.Helper()
	clean := ptrace.NewTraces()
	output.CopyTo(clean)
	for r := 0; r < clean.ResourceSpans().Len(); r++ {
		rs := clean.ResourceSpans().At(r)
		for s := 0; s < rs.ScopeSpans().Len(); s++ {
			spans := rs.ScopeSpans().At(s).Spans()
			for i := 0; i < spans.Len(); i++ {
				spans.At(i).Attributes().RemoveIf(func(k string, _ pcommon.Value) bool { return strings.HasPrefix(k, annotationPrefix) })
			}
		}
	}
	m := ptrace.ProtoMarshaler{}
	a, err := m.MarshalTraces(input)
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.MarshalTraces(clean)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatal("trace payload changed beyond jevtraces annotations")
	}
}

func TestAsyncAssessmentAndPreservation(t *testing.T) {
	cfg := testConfig()
	p, out := testProcessor(t, cfg)
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	p.client.http.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(validResponse))}, nil
	})
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	input := tracesInput()
	if err := p.ConsumeTraces(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	first := <-out
	expectState(t, first, "pending")
	assertPreserved(t, input, first)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("inference did not start")
	}
	// Matching operations in later traces share one in-flight request.
	for i := 0; i < 5; i++ {
		if err := p.ConsumeTraces(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		expectState(t, <-out, "pending")
	}
	close(release)
	key := inputKey(t, input, cfg)
	await(t, func() bool { _, ok := p.cached(key); return ok })
	if err := p.ConsumeTraces(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	warm := <-out
	expectState(t, warm, "scored")
	assertPreserved(t, input, warm)
	value, _ := firstSpan(warm).Attributes().Get("jevtraces.operation.keep_probability")
	if value.Double() != 0.1 || warm.SpanCount() != 1 || calls.Load() != 1 {
		t.Fatal("low score dropped a span or repeated inference")
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatal("worker emitted asynchronous trace data")
	}
	other, _ := testProcessor(t, cfg)
	if _, ok := other.cached(key); ok {
		t.Fatal("independent replicas shared local state")
	}
}

func TestProtectionOverridesCachedScore(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(ptrace.Span, *Config)
	}{
		{"error", func(s ptrace.Span, c *Config) {
			s.Status().SetCode(ptrace.StatusCodeError)
			s.Status().SetMessage("private failure")
		}},
		{"slow", func(s ptrace.Span, c *Config) {
			s.SetEndTimestamp(s.StartTimestamp() + pcommon.Timestamp(c.SlowSpanThreshold))
		}},
		{"protected_operation", func(s ptrace.Span, c *Config) { c.ProtectedOperations = []string{s.Name()} }},
		{"invalid_timing", func(s ptrace.Span, c *Config) { s.SetEndTimestamp(s.StartTimestamp() - 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			input := tracesInput()
			tc.mutate(firstSpan(input), cfg)
			p, out := testProcessor(t, cfg)
			p.cache.put(inputKey(t, input, cfg), assessment{}, time.Now().Add(time.Minute))
			if err := p.ConsumeTraces(context.Background(), input); err != nil {
				t.Fatal(err)
			}
			got := <-out
			expectState(t, got, "protected")
			assertPreserved(t, input, got)
			reason, _ := firstSpan(got).Attributes().Get("jevtraces.protection.reason")
			if reason.Str() != tc.name || len(p.jobs) != 0 {
				t.Fatal("protection rule did not bypass inference")
			}
			if _, ok := firstSpan(got).Attributes().Get("jevtraces.operation.keep_probability"); ok {
				t.Fatal("protected span received probability")
			}
		})
	}
}

type recordingCounter struct {
	metric.Int64Counter
	value atomic.Int64
}

func (c *recordingCounter) Add(_ context.Context, n int64, _ ...metric.AddOption) { c.value.Add(n) }

func TestQueueBoundsAndExpiry(t *testing.T) {
	cfg := testConfig()
	cfg.QueueSize = 1
	p, out := testProcessor(t, cfg)
	enqueued, rejected := &recordingCounter{}, &recordingCounter{}
	p.telemetry.enqueued, p.telemetry.rejected = enqueued, rejected
	input := tracesInput()
	p.cache.put(inputKey(t, input, cfg), assessment{}, time.Now().Add(-time.Second))
	for _, name := range []string{"GET /products/{id}", "GET /products/{id}", "other"} {
		firstSpan(input).SetName(name)
		if err := p.ConsumeTraces(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		got := <-out
		expectState(t, got, "pending")
		assertPreserved(t, input, got)
	}
	if len(p.pending) != 1 || len(p.jobs) != 1 || enqueued.value.Load() != 1 || rejected.value.Load() != 1 {
		t.Fatal("queue admission bounds or counters incorrect")
	}
}

func TestFailureCooldownAndRecovery(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	p, out := testProcessor(t, cfg)
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	p.now = func() time.Time { return time.Unix(0, now.Load()) }
	var calls atomic.Int32
	p.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		body := validResponse
		if calls.Add(1) == 1 {
			body = `{"answers":{}}`
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	input := tracesInput()
	send := func() {
		t.Helper()
		if err := p.ConsumeTraces(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		expectState(t, <-out, "pending")
	}
	send()
	await(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.failures == 1 })
	send()
	if calls.Load() != 1 {
		t.Fatal("retried during cooldown")
	}
	now.Add(int64(2 * time.Second))
	send()
	key := inputKey(t, input, cfg)
	await(t, func() bool { _, ok := p.cached(key); return ok })
	if calls.Load() != 2 {
		t.Fatal("did not recover")
	}
	now.Add(int64(cfg.ScoreTTL))
	send()
	await(t, func() bool { return calls.Load() == 3 })
}

func TestShutdownCancelsInference(t *testing.T) {
	p, out := testProcessor(t, testConfig())
	entered := make(chan struct{})
	p.client.http.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := p.ConsumeTraces(context.Background(), tracesInput()); err != nil {
		t.Fatal(err)
	}
	<-out
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("inference not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if len(p.pending) != 0 {
		t.Fatal("shutdown retained pending metadata")
	}
	if err := p.Start(context.Background(), nil); err == nil {
		t.Fatal("restart accepted")
	}
}

func TestFactoryAndDownstreamErrors(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.APIKey = "test"
	if f.TracesStability() != component.StabilityLevelAlpha || cfg.Mode != "annotate" {
		t.Fatal("incorrect defaults")
	}
	settings := processor.Settings{ID: component.NewID(Type)}
	want := errors.New("downstream unavailable")
	next, err := consumer.NewTraces(func(context.Context, ptrace.Traces) error { return want })
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.CreateTraces(context.Background(), settings, cfg, next)
	if err != nil {
		t.Fatal(err)
	}
	if p.Capabilities().MutatesData {
		t.Fatal("processor advertises input mutation")
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown(context.Background())
	input := tracesInput()
	firstSpan(input).Status().SetCode(ptrace.StatusCodeError)
	if err := p.ConsumeTraces(context.Background(), input); !errors.Is(err, want) {
		t.Fatal("downstream error swallowed")
	}
	if _, err := f.CreateMetrics(context.Background(), settings, cfg, nil); err == nil {
		t.Fatal("metrics unexpectedly supported")
	}
	if _, err := f.CreateLogs(context.Background(), settings, cfg, nil); err == nil {
		t.Fatal("logs unexpectedly supported")
	}
}
