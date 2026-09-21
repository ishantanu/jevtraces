package jevtracesprocessor

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestSummaryPrivacyAndIdentity(t *testing.T) {
	cfg := testConfig()
	input := tracesInput()
	rs := input.ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	span := firstSpan(input)
	rs.Resource().Attributes().PutStr("customer.secret", "resource-secret")
	span.Attributes().PutStr("db.query.text", "query-secret")
	span.Status().SetMessage("status-secret")
	span.Events().At(0).Attributes().PutStr("exception.stacktrace", "stack-secret")
	key, state, err := summarize(span, rs.Resource(), ss.Scope(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"resource-secret", "query-secret", "status-secret", "stack-secret", "private-event", "token=secret", "vendor=opaque", "trace_id", "span_id", "duration"} {
		if strings.Contains(string(state), private) {
			t.Fatalf("private data %q entered state", private)
		}
	}
	var decoded map[string]any
	if err := json.Unmarshal(state, &decoded); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state), "/products/{id}") {
		t.Fatal("missing operation metadata")
	}
	// Unique IDs, timestamps and unlisted values must not trigger per-span inference.
	span.SetSpanID(pcommon.SpanID{99})
	span.SetTraceID(pcommon.TraceID{98})
	span.SetStartTimestamp(span.StartTimestamp() + 1)
	span.SetEndTimestamp(span.EndTimestamp() + 1)
	span.Attributes().PutStr("url.full", "another-secret")
	if got := inputKey(t, input, cfg); got != key {
		t.Fatal("per-span or private data changed operation key")
	}
	for _, change := range []func(ptrace.Traces){
		func(td ptrace.Traces) { firstSpan(td).SetName("POST /checkout") },
		func(td ptrace.Traces) { firstSpan(td).SetKind(ptrace.SpanKindClient) },
		func(td ptrace.Traces) { firstSpan(td).Status().SetCode(ptrace.StatusCodeOk) },
		func(td ptrace.Traces) { firstSpan(td).Attributes().PutStr("http.route", "/checkout") },
		func(td ptrace.Traces) {
			td.ResourceSpans().At(0).Resource().Attributes().PutStr("service.name", "payments")
		},
		func(td ptrace.Traces) { td.ResourceSpans().At(0).ScopeSpans().At(0).Scope().SetVersion("2") },
	} {
		copy := ptrace.NewTraces()
		input.CopyTo(copy)
		change(copy)
		if inputKey(t, copy, cfg) == key {
			t.Fatal("distinct assessment inputs shared key")
		}
	}
	cfg.IncludeSpanName = false
	span.SetName("private-name")
	_, state, err = summarize(span, rs.Resource(), ss.Scope(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(state), "private-name") {
		t.Fatal("disabled span name was sent")
	}
}

func TestSummaryBounds(t *testing.T) {
	for _, mutate := range []func(ptrace.Traces, *Config){
		func(td ptrace.Traces, c *Config) { firstSpan(td).SetName(strings.Repeat("x", 513)) },
		func(td ptrace.Traces, c *Config) {
			firstSpan(td).Attributes().PutStr("http.route", strings.Repeat("x", 513))
		},
		func(td ptrace.Traces, c *Config) { firstSpan(td).Attributes().PutEmptyMap("http.route") },
		func(td ptrace.Traces, c *Config) { firstSpan(td).Attributes().PutDouble("http.route", math.NaN()) },
		func(td ptrace.Traces, c *Config) {
			c.MaxStateBytes = 256
			firstSpan(td).Attributes().PutStr("http.route", strings.Repeat("x", 400))
		},
	} {
		td := tracesInput()
		cfg := testConfig()
		mutate(td, cfg)
		rs := td.ResourceSpans().At(0)
		ss := rs.ScopeSpans().At(0)
		if _, _, err := summarize(firstSpan(td), rs.Resource(), ss.Scope(), cfg); err == nil {
			t.Fatal("unsafe summary accepted")
		}
		p, out := testProcessor(t, cfg)
		if err := p.ConsumeTraces(t.Context(), td); err != nil {
			t.Fatal(err)
		}
		expectState(t, <-out, "skipped")
		if len(p.jobs) != 0 {
			t.Fatal("invalid summary queued")
		}
	}
}

func TestCacheBoundAndExpiry(t *testing.T) {
	c := newCache(2)
	now := time.Now()
	expiry := now.Add(time.Minute)
	c.put("a", assessment{}, expiry)
	c.put("b", assessment{}, expiry)
	c.get("a", now)
	c.put("c", assessment{}, expiry)
	if _, ok := c.get("b", now); ok {
		t.Fatal("LRU entry not evicted")
	}
	c.put("a", assessment{KeepProbability: 1}, expiry)
	if a, ok := c.get("a", now); !ok || a.KeepProbability != 1 {
		t.Fatal("cache update failed")
	}
	if _, ok := c.get("a", expiry); ok {
		t.Fatal("expired value returned")
	}
	c.prune(expiry)
	if len(c.entries) != 0 || c.order.Len() != 0 {
		t.Fatal("expired entries retained")
	}
}

func TestConfigValidation(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.APIKey = "" }, func(c *Config) { c.BaseURL = "file:///tmp/secret" },
		func(c *Config) { c.BaseURL = "https://user:secret@example.com" }, func(c *Config) { c.BaseURL = "https://example.com?secret=yes" },
		func(c *Config) { c.Model = "" }, func(c *Config) { c.Mode = "reduce" },
		func(c *Config) { c.MinInferenceInterval = 0 },
		func(c *Config) { c.Timeout = 0 }, func(c *Config) { c.ScoreTTL = 0 }, func(c *Config) { c.SlowSpanThreshold = 0 },
		func(c *Config) { c.QueueSize = 0 }, func(c *Config) { c.Workers = 0 }, func(c *Config) { c.CacheSize = 0 },
		func(c *Config) { c.MaxStateBytes = 1 }, func(c *Config) { c.MaxStateBytes = 2 * 1024 * 1024 },
		func(c *Config) { c.ContextAttributes = []string{"jevtraces.model"} }, func(c *Config) { c.SpanAttributes = []string{""} },
		func(c *Config) { c.SpanAttributes = make([]string, 65) },
	} {
		cfg := testConfig()
		change(cfg)
		if cfg.Validate() == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	cfg := testConfig()
	cfg.Mode = "ANNOTATE"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}
