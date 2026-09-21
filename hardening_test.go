package jevtracesprocessor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
)

func TestConcurrentWorkersRespectRateLimit(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 4
	cfg.MinInferenceInterval = 25 * time.Millisecond
	p, out := testProcessor(t, cfg)
	var mu sync.Mutex
	var starts []time.Time
	p.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(validResponse))}, nil
	})
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		input := tracesInput()
		firstSpan(input).SetName(fmt.Sprintf("operation-%d", i))
		if err := p.ConsumeTraces(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		got := <-out
		expectState(t, got, "pending")
		assertPreserved(t, input, got)
	}
	await(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(starts) == 6 })
	mu.Lock()
	defer mu.Unlock()
	// Allow scheduler jitter between the admission gate and the mock transport.
	if elapsed := starts[5].Sub(starts[0]); elapsed < 5*cfg.MinInferenceInterval-5*time.Millisecond {
		t.Fatalf("request burst: %s", elapsed)
	}
}

func TestShutdownCancelsRateWait(t *testing.T) {
	cfg := testConfig()
	cfg.MinInferenceInterval = time.Hour
	p, out := testProcessor(t, cfg)
	delayed := &recordingCounter{}
	p.telemetry.rateLimited = delayed
	p.nextRequest = time.Now().Add(time.Hour)
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := p.ConsumeTraces(context.Background(), tracesInput()); err != nil {
		t.Fatal(err)
	}
	expectState(t, <-out, "pending")
	await(t, func() bool { return delayed.value.Load() > 0 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if len(p.pending) != 0 {
		t.Fatal("pending state retained")
	}
}

func TestReplicaFailureIsolationAndColdRestart(t *testing.T) {
	cfg := testConfig()
	healthy, healthyOut := testProcessor(t, cfg)
	failed, failedOut := testProcessor(t, cfg)
	healthy.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(validResponse))}, nil
	})
	failed.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	for _, p := range []*tracesProcessor{healthy, failed} {
		if err := p.Start(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	}
	input := tracesInput()
	key := inputKey(t, input, cfg)
	for _, p := range []*tracesProcessor{healthy, failed} {
		if err := p.ConsumeTraces(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	assertPreserved(t, input, <-healthyOut)
	assertPreserved(t, input, <-failedOut)
	await(t, func() bool { _, ok := healthy.cached(key); return ok })
	await(t, func() bool { failed.mu.Lock(); defer failed.mu.Unlock(); return failed.failures > 0 })
	if err := healthy.ConsumeTraces(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	expectState(t, <-healthyOut, "scored")
	if err := failed.ConsumeTraces(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	expectState(t, <-failedOut, "pending")
	if err := healthy.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted, restartedOut := testProcessor(t, cfg)
	// A newly constructed replica has no inherited cache; prevent real network I/O.
	restarted.client.http.Transport = failed.client.http.Transport
	if err := restarted.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ConsumeTraces(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	expectState(t, <-restartedOut, "pending")
}

func BenchmarkConsumeTraces(b *testing.B) {
	for _, warm := range []bool{false, true} {
		b.Run(fmt.Sprintf("cached=%t", warm), func(b *testing.B) {
			cfg := testConfig()
			next, _ := consumer.NewTraces(func(context.Context, ptrace.Traces) error { return nil })
			p, err := newProcessor(processor.Settings{TelemetrySettings: component.TelemetrySettings{}}, cfg, next)
			if err != nil {
				b.Fatal(err)
			}
			input := tracesInput()
			rs := input.ResourceSpans().At(0)
			ss := rs.ScopeSpans().At(0)
			key, _, err := summarize(ss.Spans().At(0), rs.Resource(), ss.Scope(), cfg)
			if err != nil {
				b.Fatal(err)
			}
			for i := 1; i < 100; i++ {
				firstSpan(input).CopyTo(ss.Spans().AppendEmpty())
			}
			if warm {
				p.cache.put(key, assessment{}, time.Now().Add(time.Hour))
			}
			// No workers: this measures the ingestion path independently of network latency.
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p.ConsumeTraces(context.Background(), input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestConcurrentSuccessPreservesFailureCooldown(t *testing.T) {
	p, out := testProcessor(t, testConfig())
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	p.client.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(validResponse))}, nil
	})
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	input := tracesInput()
	if err := p.ConsumeTraces(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	<-out
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request not started")
	}
	// Simulate another worker failing while the older successful request is in flight.
	p.mu.Lock()
	deadline := time.Now().Add(time.Minute)
	p.retryUntil = deadline
	p.failures = 2
	p.mu.Unlock()
	release <- struct{}{}
	key := inputKey(t, input, &p.cfg)
	await(t, func() bool { _, ok := p.cached(key); return ok })
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retryUntil != deadline || p.failures != 2 {
		t.Fatal("concurrent success erased failure cooldown")
	}
}
