package jevtracesprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

const annotationPrefix = "jevtraces."

type scoreJob struct {
	key   string
	state json.RawMessage
}

type tracesProcessor struct {
	cfg             Config
	next            consumer.Traces
	client          *jevClient
	logger          *zap.Logger
	telemetry       telemetry
	mu              sync.Mutex
	cache           *assessmentCache
	pending         map[string]bool
	jobs            chan scoreJob
	retryUntil      time.Time
	failures        int
	started, closed bool
	cancel          context.CancelFunc
	done            chan struct{}
	wg              sync.WaitGroup
	now             func() time.Time
}

var _ processor.Traces = (*tracesProcessor)(nil)

func newProcessor(set processor.Settings, cfg *Config, next consumer.Traces) (*tracesProcessor, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	t, err := newTelemetry(set.MeterProvider)
	if err != nil {
		return nil, err
	}
	copyCfg := *cfg
	copyCfg.ContextAttributes = append([]string(nil), cfg.ContextAttributes...)
	copyCfg.SpanAttributes = append([]string(nil), cfg.SpanAttributes...)
	copyCfg.ProtectedOperations = append([]string(nil), cfg.ProtectedOperations...)
	logger := set.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &tracesProcessor{cfg: copyCfg, next: next, client: newJevClient(&copyCfg), logger: logger, telemetry: t,
		cache: newCache(cfg.CacheSize), pending: make(map[string]bool), jobs: make(chan scoreJob, cfg.QueueSize), done: make(chan struct{}), now: time.Now}, nil
}

func (p *tracesProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *tracesProcessor) Start(context.Context, component.Host) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started || p.closed {
		return fmt.Errorf("jevtraces processor cannot be started twice or after shutdown")
	}
	p.started = true
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.wg.Add(p.cfg.Workers + 1)
	for i := 0; i < p.cfg.Workers; i++ {
		go p.worker(ctx)
	}
	go func() {
		defer p.wg.Done()
		interval := min(p.cfg.ScoreTTL, time.Minute)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.mu.Lock()
				p.cache.prune(p.now())
				p.mu.Unlock()
			}
		}
	}()
	go func() { p.wg.Wait(); close(p.done) }()
	return nil
}

func (p *tracesProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	started := p.started
	if p.cancel != nil {
		p.cancel()
	}
	p.mu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-p.done:
		// Release queued metadata when a pipeline is shut down.
		p.mu.Lock()
		for len(p.jobs) > 0 {
			<-p.jobs
		}
		clear(p.pending)
		p.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *tracesProcessor) ConsumeTraces(ctx context.Context, input ptrace.Traces) error {
	out := ptrace.NewTraces()
	input.CopyTo(out)
	for ri := 0; ri < out.ResourceSpans().Len(); ri++ {
		rs := out.ResourceSpans().At(ri)
		for si := 0; si < rs.ScopeSpans().Len(); si++ {
			ss := rs.ScopeSpans().At(si)
			for i := 0; i < ss.Spans().Len(); i++ {
				span := ss.Spans().At(i)
				p.telemetry.processed.Add(ctx, 1)
				attrs := span.Attributes()
				// This namespace is owned by the processor. Never forward stale scores
				// from an upstream instance as if they were this instance's assessment.
				attrs.RemoveIf(func(key string, _ pcommon.Value) bool { return strings.HasPrefix(key, annotationPrefix) })
				reason := protectionReason(span, &p.cfg)
				if reason != "" {
					attrs.PutStr("jevtraces.assessment.state", "protected")
					attrs.PutStr("jevtraces.protection.reason", reason)
					p.telemetry.protected.Add(ctx, 1)
					continue
				}
				key, state, err := summarize(span, rs.Resource(), ss.Scope(), &p.cfg)
				if err != nil {
					attrs.PutStr("jevtraces.assessment.state", "skipped")
					p.telemetry.skipped.Add(ctx, 1)
					continue
				}
				score, ok := p.cached(key)
				if !ok {
					attrs.PutStr("jevtraces.assessment.state", "pending")
					p.telemetry.misses.Add(ctx, 1)
					p.enqueue(ctx, key, state)
					continue
				}
				attrs.PutStr("jevtraces.assessment.state", "scored")
				attrs.PutStr("jevtraces.model", p.cfg.Model)
				attrs.PutDouble("jevtraces.operation.diagnostic_value", score.DiagnosticValue)
				attrs.PutDouble("jevtraces.operation.business_criticality", score.BusinessCriticality)
				attrs.PutDouble("jevtraces.operation.keep_probability", score.KeepProbability)
				p.telemetry.hits.Add(ctx, 1)
				p.telemetry.annotated.Add(ctx, 1)
			}
		}
	}
	return p.next.ConsumeTraces(ctx, out)
}

func (p *tracesProcessor) cached(key string) (assessment, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cache.get(key, p.now())
}

func (p *tracesProcessor) enqueue(ctx context.Context, key string, state json.RawMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending[key] {
		return
	}
	// A worker may have populated the cache since the delivery goroutine's miss.
	if _, ok := p.cache.get(key, p.now()); ok {
		return
	}
	if p.closed || p.now().Before(p.retryUntil) {
		p.telemetry.rejected.Add(ctx, 1)
		return
	}
	select {
	case p.jobs <- scoreJob{key, state}:
		p.pending[key] = true
		p.telemetry.enqueued.Add(ctx, 1)
	default:
		p.telemetry.rejected.Add(ctx, 1)
	}
}

func (p *tracesProcessor) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-p.jobs:
			if ctx.Err() != nil {
				return
			}
			p.mu.Lock()
			cooling := p.now().Before(p.retryUntil)
			if cooling {
				delete(p.pending, job.key)
			}
			p.mu.Unlock()
			if cooling {
				continue
			}
			p.telemetry.requests.Add(ctx, 1)
			started := time.Now()
			value, err := p.client.assess(ctx, job.state)
			p.telemetry.latency.Record(ctx, time.Since(started).Seconds())
			p.mu.Lock()
			delete(p.pending, job.key)
			if ctx.Err() != nil {
				p.mu.Unlock()
				return
			}
			if err == nil {
				p.cache.put(job.key, value, p.now().Add(p.cfg.ScoreTTL))
				p.failures = 0
				p.retryUntil = time.Time{}
			} else {
				p.failures = min(p.failures+1, 6)
				p.retryUntil = p.now().Add(time.Second << (p.failures - 1))
			}
			p.mu.Unlock()
			if err != nil {
				p.telemetry.failures.Add(ctx, 1)
				p.logger.Warn("Jev operation assessment failed; spans remain retained", zap.Error(err))
			}
		}
	}
}
