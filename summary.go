package jevtracesprocessor

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Only this allow-listed descriptor leaves the Collector. No IDs, raw durations,
// status messages, events, links, or unlisted attributes enter the request or key.
type operationSummary struct {
	Name         string         `json:"name,omitempty"`
	Kind         string         `json:"kind"`
	Status       string         `json:"status"`
	ScopeName    string         `json:"scope_name,omitempty"`
	ScopeVersion string         `json:"scope_version,omitempty"`
	Resource     map[string]any `json:"resource,omitempty"`
	Attributes   map[string]any `json:"attributes,omitempty"`
}

func summarize(span ptrace.Span, resource pcommon.Resource, scope pcommon.InstrumentationScope, cfg *Config) (string, json.RawMessage, error) {
	s := operationSummary{Kind: span.Kind().String(), Status: span.Status().Code().String(), ScopeName: scope.Name(), ScopeVersion: scope.Version()}
	if cfg.IncludeSpanName {
		s.Name = span.Name()
	}
	for _, value := range []string{s.Name, s.ScopeName, s.ScopeVersion} {
		if len(value) > 512 {
			return "", nil, fmt.Errorf("metadata field exceeds 512 bytes")
		}
	}
	var err error
	s.Resource, err = allowedValues(resource.Attributes(), cfg.ContextAttributes)
	if err != nil {
		return "", nil, err
	}
	s.Attributes, err = allowedValues(span.Attributes(), cfg.SpanAttributes)
	if err != nil {
		return "", nil, err
	}
	state, err := json.Marshal(struct {
		Kind      string           `json:"kind"`
		Operation operationSummary `json:"operation"`
	}{"otel_operation_metadata_assessment", s})
	if err != nil {
		return "", nil, err
	}
	if len(state) > cfg.MaxStateBytes {
		return "", nil, fmt.Errorf("metadata exceeds max_state_bytes")
	}
	// JSON canonicalizes map ordering. Exact request state is the cache identity;
	// changed allowed context cannot reuse a different operation's assessment.
	return fmt.Sprintf("%x", sha256.Sum256(state)), state, nil
}

func allowedValues(attrs pcommon.Map, keys []string) (map[string]any, error) {
	out := make(map[string]any)
	for _, key := range keys {
		v, ok := attrs.Get(key)
		if !ok {
			continue
		}
		switch v.Type() {
		case pcommon.ValueTypeStr:
			if len(v.Str()) > 512 {
				return nil, fmt.Errorf("metadata attribute exceeds 512 bytes")
			}
		case pcommon.ValueTypeDouble:
			if math.IsNaN(v.Double()) || math.IsInf(v.Double(), 0) {
				return nil, fmt.Errorf("nonfinite metadata attribute")
			}
		case pcommon.ValueTypeInt, pcommon.ValueTypeBool:
		default:
			return nil, fmt.Errorf("allowed metadata attributes must be scalar")
		}
		out[key] = v.AsRaw()
	}
	return out, nil
}

func protectionReason(span ptrace.Span, cfg *Config) string {
	if span.Status().Code() == ptrace.StatusCodeError {
		return "error"
	}
	for _, operation := range cfg.ProtectedOperations {
		if span.Name() == operation {
			return "protected_operation"
		}
	}
	start, end := uint64(span.StartTimestamp()), uint64(span.EndTimestamp())
	if start == 0 || end < start {
		return "invalid_timing"
	}
	// Compare unsigned nanoseconds to avoid time.Duration overflow on bad input.
	if end-start >= uint64(cfg.SlowSpanThreshold) {
		return "slow"
	}
	return ""
}
