// Package jevtracesprocessor annotates spans with cached Jev operation assessments.
// It does not buffer, sample, or drop traces.
package jevtracesprocessor

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
)

type Config struct {
	APIKey               configopaque.String `mapstructure:"api_key"`
	BaseURL              string              `mapstructure:"base_url"`
	Model                string              `mapstructure:"model"`
	Mode                 string              `mapstructure:"mode"`
	MinInferenceInterval time.Duration       `mapstructure:"min_inference_interval"`
	Timeout              time.Duration       `mapstructure:"timeout"`
	ScoreTTL             time.Duration       `mapstructure:"score_ttl"`
	SlowSpanThreshold    time.Duration       `mapstructure:"slow_span_threshold"`
	QueueSize            int                 `mapstructure:"queue_size"`
	Workers              int                 `mapstructure:"workers"`
	CacheSize            int                 `mapstructure:"cache_size"`
	MaxStateBytes        int                 `mapstructure:"max_state_bytes"`
	IncludeSpanName      bool                `mapstructure:"include_span_name"`
	ContextAttributes    []string            `mapstructure:"context_attributes"`
	SpanAttributes       []string            `mapstructure:"span_attributes"`
	ProtectedOperations  []string            `mapstructure:"protected_operations"`
}

func (c *Config) Validate() error {
	if strings.TrimSpace(string(c.APIKey)) == "" {
		return fmt.Errorf("api_key is required (use ${env:JEV_API_KEY})")
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("base_url must be an HTTP(S) URL without credentials, query, or fragment")
	}
	if strings.TrimSpace(c.Model) == "" {
		return fmt.Errorf("model is required")
	}
	if strings.ToLower(c.Mode) != "annotate" {
		return fmt.Errorf("jevtraces only supports mode: annotate; whole-trace sampling is not implemented")
	}
	if c.MinInferenceInterval < time.Millisecond {
		return fmt.Errorf("min_inference_interval must be at least 1ms")
	}
	if c.Timeout <= 0 || c.ScoreTTL < time.Millisecond || c.SlowSpanThreshold <= 0 {
		return fmt.Errorf("timeout and slow_span_threshold must be positive; score_ttl must be at least 1ms")
	}
	if c.QueueSize <= 0 || c.Workers <= 0 || c.CacheSize <= 0 {
		return fmt.Errorf("queue_size, workers, and cache_size must be positive")
	}
	if c.MaxStateBytes < 256 || c.MaxStateBytes > 1024*1024 {
		return fmt.Errorf("max_state_bytes must be between 256 and 1048576")
	}
	for _, keys := range [][]string{c.ContextAttributes, c.SpanAttributes} {
		if len(keys) > 64 {
			return fmt.Errorf("attribute allow-lists may contain at most 64 keys each")
		}
		for _, key := range keys {
			if key == "" || len(key) > 512 || strings.HasPrefix(key, "jevtraces.") {
				return fmt.Errorf("attribute allow-lists require nonempty keys up to 512 bytes outside jevtraces.*")
			}
		}
	}
	return nil
}
