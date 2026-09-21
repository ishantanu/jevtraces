package jevtracesprocessor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestJevResponseValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		bad        bool
	}{
		{"valid", validResponse, 200, false},
		{"zero", strings.Replace(validResponse, `"noul":0.1`, `"noul":0`, 1), 200, false},
		{"missing", `{"answers":{}}`, 200, true},
		{"null", strings.Replace(validResponse, `"noul":0.1`, `"noul":null`, 1), 200, true},
		{"wrong type", strings.Replace(validResponse, `"type":"noul"`, `"type":"choice"`, 1), 200, true},
		{"negative", strings.Replace(validResponse, `"noul":0.1`, `"noul":-1`, 1), 200, true},
		{"above one", strings.Replace(validResponse, `"noul":0.1`, `"noul":1.01`, 1), 200, true},
		{"invalid", `{`, 200, true},
		{"trailing", validResponse + ` {}`, 200, true},
		{"oversized", strings.Repeat("x", 65537), 200, true},
		{"server error", "private backend error", 500, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := newJevClient(testConfig())
			j.http.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/v1/systemone" || r.Header.Get("Authorization") != "Bearer test-key" || r.Method != "POST" {
					t.Error("incorrect API request")
				}
				var req struct {
					Model     string              `json:"model"`
					State     json.RawMessage     `json:"state"`
					Questions map[string]question `json:"questions"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if req.Model != "jev-latest" || len(req.Questions) != 3 {
					t.Error("missing model or typed questions")
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			_, err := j.assess(context.Background(), json.RawMessage(`{"operation":{"name":"checkout"}}`))
			if (err != nil) != tc.bad {
				t.Fatalf("error=%v, want bad=%v", err, tc.bad)
			}
			if err != nil && strings.Contains(err.Error(), "private backend error") {
				t.Fatal("error response body leaked")
			}
		})
	}
}

func TestHTTPTimeoutAndRedirect(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-time.After(200 * time.Millisecond):
			}
		}))
		defer server.Close()
		cfg := testConfig()
		cfg.BaseURL = server.URL
		cfg.Timeout = 30 * time.Millisecond
		started := time.Now()
		if _, err := newJevClient(cfg).assess(context.Background(), json.RawMessage(`{}`)); err == nil {
			t.Fatal("timeout ignored")
		}
		if time.Since(started) > time.Second {
			t.Fatal("timeout not bounded")
		}
	})
	t.Run("redirect", func(t *testing.T) {
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("redirect followed") }))
		defer target.Close()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
		}))
		defer server.Close()
		cfg := testConfig()
		cfg.BaseURL = server.URL
		if _, err := newJevClient(cfg).assess(context.Background(), json.RawMessage(`{}`)); err == nil {
			t.Fatal("redirect accepted")
		}
	})
}
