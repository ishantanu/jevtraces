package jevtracesprocessor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
)

type evalCase struct {
	ID       string  `json:"id"`
	Group    string  `json:"group,omitempty"`
	Name     string  `json:"name"`
	Service  string  `json:"service"`
	Expected []*bool `json:"expected"`
}
type evalResult struct {
	ID        string    `json:"id"`
	Group     string    `json:"group,omitempty"`
	Repeat    int       `json:"repeat"`
	Expected  []*bool   `json:"expected"`
	Scores    []float64 `json:"scores,omitempty"`
	LatencyMS int64     `json:"latency_ms"`
	Error     string    `json:"error,omitempty"`
}
type evalSummary struct {
	Successful, Failed, Labeled, Correct, AlwaysYesCorrect, CriticalLow int
}

func summarizeEval(rows []evalResult) evalSummary {
	var s evalSummary
	for _, r := range rows {
		if r.Error != "" {
			s.Failed++
			continue
		}
		s.Successful++
		for i, want := range r.Expected {
			if want == nil {
				continue
			}
			s.Labeled++
			if *want {
				s.AlwaysYesCorrect++
			}
			if (r.Scores[i] >= 0.5) == *want {
				s.Correct++
			}
		}
		if r.Expected[1] != nil && *r.Expected[1] && r.Scores[1] < 0.5 {
			s.CriticalLow++
		}
	}
	return s
}
func loadEvalCases(t *testing.T) ([]evalCase, []byte) {
	t.Helper()
	data, err := os.ReadFile("eval/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []evalCase
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	if len(cases) == 0 {
		t.Fatal("empty evaluation dataset")
	}
	for _, c := range cases {
		if c.ID == "" || seen[c.ID] || c.Name == "" || c.Service == "" || len(c.Expected) != 3 {
			t.Fatalf("invalid evaluation case %q", c.ID)
		}
		seen[c.ID] = true
	}
	return cases, data
}

func TestEvaluationDatasetAndSummary(t *testing.T) {
	cases, _ := loadEvalCases(t)
	cfg := testConfig()
	for _, c := range cases {
		input := tracesInput()
		span := firstSpan(input)
		span.SetName(c.Name)
		span.Attributes().Clear()
		rs := input.ResourceSpans().At(0)
		rs.Resource().Attributes().PutStr("service.name", c.Service)
		if _, _, err := summarize(span, rs.Resource(), rs.ScopeSpans().At(0).Scope(), cfg); err != nil {
			t.Fatal(err)
		}
	}
	yes, no := true, false
	rows := []evalResult{
		{Expected: []*bool{&yes, &no, nil}, Scores: []float64{0.8, 0.7, 0.2}},
		{Expected: []*bool{&yes, &yes, &yes}, Error: "unavailable"},
		{Expected: []*bool{&no, &yes, &yes}, Scores: []float64{0.2, 0.1, 0.9}},
	}
	got := summarizeEval(rows)
	if got != (evalSummary{Successful: 2, Failed: 1, Labeled: 5, Correct: 3, AlwaysYesCorrect: 3, CriticalLow: 1}) {
		t.Fatalf("wrong summary: %+v", got)
	}
}

// Opt-in only: ordinary tests never make paid inference requests.
func TestLiveInferenceEvaluation(t *testing.T) {
	if os.Getenv("JEV_EVAL") != "1" {
		t.Skip("run make eval with JEV_API_KEY for paid live evaluation")
	}
	key := os.Getenv("JEV_API_KEY")
	if key == "" {
		t.Fatal("JEV_API_KEY is required; no live evaluation was performed")
	}
	cases, data := loadEvalCases(t)
	cfg := testConfig()
	cfg.APIKey = configopaque.String(key)
	if model := os.Getenv("JEV_EVAL_MODEL"); model != "" {
		cfg.Model = model
	}
	client := newJevClient(cfg)
	questions, _ := json.Marshal(operationQuestions())
	var rows []evalResult
	started := time.Now().UTC()
	for _, c := range cases {
		input := tracesInput()
		span := firstSpan(input)
		span.SetName(c.Name)
		span.Attributes().Clear()
		rs := input.ResourceSpans().At(0)
		rs.Resource().Attributes().PutStr("service.name", c.Service)
		_, state, err := summarize(span, rs.Resource(), rs.ScopeSpans().At(0).Scope(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		for repeat := 1; repeat <= 2; repeat++ {
			begin := time.Now()
			score, err := client.assess(context.Background(), state)
			row := evalResult{ID: c.ID, Group: c.Group, Repeat: repeat, Expected: c.Expected, LatencyMS: time.Since(begin).Milliseconds()}
			if err != nil {
				row.Error = err.Error()
			} else {
				row.Scores = []float64{score.DiagnosticValue, score.BusinessCriticality, score.KeepProbability}
			}
			rows = append(rows, row)
			time.Sleep(200 * time.Millisecond)
		}
	}
	report := struct {
		Note         string       `json:"note"`
		Model        string       `json:"requested_model"`
		Started      time.Time    `json:"started_utc"`
		DatasetSHA   string       `json:"dataset_sha256"`
		QuestionsSHA string       `json:"questions_sha256"`
		Dimensions   []string     `json:"dimensions"`
		Threshold    float64      `json:"threshold"`
		Summary      evalSummary  `json:"summary"`
		Results      []evalResult `json:"results"`
	}{"Synthetic cases with provisional labels; not production accuracy or calibration. Failed requests excluded from agreement, counted separately. Repeats are not independent examples.", cfg.Model, started, fmt.Sprintf("%x", sha256.Sum256(data)), fmt.Sprintf("%x", sha256.Sum256(questions)), []string{"diagnostic_value", "business_criticality", "keep"}, 0.5, summarizeEval(rows), rows}
	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll("eval/results", 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("eval/results", started.Format("20060102T150405.000000000Z")+".json")
	if err = os.WriteFile(path, append(out, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("Report: %s; %+v", path, report.Summary)
	if report.Summary.Failed > 0 {
		t.Errorf("%d inference calls failed; report saved", report.Summary.Failed)
	}
}
