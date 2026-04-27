package collector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opensearch-balanser/internal/model"
)

func TestParseSizeGB(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		want  float64
		delta float64
	}{
		{name: "plain_gb", in: "42", want: 42, delta: 0.000001},
		{name: "gb_suffix", in: "12.5gb", want: 12.5, delta: 0.000001},
		{name: "tb_suffix", in: "1tb", want: 1024, delta: 0.000001},
		{name: "mb_suffix", in: "1024mb", want: 1, delta: 0.000001},
		{name: "bytes_suffix", in: "1073741824b", want: 1, delta: 0.000001},
		{name: "dash", in: "-", want: 0, delta: 0.000001},
		{name: "empty", in: "", want: 0, delta: 0.000001},
		{name: "invalid", in: "n/a", want: 0, delta: 0.000001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseSizeGB(tt.in)
			if abs(got-tt.want) > tt.delta {
				t.Fatalf("parseSizeGB(%q) = %f, want %f", tt.in, got, tt.want)
			}
		})
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func TestValidateMoveFallbackWithoutExplain(t *testing.T) {
	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_cluster/reroute" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, string(body))
		if strings.Contains(string(body), "\"explain\":true") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprint(w, `{"error":{"type":"x_content_parse_exception","reason":"[1:142] [cluster_reroute] unknown field [explain]"},"status":400}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"acknowledged":true}`)
	}))
	defer srv.Close()

	a := &OpenSearchAdapter{
		endpoint: srv.URL,
		client:   &http.Client{Timeout: 2 * time.Second},
	}
	step := model.PlanStep{Index: "idx", ShardID: 1, FromNode: "n1", ToNode: "n2"}
	check, err := a.ValidateMove(context.Background(), step)
	if err != nil {
		t.Fatalf("ValidateMove error: %v", err)
	}
	if !check.Allowed {
		t.Fatalf("expected allowed check, got %+v", check)
	}
	if len(requests) != 2 {
		t.Fatalf("expected 2 reroute requests, got %d", len(requests))
	}
	if !strings.Contains(requests[0], "\"explain\":true") {
		t.Fatalf("expected first request with explain=true, got %s", requests[0])
	}
	if strings.Contains(requests[1], "\"explain\":true") {
		t.Fatalf("expected fallback request without explain=true, got %s", requests[1])
	}
}
