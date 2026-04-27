package collector

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"opensearch-balanser/internal/config"
	"opensearch-balanser/internal/model"
)

type OpenSearchAdapter struct {
	endpoint string
	client   *http.Client
	user     string
	pass     string
}

func NewOpenSearchAdapter(cfg config.ClusterConfig) (*OpenSearchAdapter, error) {
	tr := &http.Transport{}
	if cfg.TLSEnabled {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.SkipTLSVerify} //nolint:gosec
		if cfg.CAFile != "" {
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read CA file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("parse CA file")
			}
			tlsCfg.RootCAs = pool
		}
		tr.TLSClientConfig = tlsCfg
	}

	return &OpenSearchAdapter{
		endpoint: strings.TrimRight(cfg.Endpoint, "/"),
		user:     cfg.Username,
		pass:     config.ResolvePassword(cfg.PasswordEnv),
		client:   &http.Client{Timeout: cfg.RequestTimeout, Transport: tr},
	}, nil
}

func (a *OpenSearchAdapter) CollectSnapshot(ctx context.Context) (model.ClusterSnapshot, error) {
	healthResp, err := a.get(ctx, "/_cluster/health")
	if err != nil {
		return model.ClusterSnapshot{}, err
	}
	var health struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(healthResp, &health); err != nil {
		return model.ClusterSnapshot{}, fmt.Errorf("parse health: %w", err)
	}

	nodesResp, err := a.get(ctx, "/_cat/nodes?format=json&bytes=gb&h=name,ip,disk.total,disk.used,node.role")
	if err != nil {
		return model.ClusterSnapshot{}, err
	}
	var catNodes []map[string]string
	if err := json.Unmarshal(nodesResp, &catNodes); err != nil {
		return model.ClusterSnapshot{}, fmt.Errorf("parse cat nodes: %w", err)
	}

	topoResp, err := a.get(ctx, "/_nodes?filter_path=nodes.*.attributes,nodes.*.name,nodes.*.host,nodes.*.ip")
	if err != nil {
		return model.ClusterSnapshot{}, err
	}
	var topo struct {
		Nodes map[string]struct {
			Name       string            `json:"name"`
			Host       string            `json:"host"`
			IP         string            `json:"ip"`
			Attributes map[string]string `json:"attributes"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(topoResp, &topo); err != nil {
		return model.ClusterSnapshot{}, fmt.Errorf("parse nodes topology: %w", err)
	}

	shardsResp, err := a.get(ctx, "/_cat/shards?format=json&bytes=gb&h=index,shard,prirep,state,node,store")
	if err != nil {
		return model.ClusterSnapshot{}, err
	}
	var catShards []map[string]string
	if err := json.Unmarshal(shardsResp, &catShards); err != nil {
		return model.ClusterSnapshot{}, fmt.Errorf("parse cat shards: %w", err)
	}

	recoveryResp, err := a.get(ctx, "/_cat/recovery?active_only=true&format=json")
	if err != nil {
		return model.ClusterSnapshot{}, err
	}
	var recoveries []map[string]string
	if err := json.Unmarshal(recoveryResp, &recoveries); err != nil {
		return model.ClusterSnapshot{}, fmt.Errorf("parse recovery: %w", err)
	}

	settingsResp, err := a.get(ctx, "/_cluster/settings?include_defaults=true")
	if err != nil {
		return model.ClusterSnapshot{}, err
	}
	w := parseWatermarks(settingsResp)

	nodes := map[string]model.Node{}
	for _, n := range catNodes {
		name := n["name"]
		total := parseSizeGB(n["disk.total"])
		used := parseSizeGB(n["disk.used"])
		roles := strings.Split(strings.TrimSpace(n["node.role"]), "")
		if strings.TrimSpace(n["node.role"]) == "" {
			roles = nil
		}

		node := model.Node{
			ID:          name,
			IP:          n["ip"],
			Host:        name,
			Status:      "online",
			Roles:       roles,
			DiskTotalGB: total,
			DiskUsedGB:  used,
		}
		for _, meta := range topo.Nodes {
			if meta.Name != name {
				continue
			}
			node.Host = meta.Host
			node.IP = meta.IP
			node.Zone = firstNonEmpty(meta.Attributes["zone"], meta.Attributes["availability_zone"])
			node.Rack = meta.Attributes["rack"]
			break
		}
		nodes[name] = node
	}

	shards := make([]model.Shard, 0, len(catShards))
	for _, s := range catShards {
		shardID, _ := strconv.Atoi(s["shard"])
		isPrimary := strings.EqualFold(s["prirep"], "p")
		index := s["index"]
		repID := fmt.Sprintf("%s/%d", index, shardID)
		shards = append(shards, model.Shard{
			Index:     index,
			ShardID:   shardID,
			Primary:   isPrimary,
			State:     s["state"],
			NodeID:    s["node"],
			SizeGB:    parseSizeGB(s["store"]),
			ReplicaID: repID,
		})
	}

	s := model.ClusterSnapshot{
		CollectedAt:      time.Now().UTC(),
		Health:           model.ClusterHealth{Status: strings.ToLower(health.Status)},
		Nodes:            nodes,
		Shards:           shards,
		ActiveOperations: len(recoveries),
		Watermarks:       w,
	}
	id, err := s.ComputeID()
	if err != nil {
		return model.ClusterSnapshot{}, err
	}
	s.ID = id
	return s, nil
}

func (a *OpenSearchAdapter) ValidateMove(ctx context.Context, step model.PlanStep) (model.AllocatorCheck, error) {
	stepRef := fmt.Sprintf("%s/%d:%s->%s", step.Index, step.ShardID, step.FromNode, step.ToNode)
	resp, err := a.rerouteDryRun(ctx, step, true)
	if err != nil && isExplainUnsupported(err) {
		resp, err = a.rerouteDryRun(ctx, step, false)
	}
	if err != nil {
		return model.AllocatorCheck{}, err
	}

	var parsed map[string]any
	if err := json.Unmarshal(resp, &parsed); err != nil {
		return model.AllocatorCheck{}, fmt.Errorf("parse reroute dry-run: %w", err)
	}
	if e, ok := parsed["explanations"]; ok {
		s := fmt.Sprintf("%v", e)
		if strings.Contains(strings.ToLower(s), "no") || strings.Contains(strings.ToLower(s), "cannot") {
			return model.AllocatorCheck{StepRef: stepRef, Allowed: false, Reason: s, Allocator: "opensearch"}, nil
		}
	}
	return model.AllocatorCheck{StepRef: stepRef, Allowed: true, Allocator: "opensearch"}, nil
}

func (a *OpenSearchAdapter) rerouteDryRun(ctx context.Context, step model.PlanStep, explain bool) ([]byte, error) {
	body := map[string]any{
		"commands": []any{map[string]any{"move": map[string]any{
			"index":     step.Index,
			"shard":     step.ShardID,
			"from_node": step.FromNode,
			"to_node":   step.ToNode,
		}}},
		"dry_run": true,
	}
	if explain {
		body["explain"] = true
	}
	b, _ := json.Marshal(body)
	return a.post(ctx, "/_cluster/reroute", b)
}

func isExplainUnsupported(err error) bool {
	// Older OpenSearch versions reject unknown field [explain] for _cluster/reroute.
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown field [explain]")
}

func (a *OpenSearchAdapter) ExecuteMove(ctx context.Context, step model.PlanStep) error {
	body := map[string]any{
		"commands": []any{map[string]any{"move": map[string]any{
			"index":     step.Index,
			"shard":     step.ShardID,
			"from_node": step.FromNode,
			"to_node":   step.ToNode,
		}}},
	}
	b, _ := json.Marshal(body)
	_, err := a.post(ctx, "/_cluster/reroute", b)
	if err != nil {
		return err
	}
	return nil
}

func (a *OpenSearchAdapter) get(ctx context.Context, path string) ([]byte, error) {
	u, err := url.Parse(a.endpoint + path)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if a.user != "" {
		req.SetBasicAuth(a.user, a.pass)
	}
	res, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", path, err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("request %s failed: %s: %s", path, res.Status, string(body))
	}
	return body, nil
}

func (a *OpenSearchAdapter) post(ctx context.Context, path string, body []byte) ([]byte, error) {
	u, err := url.Parse(a.endpoint + path)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.user != "" {
		req.SetBasicAuth(a.user, a.pass)
	}
	res, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post %s: %w", path, err)
	}
	defer res.Body.Close()
	resp, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("post %s failed: %s: %s", path, res.Status, string(resp))
	}
	return resp, nil
}

func parseWatermarks(payload []byte) model.Watermarks {
	w := model.Watermarks{LowPercent: 85, HighPercent: 90, FloodStagePercent: 95}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return w
	}
	read := func(path ...string) float64 {
		cur := any(raw)
		for _, p := range path {
			m, ok := cur.(map[string]any)
			if !ok {
				return 0
			}
			cur = m[p]
		}
		s := fmt.Sprintf("%v", cur)
		s = strings.TrimSuffix(s, "%")
		f, _ := strconv.ParseFloat(s, 64)
		return f
	}
	if v := read("persistent", "cluster", "routing", "allocation", "disk", "watermark", "low"); v > 0 {
		w.LowPercent = v
	}
	if v := read("persistent", "cluster", "routing", "allocation", "disk", "watermark", "high"); v > 0 {
		w.HighPercent = v
	}
	if v := read("persistent", "cluster", "routing", "allocation", "disk", "watermark", "flood_stage"); v > 0 {
		w.FloodStagePercent = v
	}
	if v := read("defaults", "cluster", "routing", "allocation", "disk", "watermark", "low"); v > 0 && w.LowPercent == 85 {
		w.LowPercent = v
	}
	if v := read("defaults", "cluster", "routing", "allocation", "disk", "watermark", "high"); v > 0 && w.HighPercent == 90 {
		w.HighPercent = v
	}
	if v := read("defaults", "cluster", "routing", "allocation", "disk", "watermark", "flood_stage"); v > 0 && w.FloodStagePercent == 95 {
		w.FloodStagePercent = v
	}
	return w
}

func parseSizeGB(v string) float64 {
	raw := strings.ToLower(strings.TrimSpace(v))
	if raw == "" || raw == "-" {
		return 0
	}

	// Fast path: already plain GB number (for cat APIs with bytes=gb).
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}

	unitFactors := []struct {
		suffix string
		factor float64
	}{
		{suffix: "pb", factor: 1024 * 1024},
		{suffix: "p", factor: 1024 * 1024},
		{suffix: "tb", factor: 1024},
		{suffix: "t", factor: 1024},
		{suffix: "gb", factor: 1},
		{suffix: "g", factor: 1},
		{suffix: "mb", factor: 1.0 / 1024},
		{suffix: "m", factor: 1.0 / 1024},
		{suffix: "kb", factor: 1.0 / (1024 * 1024)},
		{suffix: "k", factor: 1.0 / (1024 * 1024)},
		{suffix: "b", factor: 1.0 / (1024 * 1024 * 1024)},
	}
	for _, unit := range unitFactors {
		suffix := unit.suffix
		if !strings.HasSuffix(raw, suffix) {
			continue
		}
		num := strings.TrimSpace(strings.TrimSuffix(raw, suffix))
		if num == "" {
			return 0
		}
		f, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0
		}
		return f * unit.factor
	}
	return 0
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
