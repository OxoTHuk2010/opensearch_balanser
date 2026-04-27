package observability

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type Metrics struct {
	mu      sync.Mutex
	counter map[string]uint64
	gauge   map[string]float64
}

func NewMetrics() *Metrics {
	return &Metrics{counter: map[string]uint64{}, gauge: map[string]float64{}}
}

func (m *Metrics) Inc(name string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counter[key(name, labels)]++
}

func (m *Metrics) SetGauge(name string, value float64, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauge[key(name, labels)] = value
}

func (m *Metrics) RenderPrometheus() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	lines := make([]string, 0, len(m.counter)+len(m.gauge))
	for k, v := range m.counter {
		lines = append(lines, fmt.Sprintf("%s %d", k, v))
	}
	for k, v := range m.gauge {
		lines = append(lines, fmt.Sprintf("%s %f", k, v))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}

func key(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return sanitize(name)
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", sanitize(k), labels[k]))
	}
	return fmt.Sprintf("%s{%s}", sanitize(name), strings.Join(parts, ","))
}

func sanitize(s string) string {
	repl := strings.NewReplacer("-", "_", ".", "_", " ", "_")
	return repl.Replace(s)
}
