package api

import (
	"net/http"
	"time"

	"opensearch-balanser/internal/app"
)

func requestIDMiddleware(next http.Handler, rt app.Runtime) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corr := r.Header.Get("X-Correlation-ID")
		if corr == "" {
			corr = app.NewID("http")
		}
		w.Header().Set("X-Correlation-ID", corr)
		start := time.Now()
		rt.Logger.Info("http_request_start", map[string]any{"method": r.Method, "path": r.URL.Path, "correlation_id": corr})
		next.ServeHTTP(w, r)
		rt.Metrics.Inc("http_requests_total", map[string]string{"path": r.URL.Path, "method": r.Method})
		rt.Metrics.SetGauge("http_last_duration_seconds", time.Since(start).Seconds(), map[string]string{"path": r.URL.Path})
		rt.Logger.Info("http_request_done", map[string]any{"method": r.Method, "path": r.URL.Path, "duration_sec": time.Since(start).Seconds(), "correlation_id": corr})
	})
}
