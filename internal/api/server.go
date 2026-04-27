package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"opensearch-balanser/internal/app"
	"opensearch-balanser/internal/model"
)

type Server struct {
	http *http.Server
	svc  app.Service
	rt   app.Runtime
}

func NewServer(addr string, readTimeout, writeTimeout time.Duration, svc app.Service, rt app.Runtime) *Server {
	s := &Server{svc: svc, rt: rt}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/plan", s.handlePlan)
	mux.HandleFunc("/v1/simulate", s.handleSimulate)
	mux.HandleFunc("/v1/apply", s.handleApply)
	mux.HandleFunc("/v1/stop", s.handleStop)
	mux.HandleFunc("/v1/executions/", s.handleExecution)
	s.http = &http.Server{
		Addr:         addr,
		Handler:      requestIDMiddleware(mux, rt),
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
	return s
}

func (s *Server) ListenAndServe() error {
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(s.rt.Metrics.RenderPrometheus()))
}

func (s *Server) handlePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only POST is allowed")
		return
	}
	ctx := r.Context()
	bundle, err := s.svc.Plan(ctx)
	if err != nil {
		writeError(w, http.StatusBadRequest, "plan_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, bundle)
}

func (s *Server) handleSimulate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only POST is allowed")
		return
	}
	var req struct {
		PlanBundle model.PlanBundle `json:"plan_bundle"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	ctx := r.Context()
	bundle, err := s.svc.DryRun(ctx, req.PlanBundle)
	if err != nil {
		writeError(w, http.StatusBadRequest, "simulate_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, bundle)
}

func (s *Server) handleApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only POST is allowed")
		return
	}
	var req struct {
		PlanBundle     model.PlanBundle `json:"plan_bundle"`
		IdempotencyKey string           `json:"idempotency_key"`
		CorrelationID  string           `json:"correlation_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	ex, err := s.svc.StartExecution(r.Context(), req.PlanBundle, req.IdempotencyKey, req.CorrelationID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "apply_start_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, ex)
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only POST is allowed")
		return
	}
	var req struct {
		ExecutionID string `json:"execution_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if req.ExecutionID == "" {
		writeError(w, http.StatusBadRequest, "execution_id_required", "execution_id is required")
		return
	}
	if err := s.svc.RequestStop(req.ExecutionID); err != nil {
		if errors.Is(err, context.Canceled) {
			writeError(w, http.StatusConflict, "stop_conflict", err.Error())
			return
		}
		writeError(w, http.StatusNotFound, "execution_not_found", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleExecution(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET is allowed")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/executions/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "execution_id_required", "execution id is required")
		return
	}
	exec, ok, err := s.svc.GetExecution(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "execution_get_failed", err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "execution_not_found", fmt.Sprintf("execution %s not found", id))
		return
	}
	writeJSON(w, http.StatusOK, exec)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, reason, msg string) {
	writeJSON(w, code, map[string]any{"error": reason, "message": msg})
}
