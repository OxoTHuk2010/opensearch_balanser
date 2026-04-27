package app

import (
	"testing"
	"time"

	"opensearch-balanser/internal/model"
)

func TestFileExecutionStoreCreateGetUpdateStop(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileExecutionStore(dir)
	if err != nil {
		t.Fatalf("init store: %v", err)
	}
	exec := model.Execution{ID: "e1", IdempotencyKey: "k1", Status: model.ExecutionPending, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := store.Create(exec); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, ok, err := store.Get("e1")
	if err != nil || !ok {
		t.Fatalf("get failed: %v", err)
	}
	if got.IdempotencyKey != "k1" {
		t.Fatalf("unexpected idempotency key")
	}
	got.Status = model.ExecutionRunning
	if err := store.Update(got); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := store.RequestStop("e1"); err != nil {
		t.Fatalf("request stop: %v", err)
	}
	got, ok, err = store.Get("e1")
	if err != nil || !ok || !got.StopRequested {
		t.Fatalf("expected stop requested, got %+v, err=%v", got, err)
	}
	found, ok, err := store.FindByIdempotency("k1")
	if err != nil || !ok || found.ID != "e1" {
		t.Fatalf("find by idempotency failed")
	}
}
