package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"opensearch-balanser/internal/model"
)

type ExecutionStore interface {
	Create(exec model.Execution) error
	Get(id string) (model.Execution, bool, error)
	Update(exec model.Execution) error
	FindByIdempotency(key string) (model.Execution, bool, error)
	RequestStop(id string) error
}

type FileExecutionStore struct {
	path string
	mu   sync.Mutex
}

type storedExecutions struct {
	Items map[string]model.Execution `json:"items"`
}

func NewFileExecutionStore(dataDir string) (*FileExecutionStore, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	path := filepath.Join(dataDir, "executions.json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		seed := storedExecutions{Items: map[string]model.Execution{}}
		b, _ := json.MarshalIndent(seed, "", "  ")
		if err := os.WriteFile(path, b, 0o600); err != nil {
			return nil, err
		}
	}
	return &FileExecutionStore{path: path}, nil
}

func (s *FileExecutionStore) Create(exec model.Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.load()
	if err != nil {
		return err
	}
	if _, ok := data.Items[exec.ID]; ok {
		return fmt.Errorf("execution already exists: %s", exec.ID)
	}
	if exec.CreatedAt.IsZero() {
		exec.CreatedAt = time.Now().UTC()
	}
	exec.UpdatedAt = exec.CreatedAt
	data.Items[exec.ID] = exec
	return s.save(data)
}

func (s *FileExecutionStore) Get(id string) (model.Execution, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.load()
	if err != nil {
		return model.Execution{}, false, err
	}
	ex, ok := data.Items[id]
	return ex, ok, nil
}

func (s *FileExecutionStore) Update(exec model.Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.load()
	if err != nil {
		return err
	}
	if _, ok := data.Items[exec.ID]; !ok {
		return fmt.Errorf("execution not found: %s", exec.ID)
	}
	exec.UpdatedAt = time.Now().UTC()
	data.Items[exec.ID] = exec
	return s.save(data)
}

func (s *FileExecutionStore) FindByIdempotency(key string) (model.Execution, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.load()
	if err != nil {
		return model.Execution{}, false, err
	}
	for _, ex := range data.Items {
		if key != "" && ex.IdempotencyKey == key {
			return ex, true, nil
		}
	}
	return model.Execution{}, false, nil
}

func (s *FileExecutionStore) RequestStop(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.load()
	if err != nil {
		return err
	}
	exec, ok := data.Items[id]
	if !ok {
		return fmt.Errorf("execution not found: %s", id)
	}
	exec.StopRequested = true
	exec.UpdatedAt = time.Now().UTC()
	data.Items[id] = exec
	return s.save(data)
}

func (s *FileExecutionStore) load() (storedExecutions, error) {
	b, err := os.ReadFile(s.path)
	if err != nil {
		return storedExecutions{}, err
	}
	var data storedExecutions
	if err := json.Unmarshal(b, &data); err != nil {
		return storedExecutions{}, err
	}
	if data.Items == nil {
		data.Items = map[string]model.Execution{}
	}
	return data, nil
}

func (s *FileExecutionStore) save(data storedExecutions) error {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, b, 0o600)
}
