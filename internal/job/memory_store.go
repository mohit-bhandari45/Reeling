package job

import (
	"fmt"
	"sync"
	"time"
)

type MemoryStore struct {
	mu sync.RWMutex
	jobs map[string]*Job
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		jobs: make(map[string]*Job),
	}
}

func (s *MemoryStore) Save(j *Job) error {
	s.mu.Lock();
	defer s.mu.Unlock()

	j.UpdatedAt = time.Now()
	if j.CreatedAt.IsZero() {
		j.CreatedAt = j.UpdatedAt
	}

	s.jobs[j.ID] = j;
	return nil;
}

func (s *MemoryStore) Get(id string) (*Job, error){
	s.mu.RLock()
	defer s.mu.RUnlock()

	j, ok := s.jobs[id];
	if !ok {
		return nil, fmt.Errorf("job %s not found", id)
	}

	return j, nil
}