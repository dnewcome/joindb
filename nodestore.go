package joindb

import "sync"

// NodeStore is a content-addressed blob store. Writes are idempotent:
// Put(h, data) where h == hashOf(data) is assumed; duplicates are no-ops.
type NodeStore interface {
	Get(Hash) ([]byte, bool)
	Put(Hash, []byte)
	Has(Hash) bool
	Len() int
}

// MemStore is an in-memory NodeStore. Safe for concurrent use.
type MemStore struct {
	mu   sync.RWMutex
	data map[Hash][]byte
}

func NewMemStore() *MemStore {
	return &MemStore{data: map[Hash][]byte{}}
}

func (s *MemStore) Get(h Hash) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[h]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (s *MemStore) Put(h Hash, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[h]; exists {
		return
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	s.data[h] = buf
}

func (s *MemStore) Has(h Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[h]
	return ok
}

func (s *MemStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
