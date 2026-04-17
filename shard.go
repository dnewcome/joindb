package joindb

import (
	"sort"
	"sync"
)

// KV is a key-value pair. The shard does not interpret Value.
type KV struct {
	Key   string
	Value []byte
}

// Shard owns a keyspace. Writes are serialized through a single mutex that
// also guards patch delivery to subscribers, so the commit log is total and
// subscribers observe patches in strict LSN order. v0 is purely in-memory.
type Shard struct {
	mu   sync.RWMutex
	data map[string][]byte
	keys []string // sorted, kept in sync with data
	lsn  uint64
	subs []*Subscription
}

func NewShard() *Shard {
	return &Shard{data: map[string][]byte{}}
}

func (s *Shard) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.data[key]
	if !existed {
		i := sort.SearchStrings(s.keys, key)
		s.keys = append(s.keys, "")
		copy(s.keys[i+1:], s.keys[i:])
		s.keys[i] = key
	}
	buf := make([]byte, len(value))
	copy(buf, value)
	s.data[key] = buf

	s.lsn++
	kind := PatchInsert
	if existed {
		kind = PatchUpdate
	}
	patchVal := make([]byte, len(buf))
	copy(patchVal, buf)
	s.notifyLocked(Patch{LSN: s.lsn, Kind: kind, Key: key, Value: patchVal})
}

// Delete removes key. Returns true if it existed.
func (s *Shard) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[key]; !exists {
		return false
	}
	delete(s.data, key)
	i := sort.SearchStrings(s.keys, key)
	s.keys = append(s.keys[:i], s.keys[i+1:]...)

	s.lsn++
	s.notifyLocked(Patch{LSN: s.lsn, Kind: PatchDelete, Key: key})
	return true
}

func (s *Shard) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Range returns all pairs with lo <= key < hi, in ascending key order.
func (s *Shard) Range(lo, hi string) []KV {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rangeLocked(lo, hi)
}

func (s *Shard) rangeLocked(lo, hi string) []KV {
	if lo >= hi {
		return nil
	}
	start := sort.SearchStrings(s.keys, lo)
	end := sort.SearchStrings(s.keys, hi)
	out := make([]KV, 0, end-start)
	for _, k := range s.keys[start:end] {
		v := s.data[k]
		buf := make([]byte, len(v))
		copy(buf, v)
		out = append(out, KV{Key: k, Value: buf})
	}
	return out
}

func (s *Shard) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// LSN returns the current commit sequence number.
func (s *Shard) LSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lsn
}

// notifyLocked is called under s.mu (exclusive) after a committed write.
func (s *Shard) notifyLocked(p Patch) {
	for _, sub := range s.subs {
		if p.Key >= sub.lo && p.Key < sub.hi {
			sub.deliver(p)
		}
	}
}
