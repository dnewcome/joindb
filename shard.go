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
// subscribers observe patches in strict LSN order.
//
// Every write carries a Lamport Stamp tagged with the shard's ID; the
// Stamp is stored alongside the value (and alongside tombstones) and used
// as the comparison basis for cross-shard last-writer-wins in
// Shard.Apply. A fresh Shard with an empty ID is fine for single-process
// scenarios with no peer sync; bidirectional bridging requires distinct
// non-empty IDs on each end.
type Shard struct {
	id string

	mu      sync.RWMutex
	entries map[string]entry // live values and tombstones, keyed by key
	keys    []string         // sorted keys of *live* entries
	lsn     uint64           // local commit sequence
	lamport uint64           // local Lamport clock (max of all seen stamps + local increments)
	subs    []*Subscription
}

type entry struct {
	value    []byte // nil iff tombstone
	stamp    Stamp
	tombstone bool
}

// NewShard returns a fresh Shard with an empty ID. Suitable for local
// uses; for bidirectional sync use NewShardID.
func NewShard() *Shard { return NewShardID("") }

// NewShardID returns a Shard whose commits will be stamped with id.
func NewShardID(id string) *Shard {
	return &Shard{id: id, entries: map[string]entry{}}
}

// ID returns the shard's Stamp origin.
func (s *Shard) ID() string { return s.id }

// Put writes (key, value) with a newly minted local Stamp. Returns the
// stamp under which it was recorded.
func (s *Shard) Put(key string, value []byte) Stamp {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lamport++
	st := Stamp{Lamport: s.lamport, Origin: s.id}
	buf := make([]byte, len(value))
	copy(buf, value)
	s.applyLocked(PatchInsert, key, buf, st, true)
	return st
}

// Delete removes key (recording a tombstone with a new local Stamp).
// Returns true if a live value existed; false if the key was absent.
func (s *Shard) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, existed := s.entries[key]
	if !existed || cur.tombstone {
		return false
	}
	s.lamport++
	st := Stamp{Lamport: s.lamport, Origin: s.id}
	s.applyLocked(PatchDelete, key, nil, st, true)
	return true
}

// Apply processes a patch originating from a remote shard. It is applied
// iff its Stamp dominates the current per-key stamp; otherwise dropped.
// Returns true if the patch was applied (and thus propagated to local
// subscribers), false if dropped. The local Lamport clock is advanced to
// max(local, incoming) regardless of application.
func (s *Shard) Apply(p Patch) bool {
	if p.Stamp.IsZero() {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.Stamp.Lamport > s.lamport {
		s.lamport = p.Stamp.Lamport
	}
	cur := s.entries[p.Key]
	if !p.Stamp.Dominates(cur.stamp) {
		return false
	}
	// copy value since caller's []byte may be shared with other subscribers
	var val []byte
	if p.Value != nil {
		val = make([]byte, len(p.Value))
		copy(val, p.Value)
	}
	s.applyLocked(p.Kind, p.Key, val, p.Stamp, false)
	return true
}

// applyLocked performs the actual mutation + notify. Must be called with
// s.mu held exclusively. isLocal=true means we just minted st via Put or
// Delete; false means st originated elsewhere.
func (s *Shard) applyLocked(kind PatchKind, key string, value []byte, st Stamp, isLocal bool) {
	cur, existed := s.entries[key]
	wasLive := existed && !cur.tombstone

	switch kind {
	case PatchInsert, PatchUpdate:
		if !wasLive {
			s.insertKey(key)
		}
		s.entries[key] = entry{value: value, stamp: st}
		// distinguish insert vs update for observability
		if isLocal {
			if wasLive {
				kind = PatchUpdate
			} else {
				kind = PatchInsert
			}
		}
	case PatchDelete:
		if wasLive {
			s.removeKey(key)
		}
		s.entries[key] = entry{tombstone: true, stamp: st}
	}

	s.lsn++
	// Patch.Value: for inserts/updates send a copy so subscribers can't mutate ours.
	var pv []byte
	if value != nil {
		pv = make([]byte, len(value))
		copy(pv, value)
	}
	s.notifyLocked(Patch{
		LSN:   s.lsn,
		Kind:  kind,
		Key:   key,
		Value: pv,
		Stamp: st,
	})
}

func (s *Shard) insertKey(key string) {
	i := sort.SearchStrings(s.keys, key)
	s.keys = append(s.keys, "")
	copy(s.keys[i+1:], s.keys[i:])
	s.keys[i] = key
}

func (s *Shard) removeKey(key string) {
	i := sort.SearchStrings(s.keys, key)
	if i < len(s.keys) && s.keys[i] == key {
		s.keys = append(s.keys[:i], s.keys[i+1:]...)
	}
}

func (s *Shard) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	if !ok || e.tombstone {
		return nil, false
	}
	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true
}

// Stamp returns the per-key stamp, whether the key is live or tombstoned.
// (Origin == "" and Lamport == 0 means the key has no history on this shard.)
func (s *Shard) Stamp(key string) Stamp {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries[key].stamp
}

// StampedKV carries a key's value, its last-writer stamp, and whether
// it is a tombstone. Used by Entries for stamped state export.
type StampedKV struct {
	Key       string
	Value     []byte // nil iff Tombstone
	Stamp     Stamp
	Tombstone bool
}

// Entries returns every key the shard has ever seen (live values and
// tombstones), each with its last-writer stamp. Snapshot is atomic; the
// caller can feed these back through another Shard's Apply to
// bootstrap-replicate state (e.g. after a partition heals). Order is
// unspecified.
func (s *Shard) Entries() []StampedKV {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]StampedKV, 0, len(s.entries))
	for k, e := range s.entries {
		var v []byte
		if !e.tombstone {
			v = make([]byte, len(e.value))
			copy(v, e.value)
		}
		out = append(out, StampedKV{Key: k, Value: v, Stamp: e.stamp, Tombstone: e.tombstone})
	}
	return out
}

// Range returns all live pairs with lo <= key < hi, in ascending key order.
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
		e := s.entries[k]
		buf := make([]byte, len(e.value))
		copy(buf, e.value)
		out = append(out, KV{Key: k, Value: buf})
	}
	return out
}

func (s *Shard) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.keys)
}

// LSN returns the current commit sequence number.
func (s *Shard) LSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lsn
}

// Lamport returns the current Lamport clock.
func (s *Shard) Lamport() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lamport
}

// notifyLocked is called under s.mu (exclusive) after a committed write.
func (s *Shard) notifyLocked(p Patch) {
	for _, sub := range s.subs {
		if p.Key >= sub.lo && p.Key < sub.hi {
			sub.deliver(p)
		}
	}
}
