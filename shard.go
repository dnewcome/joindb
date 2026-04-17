package joindb

import (
	"sort"
	"sync"
	"time"
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

	mu        sync.RWMutex
	entries   map[string]entry       // live values and tombstones, keyed by key
	keys      []string               // sorted keys of *live* entries
	timeIdx   []timeKey              // sorted by (ModTime, Key); *live* entries only
	topicIdx  map[string][]timeKey   // topic -> sorted (ModTime, Key); live entries only
	lsn       uint64                 // local commit sequence
	lamport   uint64                 // local Lamport clock (max of all seen stamps + local increments)
	subs      []*Subscription
	topicSubs map[string][]*TopicSub // topic -> subscribers for that topic's live result set

	// now is time.Now by default; tests override it for determinism.
	now func() time.Time
}

type entry struct {
	value     []byte // nil iff tombstone
	stamp     Stamp
	tombstone bool
	modTime   time.Time // most recent write
	createdAt time.Time // first insert of this *live* instance; a delete+reinsert starts a new one
	topic     string    // optional grouping attribute; "" == default/no-topic
}

// timeKey is the sort key for the secondary time index: (ModTime, Key)
// lexicographic so ties on time resolve deterministically.
type timeKey struct {
	T   time.Time
	Key string
}

func (a timeKey) less(b timeKey) bool {
	if !a.T.Equal(b.T) {
		return a.T.Before(b.T)
	}
	return a.Key < b.Key
}

// NewShard returns a fresh Shard with an empty ID. Suitable for local
// uses; for bidirectional sync use NewShardID.
func NewShard() *Shard { return NewShardID("") }

// NewShardID returns a Shard whose commits will be stamped with id.
func NewShardID(id string) *Shard {
	return &Shard{
		id:        id,
		entries:   map[string]entry{},
		topicIdx:  map[string][]timeKey{},
		topicSubs: map[string][]*TopicSub{},
		now:       time.Now,
	}
}

// SetClock overrides the clock the shard uses to timestamp local writes.
// Intended for tests that need deterministic ModTimes.
func (s *Shard) SetClock(now func() time.Time) {
	s.mu.Lock()
	s.now = now
	s.mu.Unlock()
}

// ID returns the shard's Stamp origin.
func (s *Shard) ID() string { return s.id }

// Put writes (key, value) under the default topic ("").
func (s *Shard) Put(key string, value []byte) Stamp {
	return s.PutTopic(key, value, "")
}

// PutTopic writes (key, value) tagged with topic. The topic is a
// mutable attribute: a subsequent PutTopic with a different topic moves
// the row between topic-filtered result sets and causes any TopicSub on
// the old topic to emit a synthetic Leave event.
//
// createdAt: on a fresh insert (no prior live entry) it is set to the
// current clock. On an update to an existing live entry it is preserved
// from the prior record. A delete+reinsert starts a new instance, so
// its createdAt ratchets forward.
func (s *Shard) PutTopic(key string, value []byte, topic string) Stamp {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lamport++
	st := Stamp{Lamport: s.lamport, Origin: s.id}
	buf := make([]byte, len(value))
	copy(buf, value)
	mt := s.now()
	cur := s.entries[key]
	createdAt := mt
	if !cur.tombstone && !cur.createdAt.IsZero() {
		createdAt = cur.createdAt
	}
	s.applyLocked(PatchInsert, key, buf, st, mt, createdAt, topic, true)
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
	s.applyLocked(PatchDelete, key, nil, st, s.now(), time.Time{}, "", true)
	return true
}

// Apply processes a patch originating from a remote shard. It is applied
// iff its Stamp dominates the current per-key stamp; otherwise dropped.
// The incoming ModTime is preserved verbatim, so the secondary time
// index reflects the moment of the canonical (winning) write, not the
// moment this shard received it.
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
	var val []byte
	if p.Value != nil {
		val = make([]byte, len(p.Value))
		copy(val, p.Value)
	}
	mt := p.ModTime
	if mt.IsZero() {
		// Fallback for callers that forget to set ModTime: use our clock.
		mt = s.now()
	}
	// createdAt is preserved verbatim from the wire. Winner-takes-all
	// LWW means the dominant patch's createdAt becomes the canonical
	// value on every replica. Callers that forget to set it (legacy
	// patches) fall back to ModTime — identical to a fresh insert.
	createdAt := p.CreatedAt
	if createdAt.IsZero() {
		createdAt = mt
	}
	s.applyLocked(p.Kind, p.Key, val, p.Stamp, mt, createdAt, p.Topic, false)
	return true
}

// applyLocked performs the actual mutation + notify. Must be called with
// s.mu held exclusively. isLocal=true means we just minted st via Put or
// Delete; false means st originated elsewhere. newTopic is ignored when
// kind == PatchDelete.
//
// After mutating primary state and emitting to key-range subscribers,
// applyLocked synthesizes the topic-level diff: TopicEnter/Update/Leave
// events for any TopicSub registered on the relevant topic(s). This is
// the materialized-view maintenance step — when a row's topic changes,
// subscribers on the old topic see a Leave and subscribers on the new
// topic see an Enter, so their result sets stay equivalent to re-running
// the filter from scratch.
func (s *Shard) applyLocked(kind PatchKind, key string, value []byte, st Stamp, mt time.Time, createdAt time.Time, newTopic string, isLocal bool) {
	cur, existed := s.entries[key]
	wasLive := existed && !cur.tombstone
	oldTopic := cur.topic

	// Drop the key from secondary indexes under its *prior* modTime/topic
	// before overwriting — otherwise those indexes would keep stale refs.
	if wasLive {
		s.timeRemove(timeKey{T: cur.modTime, Key: key})
		s.topicIdxRemove(oldTopic, timeKey{T: cur.modTime, Key: key})
	}

	willBeLive := kind != PatchDelete
	switch kind {
	case PatchInsert, PatchUpdate:
		if !wasLive {
			s.insertKey(key)
		}
		s.entries[key] = entry{
			value:     value,
			stamp:     st,
			modTime:   mt,
			createdAt: createdAt,
			topic:     newTopic,
		}
		s.timeInsert(timeKey{T: mt, Key: key})
		s.topicIdxInsert(newTopic, timeKey{T: mt, Key: key})
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
		// Tombstones carry an empty topic and zero createdAt — they
		// don't belong to any live result set.
		s.entries[key] = entry{tombstone: true, stamp: st, modTime: mt}
	}

	s.lsn++
	var pv []byte
	if value != nil {
		pv = make([]byte, len(value))
		copy(pv, value)
	}
	s.notifyLocked(Patch{
		LSN:       s.lsn,
		Kind:      kind,
		Key:       key,
		Value:     pv,
		Stamp:     st,
		ModTime:   mt,
		CreatedAt: createdAt,
		Topic:     newTopic,
	})
	s.emitTopicDiffLocked(key, value, st, mt, createdAt, wasLive, willBeLive, oldTopic, newTopic, cur.createdAt)
}

// emitTopicDiffLocked translates the old/new (live, topic) pair into
// TopicEvent emissions. Called under s.mu (exclusive). oldCreatedAt is
// the prior entry's createdAt — carried on Leave so subscribers can
// filter their result set by creation time without extra lookups;
// newCreatedAt rides Enter/Update.
func (s *Shard) emitTopicDiffLocked(key string, value []byte, st Stamp, mt, newCreatedAt time.Time, wasLive, willBeLive bool, oldTopic, newTopic string, oldCreatedAt time.Time) {
	cp := func() []byte {
		if value == nil {
			return nil
		}
		out := make([]byte, len(value))
		copy(out, value)
		return out
	}
	switch {
	case wasLive && willBeLive && oldTopic == newTopic:
		s.deliverTopicLocked(newTopic, TopicEvent{
			LSN: s.lsn, Kind: TopicUpdate, Key: key, Value: cp(),
			Topic: newTopic, Stamp: st, ModTime: mt, CreatedAt: newCreatedAt,
		})
	case wasLive && willBeLive && oldTopic != newTopic:
		s.deliverTopicLocked(oldTopic, TopicEvent{
			LSN: s.lsn, Kind: TopicLeave, Key: key,
			Topic: oldTopic, Stamp: st, ModTime: mt, CreatedAt: oldCreatedAt,
		})
		s.deliverTopicLocked(newTopic, TopicEvent{
			LSN: s.lsn, Kind: TopicEnter, Key: key, Value: cp(),
			Topic: newTopic, Stamp: st, ModTime: mt, CreatedAt: newCreatedAt,
		})
	case !wasLive && willBeLive:
		s.deliverTopicLocked(newTopic, TopicEvent{
			LSN: s.lsn, Kind: TopicEnter, Key: key, Value: cp(),
			Topic: newTopic, Stamp: st, ModTime: mt, CreatedAt: newCreatedAt,
		})
	case wasLive && !willBeLive:
		s.deliverTopicLocked(oldTopic, TopicEvent{
			LSN: s.lsn, Kind: TopicLeave, Key: key,
			Topic: oldTopic, Stamp: st, ModTime: mt, CreatedAt: oldCreatedAt,
		})
	}
}

func (s *Shard) deliverTopicLocked(topic string, e TopicEvent) {
	for _, sub := range s.topicSubs[topic] {
		sub.deliver(e)
	}
}

func (s *Shard) topicIdxInsert(topic string, tk timeKey) {
	list := s.topicIdx[topic]
	i := sort.Search(len(list), func(i int) bool { return !list[i].less(tk) })
	list = append(list, timeKey{})
	copy(list[i+1:], list[i:])
	list[i] = tk
	s.topicIdx[topic] = list
}

func (s *Shard) topicIdxRemove(topic string, tk timeKey) {
	list := s.topicIdx[topic]
	i := sort.Search(len(list), func(i int) bool { return !list[i].less(tk) })
	if i < len(list) && list[i] == tk {
		list = append(list[:i], list[i+1:]...)
		if len(list) == 0 {
			delete(s.topicIdx, topic)
		} else {
			s.topicIdx[topic] = list
		}
	}
}

func (s *Shard) timeInsert(tk timeKey) {
	i := sort.Search(len(s.timeIdx), func(i int) bool { return !s.timeIdx[i].less(tk) })
	s.timeIdx = append(s.timeIdx, timeKey{})
	copy(s.timeIdx[i+1:], s.timeIdx[i:])
	s.timeIdx[i] = tk
}

func (s *Shard) timeRemove(tk timeKey) {
	i := sort.Search(len(s.timeIdx), func(i int) bool { return !s.timeIdx[i].less(tk) })
	if i < len(s.timeIdx) && s.timeIdx[i] == tk {
		s.timeIdx = append(s.timeIdx[:i], s.timeIdx[i+1:]...)
	}
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

// ModTime returns the per-key wall-clock time of the last write. Zero if
// the key has no history on this shard.
func (s *Shard) ModTime(key string) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries[key].modTime
}

// Topic returns the topic currently tagged on key, or "" if the key is
// absent or tombstoned.
func (s *Shard) Topic(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.entries[key]
	if e.tombstone {
		return ""
	}
	return e.topic
}

// ModifiedKV is a live key with its value, stamp, modification time,
// and creation time.
type ModifiedKV struct {
	Key       string
	Value     []byte
	Stamp     Stamp
	ModTime   time.Time
	CreatedAt time.Time
}

// Modified returns live entries whose last write falls in [tLo, tHi) and
// whose key falls in [keyLo, keyHi), ordered by modification time
// ascending (ties broken by key). Complexity is O(log N + result_size):
// the secondary (ModTime, Key) index is binary-searched to locate tLo,
// then scanned forward until a record past tHi.
//
// Pass a zero tHi for an open upper bound.
//
// Tombstones are not returned; use the patch stream if you need a
// changelog including deletes.
func (s *Shard) Modified(keyLo, keyHi string, tLo, tHi time.Time) []ModifiedKV {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.modifiedLocked(keyLo, keyHi, tLo, tHi)
}

// modifiedLocked implements Modified under caller-held lock. Callers may
// hold either a read lock (Modified) or the write lock (CursorModified,
// which atomically snapshots + registers a subscription).
func (s *Shard) modifiedLocked(keyLo, keyHi string, tLo, tHi time.Time) []ModifiedKV {
	if keyLo >= keyHi {
		return nil
	}
	if !tHi.IsZero() && !tLo.Before(tHi) {
		return nil
	}
	start := sort.Search(len(s.timeIdx), func(i int) bool {
		return !s.timeIdx[i].T.Before(tLo)
	})
	var out []ModifiedKV
	for i := start; i < len(s.timeIdx); i++ {
		tk := s.timeIdx[i]
		if !tHi.IsZero() && !tk.T.Before(tHi) {
			break
		}
		if tk.Key < keyLo || tk.Key >= keyHi {
			continue
		}
		e := s.entries[tk.Key]
		buf := make([]byte, len(e.value))
		copy(buf, e.value)
		out = append(out, ModifiedKV{
			Key:       tk.Key,
			Value:     buf,
			Stamp:     e.stamp,
			ModTime:   e.modTime,
			CreatedAt: e.createdAt,
		})
	}
	return out
}

// StampedKV carries a key's value, its last-writer stamp, its
// modification time, topic, and whether it is a tombstone. Used by
// Entries for stamped state export.
type StampedKV struct {
	Key       string
	Value     []byte // nil iff Tombstone
	Stamp     Stamp
	ModTime   time.Time
	CreatedAt time.Time
	Topic     string
	Tombstone bool
}

// topicSnapshotLocked returns live entries with the given topic whose
// modTime falls in [tLo, tHi) (open hi if tHi.IsZero()), ordered by
// modTime ascending, ties broken by key. O(log N + result_size) against
// the per-topic (modTime, key) index.
func (s *Shard) topicSnapshotLocked(topic string, tLo, tHi time.Time) []ModifiedKV {
	if !tHi.IsZero() && !tLo.Before(tHi) {
		return nil
	}
	list := s.topicIdx[topic]
	start := sort.Search(len(list), func(i int) bool {
		return !list[i].T.Before(tLo)
	})
	var out []ModifiedKV
	for i := start; i < len(list); i++ {
		tk := list[i]
		if !tHi.IsZero() && !tk.T.Before(tHi) {
			break
		}
		e := s.entries[tk.Key]
		buf := make([]byte, len(e.value))
		copy(buf, e.value)
		out = append(out, ModifiedKV{
			Key:       tk.Key,
			Value:     buf,
			Stamp:     e.stamp,
			ModTime:   e.modTime,
			CreatedAt: e.createdAt,
		})
	}
	return out
}

// Entries returns every key the shard has ever seen (live values and
// tombstones), each with its last-writer stamp and modification time.
// Snapshot is atomic; the caller can feed these back through another
// Shard's Apply to bootstrap-replicate state (e.g. after a partition
// heals). Order is unspecified.
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
		out = append(out, StampedKV{
			Key: k, Value: v, Stamp: e.stamp,
			ModTime: e.modTime, CreatedAt: e.createdAt,
			Topic: e.topic, Tombstone: e.tombstone,
		})
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
