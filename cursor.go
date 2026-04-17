package joindb

import (
	"sync"
	"time"
)

type PatchKind int

const (
	PatchInsert PatchKind = iota + 1
	PatchUpdate
	PatchDelete
)

func (k PatchKind) String() string {
	switch k {
	case PatchInsert:
		return "insert"
	case PatchUpdate:
		return "update"
	case PatchDelete:
		return "delete"
	}
	return "?"
}

// Patch is a single committed mutation. LSNs are per-shard and strictly
// monotonic; subscribers observe Patches in LSN order. The Stamp carries
// the Lamport/origin pair used for cross-shard last-writer-wins — when
// this patch was propagated from another node, Stamp records *that*
// node's Lamport at the moment of its original commit, not ours.
type Patch struct {
	LSN       uint64
	Kind      PatchKind
	Key       string
	Value     []byte // nil for PatchDelete
	Stamp     Stamp
	ModTime   time.Time // wall-clock instant the write was committed at its origin
	CreatedAt time.Time // origin's createdAt for this live instance; propagated verbatim via LWW
	Topic     string    // grouping attribute at the moment of write; "" for default
}

// TopicEventKind labels the diff produced when a key's topic membership
// changes. TopicEnter means the key just entered the topic's live
// result set; TopicUpdate means it was already in the set and its value
// changed in place; TopicLeave means it left the set (either because
// its topic changed or because it was deleted).
type TopicEventKind int

const (
	TopicEnter TopicEventKind = iota + 1
	TopicUpdate
	TopicLeave
)

func (k TopicEventKind) String() string {
	switch k {
	case TopicEnter:
		return "enter"
	case TopicUpdate:
		return "update"
	case TopicLeave:
		return "leave"
	}
	return "?"
}

// TopicEvent is a single logical change to a topic-filtered live result
// set. Topic is the topic the event applies to (i.e. the subscriber's
// topic): for Enter/Update it's the row's new topic, for Leave it's the
// topic the row just left. Value is nil for Leave. CreatedAt is the
// prior entry's createdAt on Leave; the new entry's createdAt on
// Enter/Update — so subscribers filtering by a fixed creation range
// can make the admit/evict decision without touching shard state.
type TopicEvent struct {
	LSN       uint64
	Kind      TopicEventKind
	Key       string
	Value     []byte
	Topic     string
	Stamp     Stamp
	ModTime   time.Time
	CreatedAt time.Time
}

// TopicSub is a live view over the set of keys whose current topic ==
// topic. It receives synthesized TopicEvents (Enter/Update/Leave) in
// LSN order so a consumer applying Snapshot then draining Next()
// maintains a result set equivalent to re-running the filter query at
// the current LSN.
type TopicSub struct {
	Snapshot []ModifiedKV
	StartLSN uint64

	topic string
	shard *Shard

	mu     sync.Mutex
	cond   *sync.Cond
	buf    []TopicEvent
	closed bool
}

// TopicCursor is the result of CursorTopic: an atomic snapshot of rows
// currently in (topic, [tLo, tHi)) paired with a TopicSub that delivers
// every subsequent diff affecting that topic.
type TopicCursor struct {
	Snapshot []ModifiedKV
	Sub      *TopicSub
}

// CursorTopic atomically snapshots live entries with current topic ==
// topic and modTime in [tLo, tHi), and registers a topic subscription
// that will deliver Enter/Update/Leave events for every subsequent
// write that changes the topic's membership. Pass a zero tHi for an
// open upper bound. The subscription is *not* time-filtered — callers
// manage rolling-window eviction themselves.
func (s *Shard) CursorTopic(topic string, tLo, tHi time.Time) *TopicCursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := s.topicSnapshotLocked(topic, tLo, tHi)
	sub := &TopicSub{
		StartLSN: s.lsn + 1,
		topic:    topic,
		shard:    s,
	}
	sub.cond = sync.NewCond(&sub.mu)
	s.topicSubs[topic] = append(s.topicSubs[topic], sub)
	return &TopicCursor{Snapshot: snap, Sub: sub}
}

func (s *TopicSub) deliver(e TopicEvent) {
	s.mu.Lock()
	s.buf = append(s.buf, e)
	s.cond.Signal()
	s.mu.Unlock()
}

// Next returns the next TopicEvent in LSN order. Blocks until one is
// available or the subscription is closed and its buffer is drained.
func (s *TopicSub) Next() (TopicEvent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.buf) == 0 && !s.closed {
		s.cond.Wait()
	}
	if len(s.buf) == 0 {
		return TopicEvent{}, false
	}
	e := s.buf[0]
	s.buf = s.buf[1:]
	return e, true
}

// Close unregisters the topic subscription. Buffered events remain
// drainable via Next before it returns (_, false).
func (s *TopicSub) Close() {
	s.shard.mu.Lock()
	list := s.shard.topicSubs[s.topic]
	for i, sub := range list {
		if sub == s {
			list = append(list[:i], list[i+1:]...)
			if len(list) == 0 {
				delete(s.shard.topicSubs, s.topic)
			} else {
				s.shard.topicSubs[s.topic] = list
			}
			break
		}
	}
	s.shard.mu.Unlock()

	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// Subscription is a live view over (Shard, [lo, hi)). It exposes an
// atomic initial snapshot (the range at commit StartLSN-1) plus every
// subsequent commit in the range. Apply Snapshot then iterate Next to
// maintain a consistent local mirror — no missed or duplicated rows,
// even under concurrent writers.
type Subscription struct {
	Snapshot []KV
	StartLSN uint64

	lo, hi string
	shard  *Shard

	mu     sync.Mutex
	cond   *sync.Cond
	buf    []Patch
	closed bool
}

// Subscribe atomically snapshots [lo, hi) at the current LSN and registers
// for all subsequent patches affecting that range.
func (s *Shard) Subscribe(lo, hi string) *Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := &Subscription{
		Snapshot: s.rangeLocked(lo, hi),
		StartLSN: s.lsn + 1,
		lo:       lo,
		hi:       hi,
		shard:    s,
	}
	sub.cond = sync.NewCond(&sub.mu)
	s.subs = append(s.subs, sub)
	return sub
}

// TimedCursor is the result of CursorModified: an atomic snapshot of
// keys currently in both a key range and a time range, paired with a
// key-range subscription that will deliver every subsequent write to
// that key range. The subscription is not time-filtered — callers get
// every patch in the key range so they can maintain their own
// rolling-window view (adding keys whose new ModTime enters the window,
// evicting keys whose ModTime has moved out).
//
// Consistency: every write with LSN < Sub.StartLSN whose ModTime falls
// in [tLo, tHi) is represented in Snapshot; every write with LSN >=
// Sub.StartLSN arrives via Sub.Next in order.
type TimedCursor struct {
	Snapshot []ModifiedKV
	Sub      *Subscription
}

// CursorModified atomically snapshots live entries with key in
// [keyLo, keyHi) and modTime in [tLo, tHi), and registers a
// key-range subscription for all subsequent patches in that key range.
// Pass a zero tHi for an open upper bound.
func (s *Shard) CursorModified(keyLo, keyHi string, tLo, tHi time.Time) *TimedCursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := s.modifiedLocked(keyLo, keyHi, tLo, tHi)
	sub := &Subscription{
		StartLSN: s.lsn + 1,
		lo:       keyLo,
		hi:       keyHi,
		shard:    s,
	}
	sub.cond = sync.NewCond(&sub.mu)
	s.subs = append(s.subs, sub)
	return &TimedCursor{Snapshot: snap, Sub: sub}
}

// deliver is called by Shard under its write lock. It appends to the
// subscription's buffer; it never blocks the writer.
func (s *Subscription) deliver(p Patch) {
	s.mu.Lock()
	s.buf = append(s.buf, p)
	s.cond.Signal()
	s.mu.Unlock()
}

// Next returns the next patch in LSN order. It blocks until one is
// available or the subscription is closed and its buffer is drained.
func (s *Subscription) Next() (Patch, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.buf) == 0 && !s.closed {
		s.cond.Wait()
	}
	if len(s.buf) == 0 {
		return Patch{}, false
	}
	p := s.buf[0]
	s.buf = s.buf[1:]
	return p, true
}

// Close unregisters this subscription from the shard. Buffered patches
// remain drainable via Next before it returns (_, false).
func (s *Subscription) Close() {
	s.shard.mu.Lock()
	for i, sub := range s.shard.subs {
		if sub == s {
			s.shard.subs = append(s.shard.subs[:i], s.shard.subs[i+1:]...)
			break
		}
	}
	s.shard.mu.Unlock()

	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}
