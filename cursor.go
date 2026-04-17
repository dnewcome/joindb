package joindb

import "sync"

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
	LSN   uint64
	Kind  PatchKind
	Key   string
	Value []byte // nil for PatchDelete
	Stamp Stamp
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
