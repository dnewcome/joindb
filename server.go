package joindb

import "sync"

// Server models a remote shard node: it serves KV Get/Range and
// content-addressed index-node fetches, and records a call count for each
// so query planners (and tests) can see exactly how much remote work a
// query did.
//
// In v0 everything is in-process; M6 will put real sockets underneath.
type Server struct {
	Shard    *Shard
	IdxStore NodeStore // content-addressed store backing IdxRoot
	IdxRoot  Hash
	IdxBits  uint

	mu         sync.Mutex
	GetCalls   int
	RangeCalls int
	NodeCalls  int
}

func NewServer(s *Shard) *Server {
	return &Server{Shard: s, IdxStore: NewMemStore()}
}

// PublishIndex rebuilds the full-range index over the shard's data and
// stores its nodes in IdxStore. A replica can then sync by pulling from
// IdxRoot using NodeStoreView.
func (s *Server) PublishIndex() {
	pairs := s.Shard.Range("", rangeMaxKey)
	idx := BuildIndex(pairs, s.IdxStore)
	s.mu.Lock()
	s.IdxRoot = idx.Root
	s.IdxBits = idx.Bits
	s.mu.Unlock()
}

func (s *Server) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	s.GetCalls++
	s.mu.Unlock()
	return s.Shard.Get(key)
}

func (s *Server) Range(lo, hi string) []KV {
	s.mu.Lock()
	s.RangeCalls++
	s.mu.Unlock()
	return s.Shard.Range(lo, hi)
}

func (s *Server) GetNode(h Hash) ([]byte, bool) {
	s.mu.Lock()
	s.NodeCalls++
	s.mu.Unlock()
	return s.IdxStore.Get(h)
}

func (s *Server) ResetCounts() {
	s.mu.Lock()
	s.GetCalls = 0
	s.RangeCalls = 0
	s.NodeCalls = 0
	s.mu.Unlock()
}

// NodeStoreView returns a read-only NodeStore facade over this server's
// index store so Sync can pull from it. Counts every fetched node.
func (s *Server) NodeStoreView() NodeStore { return &serverNodeStore{s: s} }

type serverNodeStore struct{ s *Server }

func (v *serverNodeStore) Get(h Hash) ([]byte, bool) { return v.s.GetNode(h) }
func (v *serverNodeStore) Put(Hash, []byte)          { panic("remote store is read-only") }
func (v *serverNodeStore) Has(h Hash) bool           { return v.s.IdxStore.Has(h) }
func (v *serverNodeStore) Len() int                  { return v.s.IdxStore.Len() }

// rangeMaxKey is a sentinel upper bound greater than any realistic key
// in v0. Good enough until we introduce open-ended range queries.
const rangeMaxKey = "\xff\xff\xff\xff"
