package joindb

import (
	"bytes"
	"sort"
	"sync/atomic"
	"testing"
)

// countingStore wraps a NodeStore and counts Get calls + bytes returned,
// to measure how much work a Sync actually does against the source.
type countingStore struct {
	inner    NodeStore
	gets     atomic.Int64
	getBytes atomic.Int64
}

func newCountingStore(inner NodeStore) *countingStore {
	return &countingStore{inner: inner}
}

func (c *countingStore) Get(h Hash) ([]byte, bool) {
	v, ok := c.inner.Get(h)
	if ok {
		c.gets.Add(1)
		c.getBytes.Add(int64(len(v)))
	}
	return v, ok
}
func (c *countingStore) Put(h Hash, data []byte) { c.inner.Put(h, data) }
func (c *countingStore) Has(h Hash) bool         { return c.inner.Has(h) }
func (c *countingStore) Len() int                { return c.inner.Len() }

func TestSyncInitial(t *testing.T) {
	src := NewMemStore()
	pairs := makeSortedPairs(2000)
	srcIdx := BuildIndex(pairs, src)

	local := NewMemStore()
	counter := newCountingStore(src)
	stats, err := Sync(local, counter, srcIdx.Root)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksFetched != src.Len() {
		t.Fatalf("initial sync fetched %d chunks; source has %d", stats.ChunksFetched, src.Len())
	}
	if counter.gets.Load() != int64(stats.ChunksFetched) {
		t.Fatalf("counter disagrees with stats: gets=%d chunks=%d",
			counter.gets.Load(), stats.ChunksFetched)
	}

	// replica should answer queries identically to source
	replica := &Index{Root: srcIdx.Root, Store: local, Bits: srcIdx.Bits}
	for _, kv := range pairs {
		v, ok := replica.Get(kv.Key)
		if !ok || !bytes.Equal(v, kv.Value) {
			t.Fatalf("replica Get(%q) = %q, %v", kv.Key, v, ok)
		}
	}
	t.Logf("initial: %d chunks, %d bytes", stats.ChunksFetched, stats.BytesFetched)
}

// The headline M3 property: after one mutation upstream, a re-sync
// transfers work proportional to changed chunks (~tree depth), not to
// total table size.
func TestSyncDelta(t *testing.T) {
	src := NewMemStore()
	pairs := makeSortedPairs(2000)
	srcIdx := BuildIndex(pairs, src)

	local := NewMemStore()
	initial, err := Sync(local, src, srcIdx.Root)
	if err != nil {
		t.Fatal(err)
	}

	// Mutate source: add one key, rebuild index.
	p2 := append(append([]KV(nil), pairs...), KV{Key: "zzz-new-key", Value: []byte("n")})
	sort.Slice(p2, func(i, j int) bool { return p2[i].Key < p2[j].Key })
	newIdx := BuildIndex(p2, src)

	counter := newCountingStore(src)
	delta, err := Sync(local, counter, newIdx.Root)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("initial: %d chunks / %d bytes; delta: %d chunks / %d bytes (src has %d total)",
		initial.ChunksFetched, initial.BytesFetched,
		delta.ChunksFetched, delta.BytesFetched, src.Len())

	if delta.ChunksFetched > 12 {
		t.Fatalf("delta sync fetched %d chunks; expected ≤12 (path to root)",
			delta.ChunksFetched)
	}
	if delta.ChunksFetched >= initial.ChunksFetched {
		t.Fatalf("delta (%d) not smaller than initial (%d)",
			delta.ChunksFetched, initial.ChunksFetched)
	}

	// Replica still correct end-to-end: all old keys + new key present.
	replica := &Index{Root: newIdx.Root, Store: local, Bits: newIdx.Bits}
	for _, kv := range p2 {
		v, ok := replica.Get(kv.Key)
		if !ok || !bytes.Equal(v, kv.Value) {
			t.Fatalf("replica Get(%q) = %q, %v", kv.Key, v, ok)
		}
	}
}

func TestSyncEmptyRoot(t *testing.T) {
	local := NewMemStore()
	src := NewMemStore()
	stats, err := Sync(local, src, Hash{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksFetched != 0 {
		t.Fatalf("empty-root sync fetched %d chunks", stats.ChunksFetched)
	}
}

func TestSyncAlreadyUpToDate(t *testing.T) {
	src := NewMemStore()
	pairs := makeSortedPairs(500)
	srcIdx := BuildIndex(pairs, src)

	local := NewMemStore()
	if _, err := Sync(local, src, srcIdx.Root); err != nil {
		t.Fatal(err)
	}
	counter := newCountingStore(src)
	stats, err := Sync(local, counter, srcIdx.Root)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksFetched != 0 {
		t.Fatalf("re-sync of unchanged root fetched %d chunks", stats.ChunksFetched)
	}
	if counter.gets.Load() != 0 {
		t.Fatalf("counter saw %d gets on no-op sync", counter.gets.Load())
	}
}
