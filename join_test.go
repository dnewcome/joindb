package joindb

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

// Seed two shards with partially overlapping keys. A has k0000..k0999,
// B has k0500..k1499; 500 keys overlap.
func seedJoinShards() (*Shard, *Shard) {
	a := NewShard()
	b := NewShard()
	for i := range 1000 {
		a.Put(fmt.Sprintf("k%04d", i), fmt.Appendf(nil, "a-%d", i))
	}
	for i := 500; i < 1500; i++ {
		b.Put(fmt.Sprintf("k%04d", i), fmt.Appendf(nil, "b-%d", i))
	}
	return a, b
}

func TestJoinNaiveOneRPCPerKey(t *testing.T) {
	a, b := seedJoinShards()
	aServer := NewServer(a)
	bPairs := b.Range("", rangeMaxKey)

	results := InnerJoinKeys(bPairs, nil, aServer)
	if len(results) != 500 {
		t.Fatalf("results: %d want 500", len(results))
	}
	if aServer.GetCalls != len(bPairs) {
		t.Fatalf("Get calls: %d want %d", aServer.GetCalls, len(bPairs))
	}
}

func TestJoinPushdownZeroQueryRPCs(t *testing.T) {
	a, b := seedJoinShards()
	aServer := NewServer(a)

	// Publish A's index and sync it to a local replica store.
	aServer.PublishIndex()
	replicaStore := NewMemStore()
	syncStats, err := Sync(replicaStore, aServer.NodeStoreView(), aServer.IdxRoot)
	if err != nil {
		t.Fatal(err)
	}
	aLocal := &Index{Root: aServer.IdxRoot, Store: replicaStore, Bits: aServer.IdxBits}

	// All sync traffic counted above; query phase must be silent.
	aServer.ResetCounts()

	bPairs := b.Range("", rangeMaxKey)
	results := InnerJoinKeys(bPairs, aLocal, aServer)
	if len(results) != 500 {
		t.Fatalf("results: %d want 500", len(results))
	}
	if aServer.GetCalls != 0 {
		t.Fatalf("pushdown issued %d Get calls; want 0", aServer.GetCalls)
	}
	if aServer.RangeCalls != 0 {
		t.Fatalf("pushdown issued %d Range calls; want 0", aServer.RangeCalls)
	}
	if aServer.NodeCalls != 0 {
		t.Fatalf("pushdown issued %d Node calls at query time; want 0", aServer.NodeCalls)
	}

	// Spot-check correctness.
	sort.Slice(results, func(i, j int) bool { return results[i].Key < results[j].Key })
	if results[0].Key != "k0500" || results[len(results)-1].Key != "k0999" {
		t.Fatalf("boundary keys: first=%s last=%s", results[0].Key, results[len(results)-1].Key)
	}
	for _, r := range results {
		var i int
		if _, err := fmt.Sscanf(r.Key, "k%d", &i); err != nil {
			t.Fatalf("parse key %s: %v", r.Key, err)
		}
		want := fmt.Appendf(nil, "a-%d", i)
		if !bytes.Equal(r.ValueA, want) {
			t.Fatalf("A value mismatch on %s: got %q want %q", r.Key, r.ValueA, want)
		}
	}

	t.Logf("sync (amortized): %d chunks / %d bytes. query: 0 remote calls for %d joins.",
		syncStats.ChunksFetched, syncStats.BytesFetched, len(results))
}

// A mutation upstream costs a small re-sync; subsequent joins still hit
// zero remote RPCs. This is the "extended DB node" story: sync is
// incremental and cheap, queries stay local.
func TestJoinPushdownSurvivesMutation(t *testing.T) {
	a, b := seedJoinShards()
	aServer := NewServer(a)

	aServer.PublishIndex()
	replicaStore := NewMemStore()
	if _, err := Sync(replicaStore, aServer.NodeStoreView(), aServer.IdxRoot); err != nil {
		t.Fatal(err)
	}

	// Mutate A: insert a key B also has.
	a.Put("k0750", []byte("a-mutated"))
	aServer.PublishIndex()

	aServer.ResetCounts()
	delta, err := Sync(replicaStore, aServer.NodeStoreView(), aServer.IdxRoot)
	if err != nil {
		t.Fatal(err)
	}
	if delta.ChunksFetched > 12 {
		t.Fatalf("delta re-sync fetched %d chunks; expected small", delta.ChunksFetched)
	}

	aLocal := &Index{Root: aServer.IdxRoot, Store: replicaStore, Bits: aServer.IdxBits}
	aServer.ResetCounts()

	results := InnerJoinKeys(b.Range("", rangeMaxKey), aLocal, aServer)
	if aServer.GetCalls != 0 || aServer.NodeCalls != 0 {
		t.Fatalf("post-sync query: Get=%d Node=%d; want 0,0",
			aServer.GetCalls, aServer.NodeCalls)
	}

	// Find the mutated key and confirm the replica sees the new value.
	var seen bool
	for _, r := range results {
		if r.Key == "k0750" {
			seen = true
			if !bytes.Equal(r.ValueA, []byte("a-mutated")) {
				t.Fatalf("stale value on k0750: %q", r.ValueA)
			}
		}
	}
	if !seen {
		t.Fatal("k0750 missing from join results")
	}
}
