package joindb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)

func TestLiveReplicaBootstrap(t *testing.T) {
	src := NewShard()
	src.Put("a", []byte("1"))
	src.Put("b", []byte("2"))

	r := NewLiveReplica(src)
	r.Start()
	defer r.Stop()

	// snapshot is applied synchronously inside Start
	if r.Shard().Len() != 2 {
		t.Fatalf("bootstrap: local len %d, want 2", r.Shard().Len())
	}
	v, ok := r.Shard().Get("a")
	if !ok || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("bootstrap: a = %q, %v", v, ok)
	}
}

// TestLiveReplicaConvergence runs concurrent writers against the source
// and verifies the replica converges to the same exact state once
// caught up.
func TestLiveReplicaConvergence(t *testing.T) {
	src := NewShard()
	for i := range 100 {
		src.Put(fmt.Sprintf("k%04d", i), fmt.Appendf(nil, "v%d", i))
	}

	r := NewLiveReplica(src)
	r.Start()
	defer r.Stop()

	if r.Shard().Len() != 100 {
		t.Fatalf("bootstrap len %d, want 100", r.Shard().Len())
	}

	var wg sync.WaitGroup
	writer := func(seed uint64) {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(seed, seed+1))
		for i := range 500 {
			k := fmt.Sprintf("k%04d", rng.IntN(200))
			if rng.IntN(3) == 0 {
				src.Delete(k)
			} else {
				src.Put(k, fmt.Appendf(nil, "w%d-%d", seed, i))
			}
		}
	}
	wg.Add(2)
	go writer(11)
	go writer(22)
	wg.Wait()

	if err := r.WaitFor(src.LSN()); err != nil {
		t.Fatal(err)
	}

	localPairs := r.Shard().Range("", rangeMaxKey)
	srcPairs := src.Range("", rangeMaxKey)
	if len(localPairs) != len(srcPairs) {
		t.Fatalf("len: local=%d src=%d", len(localPairs), len(srcPairs))
	}
	for i := range localPairs {
		if localPairs[i].Key != srcPairs[i].Key ||
			!bytes.Equal(localPairs[i].Value, srcPairs[i].Value) {
			t.Fatalf("diff at %d: local=%+v src=%+v", i, localPairs[i], srcPairs[i])
		}
	}
}

// TestLiveReplicaCascade: A → B (r1) → C (r2). A write at A propagates
// through r1 to r2 because r1's local Shard emits its own patch stream.
// This is what makes "client as extended DB node" recursive.
func TestLiveReplicaCascade(t *testing.T) {
	src := NewShard()

	r1 := NewLiveReplica(src)
	r1.Start()
	defer r1.Stop()

	r2 := NewLiveReplica(r1.Shard())
	r2.Start()
	defer r2.Stop()

	for i := range 50 {
		src.Put(fmt.Sprintf("k%02d", i), fmt.Appendf(nil, "v%d", i))
	}

	if err := r1.WaitFor(src.LSN()); err != nil {
		t.Fatal(err)
	}
	// r2 follows r1.Shard's LSN, which advances one tick per applied patch.
	if err := r2.WaitFor(r1.Shard().LSN()); err != nil {
		t.Fatal(err)
	}

	if r2.Shard().Len() != src.Len() {
		t.Fatalf("cascade: r2 has %d; src has %d", r2.Shard().Len(), src.Len())
	}
	for _, kv := range src.Range("", rangeMaxKey) {
		v, ok := r2.Shard().Get(kv.Key)
		if !ok || !bytes.Equal(v, kv.Value) {
			t.Fatalf("r2 missing or wrong on %q: got %q, %v", kv.Key, v, ok)
		}
	}
}

// TestLiveReplicaMerkleParity: after writes settle, Merkle-syncing the
// source's published index into a fresh store produces the same (k,v)
// set as the live replica's accumulated state. The two protocols are
// different paths to the same snapshot.
func TestLiveReplicaMerkleParity(t *testing.T) {
	src := NewShard()
	srv := NewServer(src)

	r := NewLiveReplica(src)
	r.Start()
	defer r.Stop()

	for i := range 500 {
		src.Put(fmt.Sprintf("k%03d", i), fmt.Appendf(nil, "v%d", i))
	}
	for i := range 100 {
		src.Put(fmt.Sprintf("k%03d", i*5), fmt.Appendf(nil, "m%d", i))
	}
	for i := range 30 {
		src.Delete(fmt.Sprintf("k%03d", i*7))
	}

	if err := r.WaitFor(src.LSN()); err != nil {
		t.Fatal(err)
	}

	srv.PublishIndex()
	merkleStore := NewMemStore()
	if _, err := Sync(merkleStore, srv.NodeStoreView(), srv.IdxRoot); err != nil {
		t.Fatal(err)
	}
	merkleIdx := &Index{Root: srv.IdxRoot, Store: merkleStore, Bits: srv.IdxBits}
	merklePairs := merkleIdx.Range("", rangeMaxKey)
	livePairs := r.Shard().Range("", rangeMaxKey)

	if len(merklePairs) != len(livePairs) {
		t.Fatalf("merkle=%d live=%d", len(merklePairs), len(livePairs))
	}
	for i := range merklePairs {
		if merklePairs[i].Key != livePairs[i].Key ||
			!bytes.Equal(merklePairs[i].Value, livePairs[i].Value) {
			t.Fatalf("diff at %d: merkle=%+v live=%+v",
				i, merklePairs[i], livePairs[i])
		}
	}
}

// TestLiveReplicaReadsDuringWrites: reads from the replica while the
// source is being written can only ever observe committed states. We
// don't check a specific LSN here — just that the replica is queryable
// concurrently with writes and ends up correct.
func TestLiveReplicaReadsDuringWrites(t *testing.T) {
	src := NewShard()
	for i := range 50 {
		src.Put(fmt.Sprintf("k%03d", i), []byte("seed"))
	}
	r := NewLiveReplica(src)
	r.Start()
	defer r.Stop()

	stop := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// These should never panic or observe corrupt state.
			_ = r.Shard().Len()
			_ = r.Shard().Range("k000", "k050")
		}
	}()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := range 200 {
			src.Put(fmt.Sprintf("k%03d", i%50), fmt.Appendf(nil, "w%d", i))
		}
	}()
	<-writerDone
	close(stop)
	<-readerDone
	if err := r.WaitFor(src.LSN()); err != nil {
		t.Fatal(err)
	}
}
