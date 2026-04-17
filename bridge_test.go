package joindb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// TestBridgeBasicPropagation: a write on A appears on B, and vice versa.
func TestBridgeBasicPropagation(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")
	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	a.Put("alpha", []byte("1"))
	waitLSN(t, b, 1)

	if v, ok := b.Get("alpha"); !ok || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("B.alpha = %q, %v", v, ok)
	}

	b.Put("beta", []byte("2"))
	waitLSN(t, a, 2)
	if v, ok := a.Get("beta"); !ok || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("A.beta = %q, %v", v, ok)
	}
}

// TestBridgeLoopPrevention: after a write propagates A→B, the echo back
// B→A is dropped (stamp origin match, or stamp equality). Neither side
// sees its own write reapplied.
func TestBridgeLoopPrevention(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")
	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	a.Put("k", []byte("v"))
	// Give the bridge time to fully settle both directions.
	waitLSN(t, b, 1)
	time.Sleep(20 * time.Millisecond)

	s := br.Stats()
	if s.AtoB_Applied != 1 {
		t.Fatalf("expected 1 A→B applied, got %+v", s)
	}
	if s.BtoA_Applied != 0 {
		t.Fatalf("expected 0 B→A applied (echo should be filtered), got %+v", s)
	}
	// B's subscription will fire for B's applied patch (with stamp
	// origin A). The forwarder skips it without entering Apply, so
	// BtoA_Dropped should also be 0.
	if s.BtoA_Dropped != 0 {
		t.Fatalf("echo should be filtered, not dropped: %+v", s)
	}
}

// TestBridgeLWWConflict: A and B concurrently write the same key before
// the bridge starts syncing. Once bridged, both sides converge to the
// write with the dominating Stamp (higher Lamport, tie broken by ID).
func TestBridgeLWWConflict(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")

	// Both bump their local Lamport identically.
	a.Put("k", []byte("from-A"))
	b.Put("k", []byte("from-B"))
	if a.Lamport() != 1 || b.Lamport() != 1 {
		t.Fatalf("expected both at lamport 1, got a=%d b=%d", a.Lamport(), b.Lamport())
	}

	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	// Drive each side once more so the bridge sees both writes through
	// its post-Start subscriptions. The second write on each side
	// bumps Lamport to 2.
	a.Put("k", []byte("from-A2"))
	b.Put("k", []byte("from-B2"))

	// Both writes at Lamport=2; B's origin > A's origin, so B wins.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		va, oka := a.Get("k")
		vb, okb := b.Get("k")
		if oka && okb && bytes.Equal(va, vb) {
			if !bytes.Equal(va, []byte("from-B2")) {
				t.Fatalf("converged on %q; expected from-B2 (tiebreak)", va)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	va, _ := a.Get("k")
	vb, _ := b.Get("k")
	t.Fatalf("did not converge: A=%q B=%q", va, vb)
}

// TestBridgeConcurrentWriters: hammer both sides with random writes and
// deletes on overlapping key spaces. After quiescence, A and B agree on
// every key's value *and* last-writer stamp.
func TestBridgeConcurrentWriters(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")
	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	var wg sync.WaitGroup
	writer := func(s *Shard, seed uint64) {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(seed, seed+1))
		for i := range 500 {
			k := fmt.Sprintf("k%03d", rng.IntN(40))
			if rng.IntN(4) == 0 {
				s.Delete(k)
			} else {
				s.Put(k, fmt.Appendf(nil, "%s-%d", s.ID(), i))
			}
		}
	}
	wg.Add(2)
	go writer(a, 11)
	go writer(b, 22)
	wg.Wait()

	// Let the bridge drain all subscription buffers.
	if !waitStable(a, b, 2*time.Second) {
		t.Fatalf("did not stabilize: A.lsn=%d B.lsn=%d A.lamport=%d B.lamport=%d",
			a.LSN(), b.LSN(), a.Lamport(), b.Lamport())
	}

	aPairs := a.Range("", rangeMaxKey)
	bPairs := b.Range("", rangeMaxKey)
	if len(aPairs) != len(bPairs) {
		t.Fatalf("len: A=%d B=%d", len(aPairs), len(bPairs))
	}
	for i := range aPairs {
		if aPairs[i].Key != bPairs[i].Key || !bytes.Equal(aPairs[i].Value, bPairs[i].Value) {
			t.Fatalf("diff @ %d: A=%+v B=%+v", i, aPairs[i], bPairs[i])
		}
		if a.Stamp(aPairs[i].Key) != b.Stamp(bPairs[i].Key) {
			t.Fatalf("stamp diff on %s: A=%s B=%s",
				aPairs[i].Key, a.Stamp(aPairs[i].Key), b.Stamp(bPairs[i].Key))
		}
	}
}

// TestBridgePartitionRestore: two shards drift while disconnected, then
// a fresh Bridge.Start() bootstrap-replicates both directions and LWW
// resolves conflicting keys.
func TestBridgePartitionRestore(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")

	// Simulated partition: no bridge. Each side writes independently.
	a.Put("only-A", []byte("from-A"))
	b.Put("only-B", []byte("from-B"))
	// Same key written on both; B wins on Lamport tie-break since "B">"A".
	a.Put("conflict", []byte("A-wrote"))
	b.Put("conflict", []byte("B-wrote"))
	// B writes again so its conflict stamp has a higher Lamport too.
	b.Put("conflict", []byte("B-wrote-again"))
	// A deletes a key B never saw.
	a.Put("gone-on-A", []byte("x"))
	a.Delete("gone-on-A")

	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	if !waitStable(a, b, 500*time.Millisecond) {
		t.Fatalf("didn't stabilize post-restore")
	}

	check := func(s *Shard, key, want string) {
		v, ok := s.Get(key)
		if !ok || !bytes.Equal(v, []byte(want)) {
			t.Fatalf("%s.Get(%q) = %q, %v; want %q", s.ID(), key, v, ok, want)
		}
	}
	check(a, "only-A", "from-A")
	check(b, "only-A", "from-A")
	check(a, "only-B", "from-B")
	check(b, "only-B", "from-B")
	check(a, "conflict", "B-wrote-again")
	check(b, "conflict", "B-wrote-again")
	if _, ok := a.Get("gone-on-A"); ok {
		t.Fatalf("tombstone should have propagated; A still has gone-on-A")
	}
	if _, ok := b.Get("gone-on-A"); ok {
		t.Fatalf("tombstone should have propagated; B never got it")
	}
}

func waitLSN(t *testing.T, s *Shard, target uint64) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if s.LSN() >= target {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s LSN >= %d (got %d)", s.ID(), target, s.LSN())
}

// waitStable polls until both shards' LSNs stop changing for a short
// interval, meaning all in-flight patches have drained through the
// bridge and back.
func waitStable(a, b *Shard, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	var lastA, lastB uint64
	stableSince := time.Time{}
	for time.Now().Before(deadline) {
		curA, curB := a.LSN(), b.LSN()
		if curA == lastA && curB == lastB {
			if stableSince.IsZero() {
				stableSince = time.Now()
			} else if time.Since(stableSince) > 50*time.Millisecond {
				return true
			}
		} else {
			stableSince = time.Time{}
		}
		lastA, lastB = curA, curB
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
