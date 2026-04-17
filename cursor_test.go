package joindb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)

func TestCursorSnapshotThenPatches(t *testing.T) {
	s := NewShard()
	s.Put("a", []byte("1"))
	s.Put("b", []byte("2"))
	s.Put("c", []byte("3"))

	sub := s.Subscribe("", "\xff\xff")
	defer sub.Close()

	if len(sub.Snapshot) != 3 {
		t.Fatalf("snapshot len %d want 3", len(sub.Snapshot))
	}
	if sub.StartLSN != s.LSN()+1 {
		t.Fatalf("StartLSN = %d; LSN = %d", sub.StartLSN, s.LSN())
	}

	s.Put("d", []byte("4"))
	p, ok := sub.Next()
	if !ok {
		t.Fatal("expected patch")
	}
	if p.Key != "d" || p.Kind != PatchInsert || !bytes.Equal(p.Value, []byte("4")) {
		t.Fatalf("unexpected patch: %+v", p)
	}

	s.Put("d", []byte("4b"))
	p, _ = sub.Next()
	if p.Kind != PatchUpdate || !bytes.Equal(p.Value, []byte("4b")) {
		t.Fatalf("expected update patch, got %+v", p)
	}

	s.Delete("d")
	p, _ = sub.Next()
	if p.Kind != PatchDelete || p.Key != "d" || p.Value != nil {
		t.Fatalf("expected delete patch, got %+v", p)
	}
}

func TestCursorRangeFilter(t *testing.T) {
	s := NewShard()
	sub := s.Subscribe("m", "s")

	// out of range — should not appear in patches
	s.Put("a", []byte("a"))
	s.Put("z", []byte("z"))
	s.Put("s", []byte("s")) // hi is exclusive
	s.Put("t", []byte("t"))
	// in range
	s.Put("m", []byte("m"))
	s.Put("r", []byte("r"))

	sub.Close()

	var seen []string
	for {
		p, ok := sub.Next()
		if !ok {
			break
		}
		seen = append(seen, p.Key)
	}
	if !eqStrings(seen, []string{"m", "r"}) {
		t.Fatalf("seen = %v; want [m r]", seen)
	}
}

func TestCursorSnapshotReflectsPreSubscribeState(t *testing.T) {
	s := NewShard()
	s.Put("a", []byte("1"))
	sub := s.Subscribe("", "\xff\xff")
	defer sub.Close()

	// write after subscribe — shouldn't appear in snapshot
	s.Put("b", []byte("2"))

	if len(sub.Snapshot) != 1 || sub.Snapshot[0].Key != "a" {
		t.Fatalf("snapshot = %v", sub.Snapshot)
	}
}

// TestCursorConcurrentNoMissNoDupe is the headline M4 property: under
// concurrent writers, applying Snapshot + every Next() patch yields a
// view equal to the source's range as of the last observed LSN. No
// missed rows, no duplicates, strict LSN order.
func TestCursorConcurrentNoMissNoDupe(t *testing.T) {
	s := NewShard()
	for i := range 100 {
		s.Put(fmt.Sprintf("k%04d", i), fmt.Appendf(nil, "v%d", i))
	}

	sub := s.Subscribe("", "\xff\xff")

	// Apply snapshot to a local map up front.
	local := make(map[string][]byte, len(sub.Snapshot))
	for _, kv := range sub.Snapshot {
		v := make([]byte, len(kv.Value))
		copy(v, kv.Value)
		local[kv.Key] = v
	}

	// Drain patches concurrently so the subscription buffer stays bounded.
	var lastLSN uint64 = sub.StartLSN - 1
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			p, ok := sub.Next()
			if !ok {
				return
			}
			if p.LSN <= lastLSN {
				t.Errorf("non-monotonic LSN: got %d after %d", p.LSN, lastLSN)
				return
			}
			lastLSN = p.LSN
			switch p.Kind {
			case PatchInsert, PatchUpdate:
				local[p.Key] = p.Value
			case PatchDelete:
				delete(local, p.Key)
			}
		}
	}()

	// Two concurrent writers doing 500 ops each.
	var wg sync.WaitGroup
	writer := func(seed uint64) {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(seed, seed+1))
		for i := range 500 {
			k := fmt.Sprintf("k%04d", rng.IntN(200))
			if rng.IntN(3) == 0 {
				s.Delete(k)
			} else {
				s.Put(k, fmt.Appendf(nil, "w%d-%d", seed, i))
			}
		}
	}
	wg.Add(2)
	go writer(1)
	go writer(2)
	wg.Wait()

	sub.Close()
	<-done

	srcRange := s.Range("", "\xff\xff")
	src := make(map[string][]byte, len(srcRange))
	for _, kv := range srcRange {
		src[kv.Key] = kv.Value
	}

	if len(local) != len(src) {
		t.Fatalf("local has %d entries, src has %d", len(local), len(src))
	}
	for k, v := range src {
		lv, ok := local[k]
		if !ok {
			t.Fatalf("local missing key %q", k)
		}
		if !bytes.Equal(lv, v) {
			t.Fatalf("key %q: local=%q src=%q", k, lv, v)
		}
	}
	if lastLSN != s.LSN() {
		t.Fatalf("lastLSN %d != shard LSN %d (missing patches)", lastLSN, s.LSN())
	}
}

func TestCursorCloseWhileBlocked(t *testing.T) {
	s := NewShard()
	sub := s.Subscribe("", "\xff")

	// Next() must unblock when Close is called.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, ok := sub.Next()
		if ok {
			t.Error("expected Next to return ok=false after Close")
		}
	}()
	sub.Close()
	<-done
}
