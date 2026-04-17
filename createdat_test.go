package joindb

import (
	"testing"
	"time"
)

// TestCreatedAtPreservedAcrossUpdates: updates must not move the
// createdAt forward. Only the first insert of a live instance fixes it.
func TestCreatedAtPreservedAcrossUpdates(t *testing.T) {
	s := NewShardID("A")
	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	s.SetClock(fc.now)

	s.Put("k", []byte("v1")) // T0
	s.Put("k", []byte("v2")) // T1
	s.Put("k", []byte("v3")) // T2

	ents := s.Entries()
	if len(ents) != 1 {
		t.Fatalf("want 1 entry; got %d", len(ents))
	}
	e := ents[0]
	if !e.CreatedAt.Equal(fc.base) {
		t.Fatalf("CreatedAt should be T0=%v; got %v", fc.base, e.CreatedAt)
	}
	if !e.ModTime.Equal(fc.base.Add(2 * time.Second)) {
		t.Fatalf("ModTime should be T2; got %v", e.ModTime)
	}
}

// TestCreatedAtResetAfterDeleteReinsert: a delete wipes the live
// instance; a subsequent Put starts a fresh instance and must ratchet
// createdAt forward to the insert's clock.
func TestCreatedAtResetAfterDeleteReinsert(t *testing.T) {
	s := NewShardID("A")
	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	s.SetClock(fc.now)

	s.Put("k", []byte("v1")) // T0 — createdAt = T0
	s.Delete("k")            // T1
	s.Put("k", []byte("v2")) // T2 — createdAt should be T2, not T0

	ents := s.Entries()
	var live StampedKV
	for _, e := range ents {
		if e.Key == "k" && !e.Tombstone {
			live = e
		}
	}
	if live.Key == "" {
		t.Fatalf("expected live entry for k; got %+v", ents)
	}
	want := fc.base.Add(2 * time.Second)
	if !live.CreatedAt.Equal(want) {
		t.Fatalf("CreatedAt should reset to T2=%v after delete+reinsert; got %v", want, live.CreatedAt)
	}
}

// TestCreatedAtPropagatedAcrossBridge: B's copy of a row created on A
// must carry A's createdAt, so a cursor on B filtering by creation time
// sees the same result set as one on A.
func TestCreatedAtPropagatedAcrossBridge(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")

	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	a.SetClock(fc.now)

	a.Put("k", []byte("v1"))

	// Give the bridge a moment to replicate.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := b.Get("k"); ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	var aCA, bCA time.Time
	for _, e := range a.Entries() {
		if e.Key == "k" {
			aCA = e.CreatedAt
		}
	}
	for _, e := range b.Entries() {
		if e.Key == "k" {
			bCA = e.CreatedAt
		}
	}
	if aCA.IsZero() {
		t.Fatalf("A missing createdAt")
	}
	if !aCA.Equal(bCA) {
		t.Fatalf("B's createdAt should match A's; A=%v B=%v", aCA, bCA)
	}
}

// TestCreatedAtLWWWinnerPrevails: concurrent writes on A and B for the
// same key resolve by LWW; the winner's createdAt is what both sides
// converge on (not a min-register).
func TestCreatedAtLWWWinnerPrevails(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")

	// Write on each side *before* bridging, so the two writes race.
	// Clocks: A at T0, B at T100. Whichever stamp dominates is the
	// canonical createdAt on both shards after convergence.
	fa := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	fb := &fakeClock{base: time.Unix(1_700_000_100, 0), step: time.Second}
	a.SetClock(fa.now)
	b.SetClock(fb.now)

	a.Put("k", []byte("A"))
	b.Put("k", []byte("B"))

	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		av, _ := a.Get("k")
		bv, _ := b.Get("k")
		if string(av) == string(bv) && av != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	var aCA, bCA time.Time
	for _, e := range a.Entries() {
		if e.Key == "k" {
			aCA = e.CreatedAt
		}
	}
	for _, e := range b.Entries() {
		if e.Key == "k" {
			bCA = e.CreatedAt
		}
	}
	if !aCA.Equal(bCA) {
		t.Fatalf("createdAt should converge; A=%v B=%v", aCA, bCA)
	}
}
