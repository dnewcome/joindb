package joindb

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// fakeClock returns successive, monotonically-increasing times so tests
// are deterministic.
type fakeClock struct {
	base time.Time
	step time.Duration
	next int
}

func (f *fakeClock) now() time.Time {
	t := f.base.Add(time.Duration(f.next) * f.step)
	f.next++
	return t
}

func TestModifiedBasicRange(t *testing.T) {
	s := NewShardID("A")
	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	s.SetClock(fc.now)

	// Writes at T=0s, 1s, 2s, ...
	s.Put("a", []byte("1")) // T0
	s.Put("b", []byte("2")) // T1
	s.Put("c", []byte("3")) // T2
	s.Put("d", []byte("4")) // T3
	s.Put("e", []byte("5")) // T4

	tLo := fc.base.Add(1 * time.Second)
	tHi := fc.base.Add(4 * time.Second)
	got := s.Modified("", rangeMaxKey, tLo, tHi)
	if len(got) != 3 {
		t.Fatalf("want 3 results in [T1,T4); got %d (%+v)", len(got), got)
	}
	wantKeys := []string{"b", "c", "d"}
	for i, m := range got {
		if m.Key != wantKeys[i] {
			t.Fatalf("pos %d: got %q, want %q", i, m.Key, wantKeys[i])
		}
	}
}

func TestModifiedKeyAndTimeIntersection(t *testing.T) {
	s := NewShardID("A")
	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	s.SetClock(fc.now)

	// Seed 10 keys across 10 seconds.
	for i := range 10 {
		s.Put(fmt.Sprintf("k%02d", i), fmt.Appendf(nil, "v%d", i))
	}

	// Query: keys [k03, k07) AND time [T2, T8)
	tLo := fc.base.Add(2 * time.Second)
	tHi := fc.base.Add(8 * time.Second)
	got := s.Modified("k03", "k07", tLo, tHi)

	wantKeys := []string{"k03", "k04", "k05", "k06"}
	if len(got) != len(wantKeys) {
		t.Fatalf("want %v; got %+v", wantKeys, got)
	}
	for i, m := range got {
		if m.Key != wantKeys[i] {
			t.Fatalf("pos %d: got %q want %q", i, m.Key, wantKeys[i])
		}
	}
}

// TestModifiedUpdateReplacesOldEntry: updating a key should remove its
// previous (modTime, key) tuple from the secondary index so the old
// timestamp no longer matches a range query.
func TestModifiedUpdateReplacesOldEntry(t *testing.T) {
	s := NewShardID("A")
	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	s.SetClock(fc.now)

	s.Put("k", []byte("v1")) // T0
	s.Put("k", []byte("v2")) // T1 — should replace the T0 entry

	// Search [T0, T0+1ns): nothing should match, because the T0 entry
	// was displaced when we wrote v2.
	tLo := fc.base
	tHi := fc.base.Add(time.Nanosecond)
	if got := s.Modified("", rangeMaxKey, tLo, tHi); len(got) != 0 {
		t.Fatalf("T0 entry should be gone; got %+v", got)
	}
	// Search including T1: should see v2.
	tHi = fc.base.Add(2 * time.Second)
	got := s.Modified("", rangeMaxKey, tLo, tHi)
	if len(got) != 1 || !bytes.Equal(got[0].Value, []byte("v2")) {
		t.Fatalf("want single v2; got %+v", got)
	}
}

// TestModifiedDeleteDropsFromTimeIndex: after a Delete the key is not
// returned by a time-range query, even one covering the original Put.
func TestModifiedDeleteDropsFromTimeIndex(t *testing.T) {
	s := NewShardID("A")
	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	s.SetClock(fc.now)

	s.Put("k", []byte("v")) // T0
	s.Delete("k")           // T1

	tLo := fc.base.Add(-time.Hour)
	tHi := fc.base.Add(time.Hour)
	if got := s.Modified("", rangeMaxKey, tLo, tHi); len(got) != 0 {
		t.Fatalf("deleted key must not appear; got %+v", got)
	}
}

// TestModifiedEmptyQueries: guard rails around empty ranges.
func TestModifiedEmptyQueries(t *testing.T) {
	s := NewShardID("A")
	s.Put("a", []byte("1"))
	if got := s.Modified("b", "a", time.Time{}, time.Now()); got != nil {
		t.Fatalf("inverted key range must yield nil")
	}
	now := time.Now()
	if got := s.Modified("", rangeMaxKey, now, now); got != nil {
		t.Fatalf("tLo==tHi must yield nil")
	}
}

// TestCursorModifiedAtomicAndLive: CursorModified's snapshot is consistent
// with StartLSN. Writes committed before Start appear in Snapshot (if
// their modTime falls in the window); writes from StartLSN onward
// arrive via Sub.Next. No gaps, no dupes even with concurrent writers.
func TestCursorModifiedAtomicAndLive(t *testing.T) {
	s := NewShardID("A")
	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	s.SetClock(fc.now)

	// Seed 5 keys at T0..T4.
	for i := range 5 {
		s.Put(fmt.Sprintf("k%d", i), fmt.Appendf(nil, "v%d", i))
	}

	// Cursor window [T2, T10): should pick up k2, k3, k4 at snapshot.
	tLo := fc.base.Add(2 * time.Second)
	tHi := fc.base.Add(10 * time.Second)
	cur := s.CursorModified("", rangeMaxKey, tLo, tHi)
	defer cur.Sub.Close()

	if len(cur.Snapshot) != 3 {
		t.Fatalf("snapshot should have 3 entries; got %d", len(cur.Snapshot))
	}

	// Writes after CursorModified: k5 at T5 (in window), k6 at T6.
	s.Put("k5", []byte("v5")) // T5
	s.Put("k6", []byte("v6")) // T6

	drained := []Patch{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 2 {
			p, ok := cur.Sub.Next()
			if !ok {
				return
			}
			drained = append(drained, p)
		}
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for cursor patches")
	}

	if drained[0].Key != "k5" || drained[1].Key != "k6" {
		t.Fatalf("expected k5 then k6; got %+v", drained)
	}
	if drained[0].LSN >= drained[1].LSN {
		t.Fatalf("LSNs must increase: %+v", drained)
	}
	if !drained[0].ModTime.Equal(fc.base.Add(5*time.Second)) {
		t.Fatalf("expected k5 modTime T5, got %v", drained[0].ModTime)
	}
}

// TestCursorModifiedBidiRollingWindow: cursor on node B rolls forward as
// A's writes propagate through the bridge. Each applied patch arrives
// on B's cursor in stamp-consistent order with its origin ModTime.
func TestCursorModifiedBidiRollingWindow(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")
	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	a.SetClock(fc.now)

	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	// Open cursor on B covering all time.
	cur := b.CursorModified("", rangeMaxKey, time.Time{}, time.Time{}.Add(1))
	defer cur.Sub.Close()
	// Note: tLo==tHi is the empty-range guard; use a wide window instead.
	cur = b.CursorModified("", rangeMaxKey, fc.base.Add(-time.Hour), fc.base.Add(time.Hour))
	defer cur.Sub.Close()

	a.Put("alpha", []byte("1"))
	a.Put("beta", []byte("2"))

	got := []Patch{}
	deadline := time.Now().Add(500 * time.Millisecond)
	for len(got) < 2 && time.Now().Before(deadline) {
		p, ok := cur.Sub.Next()
		if !ok {
			break
		}
		got = append(got, p)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 patches through bridge; got %+v", got)
	}
	// ModTime on B's cursor reflects A's clock (origin timestamp).
	if !got[0].ModTime.Equal(fc.base) {
		t.Fatalf("expected origin modtime %v, got %v", fc.base, got[0].ModTime)
	}
}

// TestModifiedBidiPreservesOriginTimestamp: when Apply accepts a remote
// patch, the local time index reflects the *remote* ModTime (the
// canonical moment of the winning write), not the local receive time.
// Query from either side should return the same ModTime.
func TestModifiedBidiPreservesOriginTimestamp(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")

	// A uses wall-clock; B uses a clock set far in the future. If Apply
	// were to re-timestamp with B's clock, the two sides would disagree.
	aClock := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	bClock := &fakeClock{base: time.Unix(2_000_000_000, 0), step: time.Second}
	a.SetClock(aClock.now)
	b.SetClock(bClock.now)

	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	a.Put("shared", []byte("from-A"))
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := b.Get("shared"); ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	aMT := a.ModTime("shared")
	bMT := b.ModTime("shared")
	if !aMT.Equal(bMT) {
		t.Fatalf("ModTime mismatch across bidi: A=%v B=%v", aMT, bMT)
	}
	// Sanity: the preserved time is A's, not B's future clock.
	if aMT.Year() > 2030 {
		t.Fatalf("B's clock leaked into A's time index: %v", aMT)
	}
}
