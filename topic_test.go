package joindb

import (
	"bytes"
	"testing"
	"time"
)

// drainTopic pulls up to want events off sub (with a short per-event
// deadline) and returns what it got.
func drainTopic(t *testing.T, sub *TopicSub, want int) []TopicEvent {
	t.Helper()
	got := []TopicEvent{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range want {
			e, ok := sub.Next()
			if !ok {
				return
			}
			got = append(got, e)
		}
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for %d events; got %d: %+v", want, len(got), got)
	}
	return got
}

// TestTopicBasicEnterUpdate: Put a key under topic X emits Enter;
// Put again under X emits Update; Delete emits Leave.
func TestTopicBasicEnterUpdate(t *testing.T) {
	s := NewShardID("A")
	cur := s.CursorTopic("news", time.Time{}, time.Time{})
	defer cur.Sub.Close()

	s.PutTopic("post1", []byte("v1"), "news")
	s.PutTopic("post1", []byte("v2"), "news")
	s.Delete("post1")

	evs := drainTopic(t, cur.Sub, 3)
	if len(evs) != 3 {
		t.Fatalf("want 3 events; got %d: %+v", len(evs), evs)
	}
	if evs[0].Kind != TopicEnter || !bytes.Equal(evs[0].Value, []byte("v1")) {
		t.Fatalf("ev[0]: %+v", evs[0])
	}
	if evs[1].Kind != TopicUpdate || !bytes.Equal(evs[1].Value, []byte("v2")) {
		t.Fatalf("ev[1]: %+v", evs[1])
	}
	if evs[2].Kind != TopicLeave || evs[2].Value != nil {
		t.Fatalf("ev[2]: %+v", evs[2])
	}
	if evs[0].LSN >= evs[1].LSN || evs[1].LSN >= evs[2].LSN {
		t.Fatalf("LSNs must be strictly increasing: %+v", evs)
	}
}

// TestTopicFlipLeaveEnter: retagging a key from topic X to topic Y
// emits a single Leave on X and a single Enter on Y at the same LSN.
// This is the materialized-view maintenance invariant: the result set
// of "topic == X" equals what a fresh query would return.
func TestTopicFlipLeaveEnter(t *testing.T) {
	s := NewShardID("A")
	newsCur := s.CursorTopic("news", time.Time{}, time.Time{})
	defer newsCur.Sub.Close()
	sportsCur := s.CursorTopic("sports", time.Time{}, time.Time{})
	defer sportsCur.Sub.Close()

	s.PutTopic("post1", []byte("v1"), "news")
	s.PutTopic("post1", []byte("v2"), "sports")

	newsEvs := drainTopic(t, newsCur.Sub, 2)
	sportsEvs := drainTopic(t, sportsCur.Sub, 1)

	if newsEvs[0].Kind != TopicEnter {
		t.Fatalf("news[0] should be Enter, got %+v", newsEvs[0])
	}
	if newsEvs[1].Kind != TopicLeave {
		t.Fatalf("news[1] should be Leave on retag, got %+v", newsEvs[1])
	}
	if sportsEvs[0].Kind != TopicEnter || !bytes.Equal(sportsEvs[0].Value, []byte("v2")) {
		t.Fatalf("sports[0] should be Enter v2, got %+v", sportsEvs[0])
	}
	// The Leave on news and the Enter on sports were produced by the
	// same write, so they share an LSN.
	if newsEvs[1].LSN != sportsEvs[0].LSN {
		t.Fatalf("flip should be atomic; news Leave LSN=%d, sports Enter LSN=%d",
			newsEvs[1].LSN, sportsEvs[0].LSN)
	}
}

// TestTopicSnapshot: CursorTopic's initial snapshot reflects every live
// row currently in (topic, [tLo, tHi)), even for rows written before
// the cursor was opened.
func TestTopicSnapshot(t *testing.T) {
	s := NewShardID("A")
	fc := &fakeClock{base: time.Unix(1_700_000_000, 0), step: time.Second}
	s.SetClock(fc.now)

	s.PutTopic("a", []byte("1"), "news")  // T0
	s.PutTopic("b", []byte("2"), "sports") // T1
	s.PutTopic("c", []byte("3"), "news")   // T2
	s.PutTopic("d", []byte("4"), "news")   // T3 — will exclude via tHi

	cur := s.CursorTopic("news", fc.base, fc.base.Add(3*time.Second))
	defer cur.Sub.Close()

	if len(cur.Snapshot) != 2 {
		t.Fatalf("snapshot should contain a, c (news in [T0, T3)); got %+v", cur.Snapshot)
	}
	if cur.Snapshot[0].Key != "a" || cur.Snapshot[1].Key != "c" {
		t.Fatalf("snapshot order: got %+v", cur.Snapshot)
	}
}

// TestTopicBidiFlipAcrossBridge: the attribute-flip diff is produced on
// *both* sides of a bridge. A writes a row tagged news; B sees an
// Enter on its news cursor. A retags to sports; B's news cursor must
// see a Leave (synthesized locally from B's prior entry state) and
// B's sports cursor sees an Enter. This is the payoff: the live
// materialized view stays consistent under replicated attribute flips
// without any extra bridge machinery.
func TestTopicBidiFlipAcrossBridge(t *testing.T) {
	a := NewShardID("A")
	b := NewShardID("B")

	br := NewBridge(a, b)
	br.Start()
	defer br.Stop()

	bNews := b.CursorTopic("news", time.Time{}, time.Time{})
	defer bNews.Sub.Close()
	bSports := b.CursorTopic("sports", time.Time{}, time.Time{})
	defer bSports.Sub.Close()

	a.PutTopic("post1", []byte("v1"), "news")
	a.PutTopic("post1", []byte("v2"), "sports")

	newsEvs := drainTopic(t, bNews.Sub, 2)
	sportsEvs := drainTopic(t, bSports.Sub, 1)

	if newsEvs[0].Kind != TopicEnter || !bytes.Equal(newsEvs[0].Value, []byte("v1")) {
		t.Fatalf("B.news[0]: want Enter v1, got %+v", newsEvs[0])
	}
	if newsEvs[1].Kind != TopicLeave {
		t.Fatalf("B.news[1]: want Leave, got %+v", newsEvs[1])
	}
	if sportsEvs[0].Kind != TopicEnter || !bytes.Equal(sportsEvs[0].Value, []byte("v2")) {
		t.Fatalf("B.sports[0]: want Enter v2, got %+v", sportsEvs[0])
	}
}

// TestTopicLWWDropSuppressesEvent: a stale remote patch that loses the
// stamp-dominance check must not produce any TopicEvent — otherwise
// subscribers would see phantom Leave/Enters that don't reflect reality.
func TestTopicLWWDropSuppressesEvent(t *testing.T) {
	a := NewShardID("A")

	cur := a.CursorTopic("news", time.Time{}, time.Time{})
	defer cur.Sub.Close()

	// Local write at lamport=1.
	a.PutTopic("post1", []byte("winner"), "news")

	// Forge a stale remote patch (lower lamport) targeting the same key
	// with a different topic. Apply must drop it.
	applied := a.Apply(Patch{
		Kind:    PatchInsert,
		Key:     "post1",
		Value:   []byte("stale"),
		Stamp:   Stamp{Lamport: 0, Origin: "B"},
		ModTime: time.Now(),
		Topic:   "sports",
	})
	if applied {
		t.Fatalf("stale stamp must be dropped")
	}

	// We should see only the local Enter; no stray Leave or Enter from
	// the forged patch.
	evs := drainTopic(t, cur.Sub, 1)
	if len(evs) != 1 || evs[0].Kind != TopicEnter {
		t.Fatalf("want single Enter; got %+v", evs)
	}
	// Any extra event would block drainTopic, but also double-check
	// buffer is empty right now.
	cur.Sub.mu.Lock()
	n := len(cur.Sub.buf)
	cur.Sub.mu.Unlock()
	if n != 0 {
		t.Fatalf("unexpected buffered events: %d", n)
	}
}
