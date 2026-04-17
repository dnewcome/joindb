package joindb

import "sync"

// Bridge connects two Shards for bidirectional sync. It subscribes to
// each side's patch stream and forwards patches to the other side via
// Shard.Apply. Apply's stamp-dominance check filters out loops and
// resolves concurrent writes to the same key by last-writer-wins
// (higher Lamport; ties broken by Origin ID).
//
// Both Shards must have distinct, non-empty IDs.
//
// Bridge does not bootstrap: it only propagates patches committed after
// Start returns. Pre-existing state on either side stays unreplicated
// unless you re-write it. (Good fit for a demo where the interesting
// thing is live propagation; bootstrap belongs to Merkle sync.)
type Bridge struct {
	a, b   *Shard
	subA   *Subscription
	subB   *Subscription

	mu    sync.Mutex
	stats BridgeStats

	wg sync.WaitGroup
}

// BridgeStats counts what the bridge has seen. Applied is patches that
// actually changed remote state; Dropped is patches rejected by the
// stamp-dominance check (loops, stale writes under conflict).
type BridgeStats struct {
	AtoB_Applied uint64
	AtoB_Dropped uint64
	BtoA_Applied uint64
	BtoA_Dropped uint64
}

// NewBridge builds a bridge between a and b. Call Start to begin.
func NewBridge(a, b *Shard) *Bridge {
	if a.id == "" || b.id == "" || a.id == b.id {
		panic("Bridge requires two Shards with distinct non-empty IDs")
	}
	return &Bridge{a: a, b: b}
}

// Start opens subscriptions, bootstrap-replicates any pre-existing state
// in both directions (via stamped Apply, so LWW resolves conflicts), and
// begins forwarding future patches. After Start returns, both shards are
// converging; in-flight and subsequent writes on either side will settle
// via the usual patch-stream path.
func (br *Bridge) Start() {
	br.subA = br.a.Subscribe("", rangeMaxKey)
	br.subB = br.b.Subscribe("", rangeMaxKey)
	br.bootstrap()
	br.wg.Add(2)
	go br.forward(br.subA, br.b, true)
	go br.forward(br.subB, br.a, false)
}

// bootstrap replays a's state into b and b's state into a via Apply.
// Apply's stamp-dominance check resolves conflicts by LWW; overlapping
// echoes from the forward goroutines (once they start) are idempotent.
func (br *Bridge) bootstrap() {
	aEntries := br.a.Entries()
	bEntries := br.b.Entries()
	for _, e := range aEntries {
		br.b.Apply(patchFromEntry(e))
	}
	for _, e := range bEntries {
		br.a.Apply(patchFromEntry(e))
	}
}

func patchFromEntry(e StampedKV) Patch {
	kind := PatchInsert
	if e.Tombstone {
		kind = PatchDelete
	}
	return Patch{Kind: kind, Key: e.Key, Value: e.Value, Stamp: e.Stamp}
}

// forward pumps patches from src's subscription into dst. aToB=true for
// the A→B direction (used only for stats attribution).
func (br *Bridge) forward(sub *Subscription, dst *Shard, aToB bool) {
	defer br.wg.Done()
	for {
		p, ok := sub.Next()
		if !ok {
			return
		}
		// Skip patches whose stamp origin is the destination itself —
		// those are echoes of writes dst already originated. Apply
		// would drop them anyway (stamp == current), but skipping here
		// avoids touching the lock.
		if p.Stamp.Origin == dst.id {
			continue
		}
		applied := dst.Apply(p)
		br.mu.Lock()
		switch {
		case aToB && applied:
			br.stats.AtoB_Applied++
		case aToB && !applied:
			br.stats.AtoB_Dropped++
		case !aToB && applied:
			br.stats.BtoA_Applied++
		case !aToB && !applied:
			br.stats.BtoA_Dropped++
		}
		br.mu.Unlock()
	}
}

// Stop closes subscriptions and waits for the forwarders to drain.
func (br *Bridge) Stop() {
	br.subA.Close()
	br.subB.Close()
	br.wg.Wait()
}

// Stats returns a snapshot of the bridge counters.
func (br *Bridge) Stats() BridgeStats {
	br.mu.Lock()
	defer br.mu.Unlock()
	return br.stats
}
