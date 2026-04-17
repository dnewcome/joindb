package joindb

import (
	"fmt"
	"sync"
)

// LiveReplica mirrors a source Shard into a local Shard. It bootstraps
// from the source's Subscription snapshot (atomic at LSN X) and applies
// every subsequent patch in LSN order. The local Shard exposes the same
// interface as any other Shard, so queries, subscriptions, and further
// replicas can all hang off of it — this is the "client as extended DB
// node" property, recursively.
//
// Invariant: r.AppliedLSN() is the highest *source* LSN reflected in the
// local Shard. The local Shard has its own independent LSN that counts
// its own commits (one per applied patch + one per snapshot entry at
// bootstrap).
type LiveReplica struct {
	local  *Shard
	source *Shard
	sub    *Subscription

	mu         sync.Mutex
	cond       *sync.Cond
	appliedLSN uint64
	stopped    bool

	wg sync.WaitGroup
}

// NewLiveReplica creates a replica that will follow source's full range.
// Call Start to begin replication.
func NewLiveReplica(source *Shard) *LiveReplica {
	r := &LiveReplica{local: NewShard(), source: source}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Start subscribes to the source, applies the initial snapshot, and
// launches a background goroutine that streams patches. Returns after
// the snapshot is applied — reads of Shard() immediately after Start
// see the bootstrap state.
func (r *LiveReplica) Start() {
	r.sub = r.source.Subscribe("", rangeMaxKey)
	for _, kv := range r.sub.Snapshot {
		r.local.Put(kv.Key, kv.Value)
	}
	r.mu.Lock()
	r.appliedLSN = r.sub.StartLSN - 1
	r.mu.Unlock()

	r.wg.Add(1)
	go r.apply()
}

func (r *LiveReplica) apply() {
	defer r.wg.Done()
	for {
		p, ok := r.sub.Next()
		if !ok {
			r.mu.Lock()
			r.stopped = true
			r.cond.Broadcast()
			r.mu.Unlock()
			return
		}
		switch p.Kind {
		case PatchInsert, PatchUpdate:
			r.local.Put(p.Key, p.Value)
		case PatchDelete:
			r.local.Delete(p.Key)
		}
		r.mu.Lock()
		r.appliedLSN = p.LSN
		r.cond.Broadcast()
		r.mu.Unlock()
	}
}

// Stop closes the subscription and waits for the apply loop to drain any
// remaining buffered patches before returning.
func (r *LiveReplica) Stop() {
	r.sub.Close()
	r.wg.Wait()
}

// Shard returns the local mirror. Safe to read concurrently with apply.
func (r *LiveReplica) Shard() *Shard { return r.local }

// AppliedLSN is the highest source LSN the local shard reflects.
func (r *LiveReplica) AppliedLSN() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.appliedLSN
}

// WaitFor blocks until the replica has applied at least untilLSN from
// the source, or until Stop has drained the subscription.
func (r *LiveReplica) WaitFor(untilLSN uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.appliedLSN < untilLSN && !r.stopped {
		r.cond.Wait()
	}
	if r.appliedLSN >= untilLSN {
		return nil
	}
	return fmt.Errorf("replica stopped at applied LSN %d; wanted %d", r.appliedLSN, untilLSN)
}
