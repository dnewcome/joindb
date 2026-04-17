# joindb — plan

A sharded database where replicated indexes let a node (or client) identify and fetch exactly the remote rows needed for a join, and where cursors stay live under concurrent inserts/deletes without restart.

## v0 goal

Single process, two in-memory "shards" wired through channels. Demonstrate three things end-to-end:

1. An index on shard A is replicated to shard B using content-addressed chunks, with incremental sync that transfers bytes proportional to *changed* data, not table size.
2. A range cursor that a client subscribes to: while paging through it, concurrent inserts/deletes on the source shard update the cursor's view without restart, duplicates, or misses.
3. A hash join that uses the replicated index to select exactly which keys to request from the remote, then issues a single bounded range get — not a scan.

No real network, persistence, failure handling, or multi-writer yet.

## Core abstractions

- **Shard** — owns a keyspace; authoritative for writes.
- **Index** — sorted `(key, rowRef)` view over a shard, chunked and content-hashed (Prolly-tree-lite).
- **IndexReplica** — remote mirror of an Index, kept in sync by diffing hashes top-down and fetching only changed subtrees. Also subscribes to a tail stream for low-latency updates.
- **Cursor** — live view over `(shard, index, range)`. Emits `{snapshot, patchStream}`.
- **Executor** — takes a logical join; decides what to evaluate locally against the replicated index vs. what to pull via range get.

## Sync protocol sketch

- Index is partitioned into chunks; each chunk has a content hash; parent nodes hash their children (Merkle / Prolly tree).
- Replica sync walks the tree top-down, fetching any subtree whose hash differs. On top of this, a tail stream carries recent patches so replicas don't have to poll.
- **Consistency.** Each replica tracks a watermark (last applied commit from source). Queries are tagged "as-of watermark W"; cross-shard joins use `min(watermarks)` so they don't read torn state. This is snapshot-ish, not strict-serializable — fine for v0.

## Live cursor semantics

- Subscribe to `(index, range)`. Server sends initial snapshot + monotonic patch stream: `insert / delete / update`, each with a commit timestamp.
- Client reconciles patches against its current window: insertions in-range shift results; deletions drop rows; paging past the window advances without restart.
- Ordering is the index's key order with deterministic tiebreak. Fractional / sequence-CRDT keys (for the "stable position under concurrent insert" case from the notes app) are out of scope for v0 but the API shouldn't preclude them.

## Milestones

1. **M1 — shard + sorted index + range get.** Single shard over a `BTreeMap`-equivalent. `get_range(lo, hi)` returns `(key, value)` pairs. Request/response types defined.
2. **M2 — chunked, content-hashed index.** Wrap the sorted map in chunked storage with hash-addressed nodes. Test: a single mutation touches exactly the affected chunk hashes up to the root.
3. **M3 — replica + pull sync.** Second shard pulls index chunks from first by hash diff. Test: after N random mutations, bytes transferred ≈ O(changed chunks), not O(table).
4. **M4 — live cursor.** Cursor gets snapshot + patches. Concurrency test: 1000 inserts/deletes interleaved with paging — no misses, no dupes.
5. **M5 — pushdown join.** `A ⋈ B` on a predicate evaluable against B's replica of A's index. Verify join issues one bounded range get instead of a scan.
6. **M6 (stretch) — real sockets.** Replace in-process channels with TCP + length-prefixed framing.

## Open decisions

1. **Language — Go.** (Decided 2026-04-16.)
2. **Chunking strategy.** Content-defined (rolling hash on keys) vs. fixed size by key count. Start fixed-size for simplicity, revisit if perf demands.
3. **Query planner location.** v0: client-side only. No split planning.

## Status (2026-04-16)

- M1 ✅ `shard.go` — sorted in-memory shard, Put/Get/Delete/Range.
- M2 ✅ `index.go`, `hash.go`, `nodestore.go` — content-addressed tree with level-aware content-defined chunking. Locality verified: one insert → ≤12 new chunks in a 2000-key tree.
- M3 ✅ `sync.go` — top-down hash-diff pull. 2000 keys → 132-chunk initial sync; one insert → 3-chunk delta.
- M4 ✅ `cursor.go` — atomic snapshot + strict LSN-ordered patch stream. Dual-writer + concurrent drainer converges exactly.
- M5 ✅ `join.go`, `server.go` — join executor with RPC counting. Pushdown path issues 0 query-time RPCs vs naive's 1-per-key.
- M7 ✅ `live.go` — LiveReplica combines M3+M4: bootstraps via Subscribe snapshot, applies patches in order. Exposes its local Shard so replicas cascade (A→B→C). Parity test confirms a fresh Merkle-sync of the source index produces the same state.

Open: M6 (real sockets over the existing `wire.go` types), sparse / keys-only index with a single bounded range get at query time, disconnect/reconnect semantics for LiveReplica (pick: rebootstrap vs. Merkle delta).

## Out of scope for v0

Persistence, crash recovery, multi-writer / consensus, schema & SQL, transactions, rebalance, auth.

## Prior art to keep revisiting

Noria (partial materialization), Differential Dataflow / Materialize (incremental view maintenance), Prolly trees / Dolt (content-addressed range diff), Zero & Replicache (client-as-DB-node sync), ElectricSQL / PowerSync (shape-based partial replication), sequence CRDTs (RGA, Logoot, fractional indexing).
