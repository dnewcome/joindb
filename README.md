# joindb

A from-scratch prototype of a sharded database where **replicated indexes let a client identify and fetch exactly the remote rows it needs** (no remote scan), and where **cursors stay live under concurrent inserts/deletes** without restart.

The animating question: if a client can maintain a coherent local mirror of a remote shard's index — cheaply, and incrementally — then a join or range query "across" shards collapses into a mostly-local operation. The client becomes an extended database node, and "the remote feels local."

This repo is a Go implementation of that idea, small enough to read in one sitting. It's a prototype, not a product: single-process, in-memory, no SQL, no consensus, no persistence. But every core mechanism from the design sketch is present, tested, and actually doing what it claims.

## Where the idea came from

The seed was a paging problem in a notes-taking app: while the user was paged through a list, a new note could be inserted or deleted, and neither offset pagination ("skip restarts") nor keyset pagination ("how do you know something was deleted?") handled it cleanly. You wanted the cursor to stay alive — to reflect concurrent mutations without missing or duplicating rows.

Extended to a distributed setting, that becomes: "what if a client could hold a live cursor over a remote shard?" And then: "what if that cursor was content-addressed, so reconnecting is cheap?" And then: "what if joins across shards used these cursors to push predicates down, so we only pay for what we actually need?"

The prototype is the thinnest possible realization of all three questions in one piece of code.

---

## What's in the box

Six concepts, each earning its keep:

| Concept        | File                            | Role                                                                 |
| -------------- | ------------------------------- | -------------------------------------------------------------------- |
| `Shard`        | `shard.go`                      | Authoritative in-memory key-value store with LSN-ordered write log.  |
| `Index`        | `index.go`, `hash.go`           | Immutable, content-addressed sorted tree (Prolly-tree-lite).         |
| `NodeStore`    | `nodestore.go`                  | Content-addressed blob store. Nodes of the Index live here.          |
| `Sync`         | `sync.go`                       | Pull-based Merkle diff: walk tree top-down, fetch only changed subtrees. |
| `Subscription` | `cursor.go`                     | Atomic range snapshot + strict LSN-ordered patch stream.             |
| `LiveReplica`  | `live.go`                       | Glues the snapshot + patch stream into a mirrored, queryable Shard.  |
| `Server`       | `server.go`                     | Remote-shard facade with per-RPC call counts (lets tests prove pushdown). |
| `InnerJoinKeys`| `join.go`                       | Join executor that uses a local index when available; counts remote RPCs. |

Plus `wire.go` — transport-agnostic request/response types that will fill out when M6 puts real sockets underneath.

All told: **~600 lines of production code, ~700 lines of tests, 30 tests passing under `-race`.**

---

## The core protocols

### 1. The shard: writes are totally ordered by LSN

`Shard` is a sorted key-value store with a single writer (serialized by a mutex). Every committed mutation increments a monotonic `lsn` and emits a `Patch` to any subscribers whose range covers the key:

```
            ┌──────────────────────────────────────┐
            │ Shard                                │
            │                                      │
 Put(k,v) → │  mu.Lock()                           │
            │  data[k] = v; keys (sorted) updated  │
            │  lsn++                               │
 Subscribe  │  for each sub whose [lo,hi) ∋ k:     │
            │      sub.deliver(Patch{lsn, ...})    │
            │  mu.Unlock()                         │
            └──────────────────────────────────────┘
```

Because patch delivery happens under the write lock, subscribers observe patches in **strict LSN order, gap-free** — this is what makes "snapshot + patch stream" a well-defined, complete description of the shard's state.

### 2. The index: content-addressed sorted tree

`Index` is a functional sorted tree, inspired by Prolly trees / Dolt / Noms. The shape:

```
                    ┌──────────────────────────┐
                    │ Internal node (L2)       │
                    │   minKey → childHash     │  ← each internal node
                    │   minKey → childHash     │    is SHA-256 of its bytes
                    └───────┬──────────┬───────┘
                 ┌──────────┘          └──────────┐
            ┌────┴────┐                      ┌────┴────┐
            │ L1      │                      │ L1      │
            └──┬───┬──┘                      └──┬───┬──┘
         ┌────┘   └────┐                  ┌─────┘   └────┐
    ┌────┴────┐   ┌────┴────┐        ┌────┴────┐    ┌────┴────┐
    │ Leaf    │   │ Leaf    │        │ Leaf    │    │ Leaf    │
    │ (k,v)…  │   │ (k,v)…  │        │ (k,v)…  │    │ (k,v)…  │
    └─────────┘   └─────────┘        └─────────┘    └─────────┘
```

Leaves hold sorted `(key, value)` pairs. Internal nodes hold `(minKey, childHash)` entries. Everything is keyed by SHA-256 of its canonical serialization and stored in a `NodeStore`.

**The whole game is where chunk boundaries land.**

Fixed-position chunking ("first 16 keys, next 16 keys, …") is tempting but broken: inserting a key at the beginning shifts every subsequent chunk's membership and invalidates every chunk hash. Instead, joindb uses **content-defined chunking**: a chunk ends when `fnv1a(key)` has a certain number of low zero bits. The boundary is a property of the key, not its position. Inserting a non-boundary key only rewrites the one chunk it lands in.

To prevent pathological tree depth, the boundary predicate gets strictly stricter at each level: leaf level needs `bits` zero bits, L1 needs `bits+1`, L2 needs `bits+2`, and so on. With `bits=4` (default), that's average fanout of 16 / 32 / 64 / … per level.

The locality claim is tested directly:

```
TestIndexLocality
    2000 keys → 132 chunks in the store.
    Insert one new key → 3 additional chunks created (one path to root).
```

### 3. Sync: hash-diff pull

Given a fresh client and a server publishing an index root, `Sync` walks the tree top-down. If the client's local store already has a node's hash, the entire subtree beneath it is identical and we skip. Otherwise we fetch, parse, and recurse on children.

```go
func syncWalk(local, source NodeStore, h Hash, stats *SyncStats) error {
    if local.Has(h) { return nil }           // identical subtree — skip
    raw, _ := source.Get(h)
    stats.ChunksFetched++
    stats.BytesFetched += len(raw)
    local.Put(h, raw)
    if internal, ok := parseNode(raw).(*internalNode); ok {
        for _, c := range internal.Children {
            syncWalk(local, source, c.Hash, stats)
        }
    }
    return nil
}
```

The combination of (a) locality at the tree level and (b) hash-skip at the sync level gives the delta-sync property. From the test output:

```
TestSyncDelta
    initial: 132 chunks / 44,787 bytes
    delta after one mutation: 3 chunks / 808 bytes
```

Two orders of magnitude cheaper, because 99% of the tree was already in the local store under unchanged hashes.

### 4. Subscription: snapshot + patch stream

A subscription is an **atomically consistent bootstrap plus a gap-free suffix**:

```go
sub := shard.Subscribe("m", "s")
// sub.Snapshot:  range [m, s) at LSN X
// sub.StartLSN:  X + 1
// sub.Next():    every patch with LSN ≥ StartLSN whose Key is in [m, s)
```

Because the snapshot is taken under the shard's write lock, it reflects a real point in the commit history. Because patches are delivered under the same lock, they arrive in LSN order with no skips. The consumer's invariant is:

> After applying `Snapshot` and every `Patch` returned by `Next` up to LSN = N, the local view of `[lo, hi)` is exactly what `shard.Range(lo, hi)` would return if the shard were frozen at LSN = N.

This is what the original notes-app use case wanted: a cursor that survives concurrent writes.

Tested directly under two concurrent writers doing 500 ops each:

```
TestCursorConcurrentNoMissNoDupe
    100 seed keys → Subscribe → 1000 concurrent random writes → Close → drain.
    Applied snapshot + all patches = exactly shard.Range("", "∞") at final LSN.
    LSNs strictly monotonic.
```

### 5. LiveReplica: both halves, one connection

The payoff protocol. A `LiveReplica` owns its own `Shard` and mirrors a source:

```
┌─────────┐      Subscribe("", ∞)        ┌──────────────┐
│ source  │ ──────────────────────────▶  │ LiveReplica  │
│  Shard  │  snapshot + patch stream     │   local      │
└─────────┘                              │   Shard      │
                                         └──────────────┘
                                               │
                                               │ (queryable!
                                               │  Subscribable!)
                                               ▼
                                         ┌──────────────┐
                                         │ cascade:     │
                                         │ r2 replicates│
                                         │ from r1      │
                                         └──────────────┘
```

1. `Start()` subscribes to the source, applies `Snapshot` to the local shard under the shard's write lock, and records `appliedLSN = StartLSN - 1`.
2. A background goroutine pumps `sub.Next()` — for each patch, it `Put`s or `Delete`s on the local shard and bumps `appliedLSN`.
3. The local `Shard` exposes every normal shard operation, **including its own Subscribe**. That's what makes replication recursive: `r2 := NewLiveReplica(r1.Shard())` works trivially, because a replica's local Shard emits its own patch stream as it applies the source's.

Tested three ways:

- **Convergence** — two concurrent writers on the source; the replica ends up pairwise-identical to the source after `WaitFor(src.LSN())`.
- **Cascade** — `A → r1 → r2`; writes to A propagate through r1 to r2 via two independent subscriptions.
- **Merkle parity** — after writes settle, a fresh `Sync` of the source's published index produces the same `(key, value)` set as the live replica's accumulated state. The two protocols are different paths to the same snapshot.

### 6. Pushdown join: the RPC count is the point

`InnerJoinKeys(bPairs, aLocal, remoteA)`:

- If `aLocal` is non-nil, every A-side lookup hits the replicated index locally.
- Otherwise, it falls back to `remoteA.Get(key)`.

The `Server` wrapper counts every RPC. The two regimes contrast sharply:

```
TestJoinNaiveOneRPCPerKey
    1000 B-rows → 1000 remote Get RPCs → 500 join results.

TestJoinPushdownZeroQueryRPCs
    Amortized: 68 chunks / 14,572 bytes synced once.
    Query: 0 remote RPCs of any kind → 500 join results (identical).

TestJoinPushdownSurvivesMutation
    Mutate A → republish index → delta sync ≤ 12 chunks → still 0 query RPCs.
```

This is the "clients could form sort of an extended database node, and operations locally would work as if the remote database was local" sentence from the original design sketch, running in code.

---

## File map

```
joindb/
├── go.mod
├── PLAN.md              ← living design doc with milestone status
│
├── hash.go              ← Hash = SHA-256, short-string for logs
├── nodestore.go         ← NodeStore interface + MemStore impl
├── shard.go             ← Shard: Put/Get/Delete/Range/LSN + subscriber dispatch
├── index.go             ← content-addressed tree: Build, Get, Range; isBoundary
├── sync.go              ← top-down hash-diff pull
├── cursor.go            ← Patch, PatchKind, Subscription, Shard.Subscribe
├── live.go              ← LiveReplica: Start, Stop, WaitFor, cascade
├── server.go            ← Server: wraps Shard + IdxStore, counts RPCs
├── join.go              ← InnerJoinKeys: pushdown vs naive
├── wire.go              ← GetReq/Resp etc., unused until M6 (real sockets)
│
└── *_test.go            ← 30 tests across all layers
```

Every file stands on its own. Reading order matches the milestone order: shard → index → sync → cursor → live → join.

---

## Running it

```bash
go test -count=1 -race ./...
```

Verbose, with byte counts for the interesting tests:

```bash
go test -count=1 -race -v -run 'Sync|Join|Replica' ./...
```

You'll see the numbers cited in this README in the test logs.

---

## Design decisions worth knowing

**Why content-defined chunking?**
Fixed-position chunking cascades: insert at position 0 and you rebuild every chunk. Content-defined chunking places boundaries at key-derived positions, so an insert only perturbs the chunk it lands in. This is what makes tree-level locality real, which is what makes `Sync` cheap, which is what makes `LiveReplica`'s cold start plausible.

**Why a level-aware boundary predicate?**
If every level used the same boundary mask, the tree couldn't reduce in size at each level up — pathological cases could recurse forever. Requiring one extra zero bit per level means higher levels are strictly sparser than lower ones, so depth is bounded.

**Why copy values everywhere?**
Every `Put`, `Get`, and `Range` makes defensive copies. A prototype with value aliasing bugs would waste hours of debugging; the copies cost microseconds. We can optimize later when the workload is known.

**Why SHA-256 and not something faster?**
It's fine. Hashing isn't on the hot path for anything we measure. If this becomes real, BLAKE3 is the drop-in replacement.

**Why does `LiveReplica` initialize from the subscription snapshot, not Merkle sync?**
Because the subscription snapshot is atomic-at-a-known-LSN for free. Combining Merkle sync with live subscription in one protocol requires a server that can publish indexes at explicit LSN anchors and a client that can diff across LSN ranges — worth doing, but that's the next step, not this one. The `TestLiveReplicaMerkleParity` test confirms both paths lead to the same state.

**Why are there both `Shard` and `Index` types with overlapping operations?**
`Shard` is the writable authoritative store; `Index` is an immutable content-addressed view of a shard's data at a point in time. A shard produces indexes; indexes don't mutate. The separation is what lets the sync protocol be cache-friendly and the query path be consistent.

---

## What's explicitly not here

| Concern                                      | Where it would go           |
| -------------------------------------------- | --------------------------- |
| Persistence, crash recovery                  | `NodeStore` impl on disk    |
| Multi-writer / consensus                     | Beyond v0 entirely          |
| Schema, SQL parsing, transactions            | Outside this layer          |
| Real sharding, rebalance                     | M6+                         |
| Real sockets                                 | M6 (wire types are ready)   |
| Authentication, TLS                          | M6+                         |
| Cursors with fractional-index / CRDT keys    | Natural extension of `Patch`|
| Disconnected reconnection for `LiveReplica`  | Merkle catch-up + resubscribe |
| Sparse / keys-only secondary indexes         | Straightforward `Index` variant|
| Query planner with cost-based pushdown       | Beyond this prototype       |

The PLAN.md tracks most of these.

---

## Prior art worth knowing

The space this prototype sits in has been explored from many angles. Some of the systems that informed the design:

- **Noria** (Jon Gjengset's PhD thesis) — partial materialization of views across shards. Closest existing system to "replicated index for query pushdown."
- **Differential Dataflow / Materialize** — incremental view maintenance as a first-class operation. This is the theory behind "keep a cursor coherent under inserts/deletes."
- **Prolly trees / Dolt / Noms** — content-addressed B-trees with content-defined chunking. The direct ancestor of `index.go`.
- **Zero / Replicache (Rocicorp)** — client-side query engine with server sync. The commercial version of "client as extended DB node."
- **ElectricSQL / PowerSync** — shape-based partial replication over SQL stores.
- **Sequence CRDTs (RGA, Logoot, fractional indexing)** — stable ordering under concurrent insert, which is what the original notes-app paging problem actually needed.
- **Dynamo-style anti-entropy** — Merkle trees for efficient replica reconciliation. Older and simpler than Prolly, same family.

joindb is not any of these. It's a tiny, readable attempt to put the smallest subset of their ideas into a single coherent shape.

---

## A final note

This prototype exists because a problem that looked like "paging UX in a notes app" turned out, on inspection, to have the same deep structure as "efficient cross-shard joins." That's usually a sign the underlying abstraction is worth a closer look. Whether this particular synthesis — content-addressed snapshot + live patch stream, composed recursively — is actually the right one for a real system is an open question. But the pieces all fit, and the tests run, and the numbers are what they should be. That's a start.
