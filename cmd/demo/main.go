package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"joindb"
)

//go:embed index.html
var staticFS embed.FS

func main() {
	addr := ":8080"
	d := newDemo()
	d.start()

	mux := http.NewServeMux()
	mux.HandleFunc("/", d.handleIndex)
	mux.HandleFunc("/api/events", d.handleEvents)
	mux.HandleFunc("/api/put", d.handlePut)
	mux.HandleFunc("/api/delete", d.handleDelete)
	mux.HandleFunc("/api/burst", d.handleBurst)
	mux.HandleFunc("/api/sync", d.handleSync)
	mux.HandleFunc("/api/reset", d.handleReset)

	log.Printf("joindb demo listening on http://localhost%s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

// demo holds the source shard, its Server facade (for Merkle index), a
// LiveReplica whose patch log we mirror for the UI, and a separate
// NodeStore that stands in for the replica's view of the Merkle tree —
// populated only by explicit /api/sync calls so the "delta sync" story
// is visible.
type demo struct {
	mu sync.Mutex

	source  *joindb.Shard
	server  *joindb.Server
	replica *joindb.LiveReplica

	// replicaStore is the replica-side view of the source's index. It is
	// populated only by /api/sync; the UI colors nodes green if they're
	// present here and yellow/pending if not. This is separate from the
	// LiveReplica.Shard() mirror, which uses the patch-stream path.
	replicaStore *joindb.MemStore

	lastSync  joindb.SyncStats
	totalSync joindb.SyncStats
	numSyncs  int

	// patchLog is the in-UI activity log, most recent first.
	patchLog []patchEntry

	tailStop chan struct{} // closed to retire the current tailPatches goroutine

	// SSE subscribers
	subs  map[chan []byte]struct{}
	dirty chan struct{}
}

type patchEntry struct {
	Time  string `json:"time"`
	LSN   uint64 `json:"lsn"`
	Kind  string `json:"kind"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

const patchLogCap = 200

func newDemo() *demo {
	src := joindb.NewShard()
	d := &demo{
		source:       src,
		server:       joindb.NewServer(src),
		replica:      joindb.NewLiveReplica(src),
		replicaStore: joindb.NewMemStore(),
		subs:         map[chan []byte]struct{}{},
		dirty:        make(chan struct{}, 1),
		tailStop:     make(chan struct{}),
	}
	d.replica.Start()
	d.server.PublishIndex()
	return d
}

// start launches:
//   - a patch-tailing goroutine on the source that appends to patchLog
//     and triggers a broadcast
//   - a coalescing broadcaster that renders state every 50ms if dirty
func (d *demo) start() {
	go d.tailPatches(d.source, d.tailStop)
	go d.broadcastLoop()
}

func (d *demo) tailPatches(src *joindb.Shard, stop chan struct{}) {
	sub := src.Subscribe("", rangeMaxKey)
	defer sub.Close()
	done := make(chan struct{})
	go func() {
		select {
		case <-stop:
			sub.Close()
		case <-done:
		}
	}()
	defer close(done)
	for {
		p, ok := sub.Next()
		if !ok {
			return
		}
		d.mu.Lock()
		entry := patchEntry{
			Time: time.Now().Format("15:04:05.000"),
			LSN:  p.LSN,
			Kind: p.Kind.String(),
			Key:  p.Key,
		}
		if p.Value != nil {
			entry.Value = string(p.Value)
		}
		d.patchLog = append([]patchEntry{entry}, d.patchLog...)
		if len(d.patchLog) > patchLogCap {
			d.patchLog = d.patchLog[:patchLogCap]
		}
		d.mu.Unlock()
		d.markDirty()
	}
}

// markDirty / broadcastLoop: coalesce many rapid events into ~20Hz
// updates so a 1000-row burst doesn't swamp the SSE channel.
func (d *demo) markDirty() {
	select {
	case d.dirty <- struct{}{}:
	default:
	}
}

func (d *demo) broadcastLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var pending bool
	for {
		select {
		case <-d.dirty:
			pending = true
		case <-ticker.C:
			if !pending {
				continue
			}
			pending = false
			payload := d.snapshotJSON()
			d.fanout(payload)
		}
	}
}

func (d *demo) fanout(payload []byte) {
	d.mu.Lock()
	chans := make([]chan []byte, 0, len(d.subs))
	for c := range d.subs {
		chans = append(chans, c)
	}
	d.mu.Unlock()
	for _, c := range chans {
		select {
		case c <- payload:
		default:
			// slow consumer — skip; they'll get the next tick
		}
	}
}

// stateJSON is the full UI state sent over SSE.
type stateJSON struct {
	SourceLSN     uint64       `json:"sourceLSN"`
	ReplicaLSN    uint64       `json:"replicaLSN"`
	SourceCount   int          `json:"sourceCount"`
	ReplicaCount  int          `json:"replicaCount"`
	SourceKVs     []kvJSON     `json:"sourceKVs"`
	ReplicaKVs    []kvJSON     `json:"replicaKVs"`
	Tree          *nodeJSON    `json:"tree"`
	SourceChunks  int          `json:"sourceChunks"`
	ReplicaChunks int          `json:"replicaChunks"`
	LastSync      syncJSON     `json:"lastSync"`
	TotalSync     syncJSON     `json:"totalSync"`
	NumSyncs      int          `json:"numSyncs"`
	Patches       []patchEntry `json:"patches"`
}

type kvJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type nodeJSON struct {
	Hash      string      `json:"hash"`
	Kind      string      `json:"kind"`
	InReplica bool        `json:"inReplica"`
	Count     int         `json:"count"`
	MinKey    string      `json:"minKey"`
	MaxKey    string      `json:"maxKey"`
	Keys      []string    `json:"keys,omitempty"`
	Children  []*nodeJSON `json:"children,omitempty"`
	Size      int         `json:"size"`
}

type syncJSON struct {
	Chunks int `json:"chunks"`
	Bytes  int `json:"bytes"`
}

func (d *demo) snapshotJSON() []byte {
	d.mu.Lock()
	source := d.source
	server := d.server
	replica := d.replica
	replicaStore := d.replicaStore
	logCopy := make([]patchEntry, len(d.patchLog))
	copy(logCopy, d.patchLog)
	last := d.lastSync
	total := d.totalSync
	numSyncs := d.numSyncs
	d.mu.Unlock()

	// Republish so the visualized tree tracks live state.
	server.PublishIndex()
	root := server.IdxRoot

	srcPairs := source.Range("", rangeMaxKey)
	repPairs := replica.Shard().Range("", rangeMaxKey)

	var tree *nodeJSON
	if !root.IsZero() {
		tree = buildNodeJSON(server.IdxStore, replicaStore, root)
	}

	s := stateJSON{
		SourceLSN:     source.LSN(),
		ReplicaLSN:    replica.AppliedLSN(),
		SourceCount:   len(srcPairs),
		ReplicaCount:  len(repPairs),
		SourceKVs:     toKVJSON(srcPairs),
		ReplicaKVs:    toKVJSON(repPairs),
		Tree:          tree,
		SourceChunks:  server.IdxStore.Len(),
		ReplicaChunks: replicaStore.Len(),
		LastSync:      syncJSON{last.ChunksFetched, last.BytesFetched},
		TotalSync:     syncJSON{total.ChunksFetched, total.BytesFetched},
		NumSyncs:      numSyncs,
		Patches:       logCopy,
	}
	buf, _ := json.Marshal(s)
	return buf
}

func toKVJSON(pairs []joindb.KV) []kvJSON {
	out := make([]kvJSON, len(pairs))
	for i, p := range pairs {
		out[i] = kvJSON{Key: p.Key, Value: string(p.Value)}
	}
	return out
}

func buildNodeJSON(src joindb.NodeStore, replica joindb.NodeStore, h joindb.Hash) *nodeJSON {
	info, err := joindb.LoadNode(src, h)
	if err != nil {
		return &nodeJSON{Hash: h.String(), Kind: "missing"}
	}
	n := &nodeJSON{
		Hash:      h.String(),
		Kind:      info.Kind.String(),
		InReplica: replica.Has(h),
		Size:      info.RawSize,
	}
	switch info.Kind {
	case joindb.NodeLeaf:
		n.Count = len(info.Pairs)
		if len(info.Pairs) > 0 {
			n.MinKey = info.Pairs[0].Key
			n.MaxKey = info.Pairs[len(info.Pairs)-1].Key
		}
		keys := make([]string, 0, len(info.Pairs))
		for _, kv := range info.Pairs {
			keys = append(keys, kv.Key)
		}
		n.Keys = keys
	case joindb.NodeInternal:
		n.Count = len(info.Children)
		if len(info.Children) > 0 {
			n.MinKey = info.Children[0].MinKey
		}
		children := make([]*nodeJSON, 0, len(info.Children))
		var lastMax string
		for _, c := range info.Children {
			child := buildNodeJSON(src, replica, c.Hash)
			if child.MaxKey > lastMax {
				lastMax = child.MaxKey
			}
			children = append(children, child)
		}
		n.MaxKey = lastMax
		n.Children = children
	}
	return n
}

// ---------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------

const rangeMaxKey = "\xff\xff\xff\xff"

func (d *demo) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	b, err := staticFS.ReadFile("index.html")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(b)
}

func (d *demo) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 4)
	d.mu.Lock()
	d.subs[ch] = struct{}{}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.subs, ch)
		d.mu.Unlock()
	}()

	fmt.Fprintf(w, "data: %s\n\n", d.snapshotJSON())
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case payload := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}

func (d *demo) handlePut(w http.ResponseWriter, r *http.Request) {
	var req struct{ Key, Value string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if req.Key == "" {
		http.Error(w, "key required", 400)
		return
	}
	d.mu.Lock()
	src := d.source
	d.mu.Unlock()
	src.Put(req.Key, []byte(req.Value))
	d.markDirty()
	w.WriteHeader(204)
}

func (d *demo) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req struct{ Key string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	d.mu.Lock()
	src := d.source
	d.mu.Unlock()
	src.Delete(req.Key)
	d.markDirty()
	w.WriteHeader(204)
}

func (d *demo) handleBurst(w http.ResponseWriter, r *http.Request) {
	var req struct {
		N      int    `json:"n"`
		Prefix string `json:"prefix"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if req.N <= 0 {
		req.N = 50
	}
	if req.N > 5000 {
		req.N = 5000
	}
	if req.Prefix == "" {
		req.Prefix = "k"
	}
	d.mu.Lock()
	src := d.source
	d.mu.Unlock()
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 42))
	for range req.N {
		k := fmt.Sprintf("%s%04d", req.Prefix, rng.IntN(10000))
		src.Put(k, fmt.Appendf(nil, "v%d", rng.IntN(1_000_000)))
	}
	d.markDirty()
	w.WriteHeader(204)
}

// handleSync triggers a Merkle-based pull from source's IdxStore into the
// replica-side store. The delta stats (chunks fetched, bytes fetched) are
// the interesting number — after a burst of writes, the delta should be
// much smaller than the full tree.
func (d *demo) handleSync(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	server := d.server
	replicaStore := d.replicaStore
	d.mu.Unlock()

	server.PublishIndex()
	root := server.IdxRoot
	stats, err := joindb.Sync(replicaStore, server.NodeStoreView(), root)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	d.mu.Lock()
	d.lastSync = stats
	d.totalSync.ChunksFetched += stats.ChunksFetched
	d.totalSync.BytesFetched += stats.BytesFetched
	d.numSyncs++
	d.mu.Unlock()
	d.markDirty()
	json.NewEncoder(w).Encode(stats)
}

func (d *demo) handleReset(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	oldReplica := d.replica
	oldTailStop := d.tailStop
	d.source = joindb.NewShard()
	d.server = joindb.NewServer(d.source)
	d.replica = joindb.NewLiveReplica(d.source)
	d.replicaStore = joindb.NewMemStore()
	d.lastSync = joindb.SyncStats{}
	d.totalSync = joindb.SyncStats{}
	d.numSyncs = 0
	d.patchLog = nil
	d.tailStop = make(chan struct{})
	newSource := d.source
	newTailStop := d.tailStop
	newServer := d.server
	d.mu.Unlock()

	close(oldTailStop)
	oldReplica.Stop()
	d.mu.Lock()
	d.replica.Start()
	d.mu.Unlock()
	newServer.PublishIndex()
	go d.tailPatches(newSource, newTailStop)
	d.markDirty()
	w.WriteHeader(204)
}
