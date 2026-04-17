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

const rangeMaxKey = "\xff\xff\xff\xff"

func main() {
	addr := ":8081"
	d := newDemo()
	d.start()

	mux := http.NewServeMux()
	mux.HandleFunc("/", d.handleIndex)
	mux.HandleFunc("/api/events", d.handleEvents)
	mux.HandleFunc("/api/put", d.handlePut)
	mux.HandleFunc("/api/delete", d.handleDelete)
	mux.HandleFunc("/api/burst", d.handleBurst)
	mux.HandleFunc("/api/partition", d.handlePartition)
	mux.HandleFunc("/api/reset", d.handleReset)

	log.Printf("joindb bidi demo listening on http://localhost%s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

type demo struct {
	mu sync.Mutex

	a, b   *joindb.Shard
	bridge *joindb.Bridge

	// partitioned=true severs the bridge (stops forwarding). Writes
	// accumulate on each side; concurrent conflicts resolve via LWW
	// when the bridge is restored.
	partitioned bool

	// Per-side patch logs, shown in the UI newest-first.
	logA []patchEntry
	logB []patchEntry

	subs  map[chan []byte]struct{}
	dirty chan struct{}
}

const patchLogCap = 120

type patchEntry struct {
	Time   string `json:"time"`
	LSN    uint64 `json:"lsn"`
	Kind   string `json:"kind"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Origin string `json:"origin"`
	Clock  uint64 `json:"clock"`
}

func newDemo() *demo {
	a := joindb.NewShardID("A")
	b := joindb.NewShardID("B")
	br := joindb.NewBridge(a, b)
	d := &demo{
		a:      a,
		b:      b,
		bridge: br,
		subs:   map[chan []byte]struct{}{},
		dirty:  make(chan struct{}, 1),
	}
	br.Start()
	return d
}

func (d *demo) start() {
	go d.tail(d.a, &d.logA)
	go d.tail(d.b, &d.logB)
	go d.broadcastLoop()
}

// tail subscribes directly to a shard so the UI log reflects every
// patch the shard commits (including ones applied from its peer).
func (d *demo) tail(s *joindb.Shard, dst *[]patchEntry) {
	sub := s.Subscribe("", rangeMaxKey)
	defer sub.Close()
	for {
		p, ok := sub.Next()
		if !ok {
			return
		}
		e := patchEntry{
			Time:   time.Now().Format("15:04:05.000"),
			LSN:    p.LSN,
			Kind:   p.Kind.String(),
			Key:    p.Key,
			Origin: p.Stamp.Origin,
			Clock:  p.Stamp.Lamport,
		}
		if p.Value != nil {
			e.Value = string(p.Value)
		}
		d.mu.Lock()
		*dst = append([]patchEntry{e}, *dst...)
		if len(*dst) > patchLogCap {
			*dst = (*dst)[:patchLogCap]
		}
		d.mu.Unlock()
		d.markDirty()
	}
}

func (d *demo) markDirty() {
	select {
	case d.dirty <- struct{}{}:
	default:
	}
}

func (d *demo) broadcastLoop() {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var pending bool
	for {
		select {
		case <-d.dirty:
			pending = true
		case <-tick.C:
			if !pending {
				continue
			}
			pending = false
			d.fanout(d.snapshotJSON())
		}
	}
}

func (d *demo) fanout(buf []byte) {
	d.mu.Lock()
	chans := make([]chan []byte, 0, len(d.subs))
	for c := range d.subs {
		chans = append(chans, c)
	}
	d.mu.Unlock()
	for _, c := range chans {
		select {
		case c <- buf:
		default:
		}
	}
}

// --- state JSON ---------------------------------------------------------

type stateJSON struct {
	A           sideJSON        `json:"a"`
	B           sideJSON        `json:"b"`
	Partitioned bool            `json:"partitioned"`
	Bridge      joindb.BridgeStats `json:"bridgeStats"`
	LogA        []patchEntry    `json:"logA"`
	LogB        []patchEntry    `json:"logB"`
	Divergence  []divJSON       `json:"divergence"`
}

type sideJSON struct {
	ID      string   `json:"id"`
	LSN     uint64   `json:"lsn"`
	Lamport uint64   `json:"lamport"`
	KVs     []kvJSON `json:"kvs"`
}

type kvJSON struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Origin string `json:"origin"`
	Clock  uint64 `json:"clock"`
}

type divJSON struct {
	Key      string `json:"key"`
	AValue   string `json:"aValue,omitempty"`
	BValue   string `json:"bValue,omitempty"`
	AStamp   string `json:"aStamp"`
	BStamp   string `json:"bStamp"`
	APresent bool   `json:"aPresent"`
	BPresent bool   `json:"bPresent"`
}

func (d *demo) snapshotJSON() []byte {
	d.mu.Lock()
	logA := append([]patchEntry(nil), d.logA...)
	logB := append([]patchEntry(nil), d.logB...)
	partitioned := d.partitioned
	stats := d.bridge.Stats()
	d.mu.Unlock()

	aSide := collectSide(d.a)
	bSide := collectSide(d.b)
	div := computeDivergence(d.a, d.b, aSide.KVs, bSide.KVs)

	s := stateJSON{
		A:           aSide,
		B:           bSide,
		Partitioned: partitioned,
		Bridge:      stats,
		LogA:        logA,
		LogB:        logB,
		Divergence:  div,
	}
	buf, _ := json.Marshal(s)
	return buf
}

func collectSide(s *joindb.Shard) sideJSON {
	pairs := s.Range("", rangeMaxKey)
	kvs := make([]kvJSON, 0, len(pairs))
	for _, p := range pairs {
		st := s.Stamp(p.Key)
		kvs = append(kvs, kvJSON{
			Key:    p.Key,
			Value:  string(p.Value),
			Origin: st.Origin,
			Clock:  st.Lamport,
		})
	}
	return sideJSON{
		ID:      s.ID(),
		LSN:     s.LSN(),
		Lamport: s.Lamport(),
		KVs:     kvs,
	}
}

// computeDivergence: keys where A and B disagree on value or presence.
// Useful when partitioned — both sides drift; lists populate; once
// restored, list drains as LWW resolves each key.
func computeDivergence(a, b *joindb.Shard, aKVs, bKVs []kvJSON) []divJSON {
	aMap := make(map[string]kvJSON, len(aKVs))
	for _, kv := range aKVs {
		aMap[kv.Key] = kv
	}
	bMap := make(map[string]kvJSON, len(bKVs))
	for _, kv := range bKVs {
		bMap[kv.Key] = kv
	}
	seen := make(map[string]bool, len(aKVs)+len(bKVs))
	out := []divJSON{}
	for _, kv := range aKVs {
		seen[kv.Key] = true
		bv, bok := bMap[kv.Key]
		if !bok {
			out = append(out, divJSON{
				Key: kv.Key, AValue: kv.Value, APresent: true,
				AStamp: fmt.Sprintf("%s@%d", kv.Origin, kv.Clock),
				BStamp: fmt.Sprintf("%s@%d", b.Stamp(kv.Key).Origin, b.Stamp(kv.Key).Lamport),
			})
			continue
		}
		if bv.Value != kv.Value {
			out = append(out, divJSON{
				Key: kv.Key, AValue: kv.Value, BValue: bv.Value,
				APresent: true, BPresent: true,
				AStamp: fmt.Sprintf("%s@%d", kv.Origin, kv.Clock),
				BStamp: fmt.Sprintf("%s@%d", bv.Origin, bv.Clock),
			})
		}
	}
	for _, kv := range bKVs {
		if seen[kv.Key] {
			continue
		}
		out = append(out, divJSON{
			Key: kv.Key, BValue: kv.Value, BPresent: true,
			AStamp: fmt.Sprintf("%s@%d", a.Stamp(kv.Key).Origin, a.Stamp(kv.Key).Lamport),
			BStamp: fmt.Sprintf("%s@%d", kv.Origin, kv.Clock),
		})
	}
	return out
}

// --- handlers -----------------------------------------------------------

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
		case buf := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", buf)
			flusher.Flush()
		}
	}
}

func (d *demo) resolveSide(name string) *joindb.Shard {
	if name == "A" {
		return d.a
	}
	if name == "B" {
		return d.b
	}
	return nil
}

func (d *demo) handlePut(w http.ResponseWriter, r *http.Request) {
	var req struct{ Side, Key, Value string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s := d.resolveSide(req.Side)
	if s == nil || req.Key == "" {
		http.Error(w, "side and key required", 400)
		return
	}
	s.Put(req.Key, []byte(req.Value))
	d.markDirty()
	w.WriteHeader(204)
}

func (d *demo) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req struct{ Side, Key string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s := d.resolveSide(req.Side)
	if s == nil {
		http.Error(w, "side required", 400)
		return
	}
	s.Delete(req.Key)
	d.markDirty()
	w.WriteHeader(204)
}

func (d *demo) handleBurst(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Side string
		N    int
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s := d.resolveSide(req.Side)
	if s == nil {
		http.Error(w, "side required", 400)
		return
	}
	if req.N <= 0 {
		req.N = 20
	}
	if req.N > 2000 {
		req.N = 2000
	}
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(len(req.Side))))
	for range req.N {
		k := fmt.Sprintf("k%03d", rng.IntN(60))
		if rng.IntN(5) == 0 {
			s.Delete(k)
			continue
		}
		s.Put(k, fmt.Appendf(nil, "%s-%d", s.ID(), rng.IntN(1_000_000)))
	}
	d.markDirty()
	w.WriteHeader(204)
}

// handlePartition toggles the bridge on/off. When partitioned the bridge
// is Stopped; when restored a fresh bridge is started. Writes made
// during the partition stay local until restore, at which point LWW
// resolves them.
func (d *demo) handlePartition(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	wasPart := d.partitioned
	if !wasPart {
		d.bridge.Stop()
		d.partitioned = true
	} else {
		d.bridge = joindb.NewBridge(d.a, d.b)
		d.bridge.Start()
		d.partitioned = false
	}
	d.mu.Unlock()
	d.markDirty()
	w.WriteHeader(204)
}

func (d *demo) handleReset(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.bridge.Stop()
	d.a = joindb.NewShardID("A")
	d.b = joindb.NewShardID("B")
	d.bridge = joindb.NewBridge(d.a, d.b)
	d.logA = nil
	d.logB = nil
	d.partitioned = false
	a := d.a
	b := d.b
	d.mu.Unlock()
	d.bridge.Start()
	go d.tail(a, &d.logA)
	go d.tail(b, &d.logB)
	d.markDirty()
	w.WriteHeader(204)
}
