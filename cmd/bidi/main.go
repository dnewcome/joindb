package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"sort"
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
	mux.HandleFunc("/api/cursor", d.handleCursor)
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

	// Live-MV cursor on A. When cursorTopic == "" we use CursorModified
	// (key-range + time-window); when set, CursorTopic delivers
	// Enter/Update/Leave diffs for the topic. Either way cursorView is
	// the materialized result set; pruneLoop trims entries whose ModTime
	// has rolled out of the time window (rolling mode only), and Left
	// entries fade out after ~2s so the attribute-flip story is visible.
	//
	// Two modes:
	//   "rolling": admit entries whose modTime is in (now-windowSec, now).
	//   "fixed":   admit entries whose createdAt is in [createdLo, createdHi).
	// Fixed mode is stable over time — createdAt doesn't ratchet on
	// update, so the membership only changes via delete or (for topic
	// cursors) an attribute flip.
	cursorClose func()
	cursorTopic string
	cursorMode  string // "rolling" | "fixed"
	windowSec   int
	createdLo   time.Time
	createdHi   time.Time // zero == open upper bound
	cursorView  map[string]cursorEntry

	subs  map[chan []byte]struct{}
	dirty chan struct{}
}

type cursorEntry struct {
	Key       string
	Value     string
	Origin    string
	Clock     uint64
	ModTime   time.Time
	CreatedAt time.Time
	Topic     string
	Left      bool      // true once a TopicLeave evicted this row; displayed as a fading "ghost" until pruned
	LeftAt    time.Time
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
		a:          a,
		b:          b,
		bridge:     br,
		cursorMode: "rolling",
		windowSec:  30,
		cursorView: map[string]cursorEntry{},
		subs:       map[chan []byte]struct{}{},
		dirty:      make(chan struct{}, 1),
	}
	br.Start()
	return d
}

// ghostFade is how long a Leave-evicted row stays visible (greyed-out)
// before pruneLoop drops it — long enough for the user to see the flip.
const ghostFade = 2 * time.Second

func (d *demo) start() {
	go d.tail(d.a, &d.logA)
	go d.tail(d.b, &d.logB)
	go d.broadcastLoop()
	go d.pruneLoop()
	d.openCursor()
}

// pruneLoop has two jobs: drop Left ghosts once their fade window has
// elapsed, and (in rolling mode) evict entries whose ModTime has rolled
// out of the time window. Fixed mode doesn't need the second pass — a
// row's createdAt doesn't change, so membership is stable except for
// Left ghosts. Either change invalidates the rendered view.
func (d *demo) pruneLoop() {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for range tick.C {
		d.mu.Lock()
		now := time.Now()
		rolling := d.cursorMode == "rolling"
		cutoff := now.Add(-time.Duration(d.windowSec) * time.Second)
		ghostCut := now.Add(-ghostFade)
		changed := false
		for k, e := range d.cursorView {
			if e.Left && e.LeftAt.Before(ghostCut) {
				delete(d.cursorView, k)
				changed = true
				continue
			}
			if rolling && !e.Left && e.ModTime.Before(cutoff) {
				delete(d.cursorView, k)
				changed = true
			}
		}
		d.mu.Unlock()
		if changed {
			d.markDirty()
		}
	}
}

// openCursor (re)opens the live-MV cursor on A using the demo's
// current mode/topic/window/creation-range fields. Rolling mode passes
// a time lower bound to the shard so the snapshot is pre-filtered by
// modTime; fixed mode asks for everything and filters the result by
// createdAt in [createdLo, createdHi). If cursorTopic is non-empty we
// also branch to the TopicSub path so Enter/Update/Leave diffs drive
// the view; otherwise we use the plain key-range subscription.
// The old cursor (if any) is closed via the cursorClose hook.
func (d *demo) openCursor() {
	d.mu.Lock()
	if d.cursorClose != nil {
		d.cursorClose()
		d.cursorClose = nil
	}
	topic := d.cursorTopic
	rolling := d.cursorMode == "rolling"
	cLo, cHi := d.createdLo, d.createdHi
	var tLo time.Time
	if rolling {
		tLo = time.Now().Add(-time.Duration(d.windowSec) * time.Second)
	}
	d.cursorView = map[string]cursorEntry{}

	admitCreated := func(ca time.Time) bool {
		if rolling {
			return true
		}
		if !ca.Before(cHi) && !cHi.IsZero() {
			return false
		}
		return !ca.Before(cLo)
	}

	if topic == "" {
		cur := d.a.CursorModified("", rangeMaxKey, tLo, time.Time{})
		for _, m := range cur.Snapshot {
			if !admitCreated(m.CreatedAt) {
				continue
			}
			d.cursorView[m.Key] = cursorEntry{
				Key: m.Key, Value: string(m.Value),
				Origin: m.Stamp.Origin, Clock: m.Stamp.Lamport,
				ModTime: m.ModTime, CreatedAt: m.CreatedAt,
			}
		}
		d.cursorClose = cur.Sub.Close
		sub := cur.Sub
		d.mu.Unlock()
		go d.tailModified(sub)
	} else {
		cur := d.a.CursorTopic(topic, tLo, time.Time{})
		for _, m := range cur.Snapshot {
			if !admitCreated(m.CreatedAt) {
				continue
			}
			d.cursorView[m.Key] = cursorEntry{
				Key: m.Key, Value: string(m.Value),
				Origin: m.Stamp.Origin, Clock: m.Stamp.Lamport,
				ModTime: m.ModTime, CreatedAt: m.CreatedAt, Topic: topic,
			}
		}
		d.cursorClose = cur.Sub.Close
		sub := cur.Sub
		d.mu.Unlock()
		go d.tailTopic(sub, topic)
	}
	d.markDirty()
}

// inCreatedRange reports whether ca falls in the current fixed cursor
// range. Caller must hold d.mu. Only meaningful in fixed mode.
func (d *demo) inCreatedRange(ca time.Time) bool {
	if !d.createdHi.IsZero() && !ca.Before(d.createdHi) {
		return false
	}
	return !ca.Before(d.createdLo)
}

// tailModified mirrors physical patches into cursorView. Deletes drop
// the row; inserts/updates upsert it. In rolling mode the time filter
// is enforced by pruneLoop; in fixed mode admission here is gated by
// the patch's createdAt.
func (d *demo) tailModified(sub *joindb.Subscription) {
	for {
		p, ok := sub.Next()
		if !ok {
			return
		}
		d.mu.Lock()
		// Silently drop events from a cursor that's been superseded.
		if d.cursorTopic != "" {
			d.mu.Unlock()
			continue
		}
		switch p.Kind {
		case joindb.PatchDelete:
			delete(d.cursorView, p.Key)
		default:
			if d.cursorMode == "fixed" && !d.inCreatedRange(p.CreatedAt) {
				// Out-of-range insert/update. If the key was previously
				// in-range (e.g. reinsert after delete ratcheted its
				// createdAt forward), drop it.
				delete(d.cursorView, p.Key)
				d.mu.Unlock()
				d.markDirty()
				continue
			}
			d.cursorView[p.Key] = cursorEntry{
				Key: p.Key, Value: string(p.Value),
				Origin: p.Stamp.Origin, Clock: p.Stamp.Lamport,
				ModTime: p.ModTime, CreatedAt: p.CreatedAt, Topic: p.Topic,
			}
		}
		d.mu.Unlock()
		d.markDirty()
	}
}

// tailTopic applies TopicEvents to cursorView. Leave marks the row as
// a Left ghost (pruneLoop will drop it once ghostFade elapses); Enter
// and Update upsert. Because the server produces Leave whenever a
// row's topic changes *or* the row is deleted, cursorView always
// matches "what `CursorTopic(topic, ...)` would return if reopened."
func (d *demo) tailTopic(sub *joindb.TopicSub, topic string) {
	for {
		e, ok := sub.Next()
		if !ok {
			return
		}
		d.mu.Lock()
		if d.cursorTopic != topic {
			d.mu.Unlock()
			continue
		}
		switch e.Kind {
		case joindb.TopicLeave:
			if ent, ok := d.cursorView[e.Key]; ok {
				ent.Left = true
				ent.LeftAt = time.Now()
				d.cursorView[e.Key] = ent
			}
		case joindb.TopicEnter, joindb.TopicUpdate:
			if d.cursorMode == "fixed" && !d.inCreatedRange(e.CreatedAt) {
				// Enter outside fixed creation range: not our row.
				// Update outside range: shouldn't happen (createdAt is
				// stable), but treat as an eviction to stay consistent.
				delete(d.cursorView, e.Key)
				d.mu.Unlock()
				d.markDirty()
				continue
			}
			d.cursorView[e.Key] = cursorEntry{
				Key: e.Key, Value: string(e.Value),
				Origin: e.Stamp.Origin, Clock: e.Stamp.Lamport,
				ModTime: e.ModTime, CreatedAt: e.CreatedAt, Topic: e.Topic,
			}
		}
		d.mu.Unlock()
		d.markDirty()
	}
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
	A           sideJSON           `json:"a"`
	B           sideJSON           `json:"b"`
	Partitioned bool               `json:"partitioned"`
	Bridge      joindb.BridgeStats `json:"bridgeStats"`
	LogA        []patchEntry       `json:"logA"`
	LogB        []patchEntry       `json:"logB"`
	Divergence  []divJSON          `json:"divergence"`
	Cursor      cursorJSON         `json:"cursor"`
}

type cursorJSON struct {
	Mode      string            `json:"mode"` // "rolling" | "fixed"
	WindowSec int               `json:"windowSec"`
	CreatedLo int64             `json:"createdLo"` // millis; 0 when unset
	CreatedHi int64             `json:"createdHi"` // millis; 0 == open upper bound
	Topic     string            `json:"topic"`
	Now       int64             `json:"now"`
	Entries   []cursorEntryJSON `json:"entries"`
}

type cursorEntryJSON struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Origin    string `json:"origin"`
	Clock     uint64 `json:"clock"`
	ModTime   int64  `json:"modTime"`
	CreatedAt int64  `json:"createdAt"`
	Topic     string `json:"topic,omitempty"`
	Left      bool   `json:"left,omitempty"`
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
	Topic  string `json:"topic,omitempty"`
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
	windowSec := d.windowSec
	topic := d.cursorTopic
	mode := d.cursorMode
	createdLo := d.createdLo
	createdHi := d.createdHi
	rolling := mode == "rolling"
	cutoff := time.Now().Add(-time.Duration(windowSec) * time.Second)
	curEntries := make([]cursorEntryJSON, 0, len(d.cursorView))
	for _, e := range d.cursorView {
		// Left ghosts are always shown until pruned.
		if rolling && !e.Left && e.ModTime.Before(cutoff) {
			continue
		}
		curEntries = append(curEntries, cursorEntryJSON{
			Key:       e.Key,
			Value:     e.Value,
			Origin:    e.Origin,
			Clock:     e.Clock,
			ModTime:   e.ModTime.UnixMilli(),
			CreatedAt: unixMilliOrZero(e.CreatedAt),
			Topic:     e.Topic,
			Left:      e.Left,
		})
	}
	d.mu.Unlock()

	sort.Slice(curEntries, func(i, j int) bool {
		return curEntries[i].ModTime > curEntries[j].ModTime
	})

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
		Cursor: cursorJSON{
			Mode:      mode,
			WindowSec: windowSec,
			CreatedLo: unixMilliOrZero(createdLo),
			CreatedHi: unixMilliOrZero(createdHi),
			Topic:     topic,
			Now:       time.Now().UnixMilli(),
			Entries:   curEntries,
		},
	}
	buf, _ := json.Marshal(s)
	return buf
}

// unixMilliOrZero converts a time to unix millis, returning 0 for the
// zero time (so the UI can treat 0 as "unset").
func unixMilliOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func collectSide(s *joindb.Shard) sideJSON {
	pairs := s.Range("", rangeMaxKey)
	kvs := make([]kvJSON, 0, len(pairs))
	for _, p := range pairs {
		st := s.Stamp(p.Key)
		kvs = append(kvs, kvJSON{
			Key:    p.Key,
			Value:  string(p.Value),
			Topic:  s.Topic(p.Key),
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
	var req struct{ Side, Key, Value, Topic string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s := d.resolveSide(req.Side)
	if s == nil || req.Key == "" {
		http.Error(w, "side and key required", 400)
		return
	}
	s.PutTopic(req.Key, []byte(req.Value), req.Topic)
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
	// A handful of topics so the topic cursor has something to filter on.
	topics := []string{"news", "sports", "tech", "", ""}
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(len(req.Side))))
	for range req.N {
		k := fmt.Sprintf("k%03d", rng.IntN(60))
		if rng.IntN(5) == 0 {
			s.Delete(k)
			continue
		}
		topic := topics[rng.IntN(len(topics))]
		s.PutTopic(k, fmt.Appendf(nil, "%s-%d", s.ID(), rng.IntN(1_000_000)), topic)
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

// handleCursor reconfigures the cursor on A. Mode is "rolling" (default)
// or "fixed"; rolling uses WindowSec against modTime, fixed uses
// [CreatedLo, CreatedHi) against createdAt. The old cursor is closed
// (its tailer exits); a fresh CursorModified or CursorTopic is taken
// atomically against A's current LSN.
func (d *demo) handleCursor(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode      string
		WindowSec int
		CreatedLo int64 // unix millis; 0 == unset
		CreatedHi int64 // unix millis; 0 == open upper bound
		Topic     string
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	d.mu.Lock()
	if req.Mode == "" {
		req.Mode = d.cursorMode
	}
	if req.Mode != "rolling" && req.Mode != "fixed" {
		req.Mode = "rolling"
	}
	if req.WindowSec <= 0 {
		req.WindowSec = d.windowSec
	}
	if req.WindowSec < 1 {
		req.WindowSec = 1
	}
	if req.WindowSec > 3600 {
		req.WindowSec = 3600
	}
	d.cursorMode = req.Mode
	d.windowSec = req.WindowSec
	d.cursorTopic = req.Topic
	if req.CreatedLo > 0 {
		d.createdLo = time.UnixMilli(req.CreatedLo)
	} else {
		d.createdLo = time.Time{}
	}
	if req.CreatedHi > 0 {
		d.createdHi = time.UnixMilli(req.CreatedHi)
	} else {
		d.createdHi = time.Time{}
	}
	d.mu.Unlock()
	d.openCursor()
	w.WriteHeader(204)
}

func (d *demo) handleReset(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	if d.cursorClose != nil {
		d.cursorClose()
		d.cursorClose = nil
	}
	d.bridge.Stop()
	d.a = joindb.NewShardID("A")
	d.b = joindb.NewShardID("B")
	d.bridge = joindb.NewBridge(d.a, d.b)
	d.logA = nil
	d.logB = nil
	d.partitioned = false
	d.cursorView = map[string]cursorEntry{}
	a := d.a
	b := d.b
	d.mu.Unlock()
	d.bridge.Start()
	go d.tail(a, &d.logA)
	go d.tail(b, &d.logB)
	d.openCursor()
	d.markDirty()
	w.WriteHeader(204)
}
