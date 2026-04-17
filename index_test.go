package joindb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
)

func TestIndexEmpty(t *testing.T) {
	idx := BuildIndex(nil, NewMemStore())
	if !idx.Root.IsZero() {
		t.Fatal("empty index should have zero Root")
	}
	if _, ok := idx.Get("x"); ok {
		t.Fatal("Get on empty index returned ok=true")
	}
	if got := idx.Range("a", "z"); len(got) != 0 {
		t.Fatalf("Range on empty index returned %v", got)
	}
}

func TestIndexGetAndRange(t *testing.T) {
	pairs := makeSortedPairs(300)
	idx := BuildIndex(pairs, NewMemStore())

	for _, kv := range pairs {
		v, ok := idx.Get(kv.Key)
		if !ok {
			t.Fatalf("missing key %q", kv.Key)
		}
		if !bytes.Equal(v, kv.Value) {
			t.Fatalf("Get(%q) = %q; want %q", kv.Key, v, kv.Value)
		}
	}
	if _, ok := idx.Get("__nope__"); ok {
		t.Fatal("Get on missing key returned ok=true")
	}

	rng := rand.New(rand.NewPCG(1, 2))
	for range 20 {
		a := rng.IntN(len(pairs))
		b := a + rng.IntN(len(pairs)-a+1)
		lo := "\x00"
		hi := "\xff\xff"
		if a < len(pairs) {
			lo = pairs[a].Key
		}
		if b < len(pairs) {
			hi = pairs[b].Key
		}
		got := idx.Range(lo, hi)
		want := pairs[a:b]
		if len(got) != len(want) {
			t.Fatalf("Range[%s,%s): got %d want %d", lo, hi, len(got), len(want))
		}
		for i, kv := range got {
			if kv.Key != want[i].Key || !bytes.Equal(kv.Value, want[i].Value) {
				t.Fatalf("pos %d: got %+v want %+v", i, kv, want[i])
			}
		}
	}
}

func TestIndexDeterminism(t *testing.T) {
	pairs := makeSortedPairs(500)
	idx1 := BuildIndex(pairs, NewMemStore())
	idx2 := BuildIndex(pairs, NewMemStore())
	if idx1.Root != idx2.Root {
		t.Fatalf("same pairs produced different roots: %s vs %s", idx1.Root, idx2.Root)
	}

	// Different value on one pair → different root.
	p2 := append([]KV(nil), pairs...)
	p2[100].Value = []byte("mutated")
	idx3 := BuildIndex(p2, NewMemStore())
	if idx3.Root == idx1.Root {
		t.Fatal("mutated pair produced same root")
	}
}

// TestIndexLocality is the key M2 property: inserting one key into an
// existing dataset should add ~O(tree depth) new chunks to the shared store.
// Unchanged subtrees retain their hashes, so the rebuild mostly hits
// existing entries.
func TestIndexLocality(t *testing.T) {
	store := NewMemStore()
	pairs := makeSortedPairs(2000)
	idx1 := BuildIndex(pairs, store)
	before := store.Len()

	// insert a key that doesn't collide with any existing one
	newKV := KV{Key: "zzz-new-key", Value: []byte("n")}
	p2 := append(append([]KV(nil), pairs...), newKV)
	sort.Slice(p2, func(i, j int) bool { return p2[i].Key < p2[j].Key })
	idx2 := BuildIndex(p2, store)

	added := store.Len() - before
	// With ~2000 keys at bits=4 (avg 16 leaf, 32 L1, 64 L2, ...), tree depth
	// is ~3. Allow generous headroom for boundary shifts in the adjacent
	// chunk, but it should nowhere near match the total chunk count.
	if added > 12 {
		t.Fatalf("insert touched %d new chunks; expected small (≤12). Total: %d",
			added, store.Len())
	}
	if idx1.Root == idx2.Root {
		t.Fatal("root hash unchanged after insert")
	}
	// Verify the new key is reachable in the new index.
	if v, ok := idx2.Get(newKV.Key); !ok || !bytes.Equal(v, newKV.Value) {
		t.Fatalf("new key not found in idx2: %q, %v", v, ok)
	}
	// And that old keys still resolve in the old index.
	if _, ok := idx1.Get(newKV.Key); ok {
		t.Fatal("new key unexpectedly present in idx1")
	}
	if v, ok := idx1.Get(pairs[42].Key); !ok || !bytes.Equal(v, pairs[42].Value) {
		t.Fatalf("old key disappeared from idx1: %v", ok)
	}
}

func TestIndexSingleKey(t *testing.T) {
	pairs := []KV{{Key: "only", Value: []byte("one")}}
	idx := BuildIndex(pairs, NewMemStore())
	if idx.Root.IsZero() {
		t.Fatal("single-key index has zero root")
	}
	v, ok := idx.Get("only")
	if !ok || !bytes.Equal(v, []byte("one")) {
		t.Fatalf("Get(only) = %q, %v", v, ok)
	}
}

func TestIndexValueIsolation(t *testing.T) {
	pairs := []KV{{Key: "k", Value: []byte{1, 2, 3}}}
	idx := BuildIndex(pairs, NewMemStore())
	got, _ := idx.Get("k")
	got[0] = 99
	got2, _ := idx.Get("k")
	if got2[0] != 1 {
		t.Fatalf("index value mutable via returned slice: %v", got2)
	}
}

func makeSortedPairs(n int) []KV {
	pairs := make([]KV, n)
	for i := range n {
		pairs[i] = KV{
			Key:   fmt.Sprintf("key-%06d", i),
			Value: fmt.Appendf(nil, "val-%d", i),
		}
	}
	return pairs
}
