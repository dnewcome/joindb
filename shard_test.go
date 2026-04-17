package joindb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	s := NewShard()
	if _, ok := s.Get("missing"); ok {
		t.Fatal("Get on missing key returned ok=true")
	}
	s.Put("a", []byte("1"))
	s.Put("b", []byte("2"))
	if v, ok := s.Get("a"); !ok || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("Get(a) = %v, %v; want 1, true", v, ok)
	}
	if ok := s.Delete("a"); !ok {
		t.Fatal("Delete(a) returned false")
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("Get(a) after Delete returned ok=true")
	}
	if ok := s.Delete("a"); ok {
		t.Fatal("Delete(a) a second time returned true")
	}
}

func TestPutOverwrite(t *testing.T) {
	s := NewShard()
	s.Put("k", []byte("v1"))
	s.Put("k", []byte("v2"))
	if v, _ := s.Get("k"); !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("got %q, want v2", v)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d; want 1", s.Len())
	}
}

func TestRangeOrder(t *testing.T) {
	s := NewShard()
	for _, k := range []string{"d", "b", "a", "c", "e"} {
		s.Put(k, []byte(k))
	}
	got := s.Range("b", "e")
	want := []string{"b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("len %d want %d", len(got), len(want))
	}
	for i, kv := range got {
		if kv.Key != want[i] {
			t.Fatalf("pos %d: got %q want %q", i, kv.Key, want[i])
		}
	}
}

func TestRangeEmpty(t *testing.T) {
	s := NewShard()
	s.Put("a", nil)
	if got := s.Range("b", "c"); len(got) != 0 {
		t.Fatalf("want empty range for non-overlap, got %v", got)
	}
	if got := s.Range("a", "a"); len(got) != 0 {
		t.Fatalf("want empty range for lo==hi, got %v", got)
	}
}

func TestRangeBounds(t *testing.T) {
	s := NewShard()
	for _, k := range []string{"a", "b", "c"} {
		s.Put(k, []byte(k))
	}
	got := s.Range("", "z")
	if len(got) != 3 {
		t.Fatalf("want 3, got %d", len(got))
	}
	got = s.Range("a", "c")
	keys := make([]string, len(got))
	for i, kv := range got {
		keys[i] = kv.Key
	}
	if !eqStrings(keys, []string{"a", "b"}) {
		t.Fatalf("want [a b] (hi exclusive), got %v", keys)
	}
}

// TestValueIsolation verifies the shard neither aliases caller-provided
// slices nor hands out references to its own storage.
func TestValueIsolation(t *testing.T) {
	s := NewShard()
	v := []byte{1, 2, 3}
	s.Put("k", v)
	v[0] = 99
	got, _ := s.Get("k")
	if got[0] != 1 {
		t.Fatalf("shard value mutated by caller's slice: got %v", got)
	}
	got[1] = 99
	got2, _ := s.Get("k")
	if got2[1] != 2 {
		t.Fatalf("shard value mutated via returned slice: got %v", got2)
	}
}

func TestRangeRandomized(t *testing.T) {
	s := NewShard()
	rng := rand.New(rand.NewPCG(42, 42))
	const N = 500
	seen := map[string]bool{}
	for range N {
		k := fmt.Sprintf("k%05d", rng.IntN(100000))
		s.Put(k, []byte(k))
		seen[k] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for trial := range 20 {
		a := rng.IntN(len(keys))
		b := a + rng.IntN(len(keys)-a+1)
		var lo, hi string
		if a < len(keys) {
			lo = keys[a]
		} else {
			lo = "\xff"
		}
		if b < len(keys) {
			hi = keys[b]
		} else {
			hi = "\xff\xff"
		}
		got := s.Range(lo, hi)
		want := keys[a:b]
		if len(got) != len(want) {
			t.Fatalf("trial %d [%s,%s): len %d want %d", trial, lo, hi, len(got), len(want))
		}
		for i, kv := range got {
			if kv.Key != want[i] {
				t.Fatalf("trial %d pos %d: got %q want %q", trial, i, kv.Key, want[i])
			}
		}
	}
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
