package joindb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
)

// Index is a content-addressed, immutable sorted key-value tree whose nodes
// live in a NodeStore. Chunk boundaries are content-defined (FNV hash of
// key), so an insert or delete that doesn't land on a boundary only rewrites
// chunks on the path from its leaf to the root — siblings keep their hashes.
//
// Higher tree levels use sparser boundary masks (one extra zero bit per level)
// so every level strictly reduces the child count, and depth stays O(log N).
type Index struct {
	Root  Hash
	Store NodeStore
	Bits  uint // leaf-level boundary = fnv(key) & ((1<<Bits)-1) == 0
}

const (
	kindLeaf     byte = 0x01
	kindInternal byte = 0x02

	defaultBits uint = 4 // avg 16 keys/leaf, 32 refs/L1-internal, ...
)

// BuildIndex builds a new index from sorted pairs, storing nodes in store.
// pairs MUST be sorted by Key and contain no duplicates. An empty pairs
// slice yields an index with the zero Root hash.
func BuildIndex(pairs []KV, store NodeStore) *Index {
	return BuildIndexWithBits(pairs, store, defaultBits)
}

func BuildIndexWithBits(pairs []KV, store NodeStore, bits uint) *Index {
	idx := &Index{Store: store, Bits: bits}
	if len(pairs) == 0 {
		return idx
	}

	var level []childRef
	start := 0
	for i := range pairs {
		if i == len(pairs)-1 || isBoundary(pairs[i].Key, 0, bits) {
			chunk := pairs[start : i+1]
			h := storeLeaf(store, chunk)
			level = append(level, childRef{MinKey: chunk[0].Key, Hash: h})
			start = i + 1
		}
	}

	for L := 1; len(level) > 1; L++ {
		var next []childRef
		gstart := 0
		for i := range level {
			if i == len(level)-1 || isBoundary(level[i].MinKey, L, bits) {
				group := level[gstart : i+1]
				h := storeInternal(store, group)
				next = append(next, childRef{MinKey: group[0].MinKey, Hash: h})
				gstart = i + 1
			}
		}
		if len(next) == len(level) {
			// No splits fired at this level; force a single parent to avoid
			// an infinite loop on pathological key sets.
			h := storeInternal(store, level)
			level = []childRef{{MinKey: level[0].MinKey, Hash: h}}
			continue
		}
		level = next
	}

	idx.Root = level[0].Hash
	return idx
}

// isBoundary: at level L, boundary fires when fnv(key) has (bits+L) low
// zero bits. Higher levels are rarer, so internal nodes aggregate children
// without exploding depth.
func isBoundary(key string, level int, bits uint) bool {
	h := fnv.New64a()
	h.Write([]byte(key))
	mask := uint64(1)<<(bits+uint(level)) - 1
	return h.Sum64()&mask == 0
}

type childRef struct {
	MinKey string
	Hash   Hash
}

func storeLeaf(store NodeStore, pairs []KV) Hash {
	var buf bytes.Buffer
	buf.WriteByte(kindLeaf)
	putUvarint(&buf, uint64(len(pairs)))
	for _, kv := range pairs {
		putUvarint(&buf, uint64(len(kv.Key)))
		buf.WriteString(kv.Key)
		putUvarint(&buf, uint64(len(kv.Value)))
		buf.Write(kv.Value)
	}
	b := buf.Bytes()
	h := hashOf(b)
	store.Put(h, b)
	return h
}

func storeInternal(store NodeStore, children []childRef) Hash {
	var buf bytes.Buffer
	buf.WriteByte(kindInternal)
	putUvarint(&buf, uint64(len(children)))
	for _, c := range children {
		putUvarint(&buf, uint64(len(c.MinKey)))
		buf.WriteString(c.MinKey)
		buf.Write(c.Hash[:])
	}
	b := buf.Bytes()
	h := hashOf(b)
	store.Put(h, b)
	return h
}

func putUvarint(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

// Get returns the value for key, descending from the root.
func (idx *Index) Get(key string) ([]byte, bool) {
	if idx.Root.IsZero() {
		return nil, false
	}
	return idx.descendGet(idx.Root, key)
}

func (idx *Index) descendGet(h Hash, key string) ([]byte, bool) {
	node, err := idx.loadNode(h)
	if err != nil {
		return nil, false
	}
	switch n := node.(type) {
	case *leafNode:
		i := sort.Search(len(n.Pairs), func(i int) bool { return n.Pairs[i].Key >= key })
		if i < len(n.Pairs) && n.Pairs[i].Key == key {
			v := n.Pairs[i].Value
			out := make([]byte, len(v))
			copy(out, v)
			return out, true
		}
		return nil, false
	case *internalNode:
		i := sort.Search(len(n.Children), func(i int) bool { return n.Children[i].MinKey > key })
		if i == 0 {
			return nil, false
		}
		return idx.descendGet(n.Children[i-1].Hash, key)
	}
	return nil, false
}

// Range returns pairs with lo <= key < hi, in ascending key order.
func (idx *Index) Range(lo, hi string) []KV {
	if lo >= hi || idx.Root.IsZero() {
		return nil
	}
	var out []KV
	idx.descendRange(idx.Root, lo, hi, &out)
	return out
}

func (idx *Index) descendRange(h Hash, lo, hi string, out *[]KV) {
	node, err := idx.loadNode(h)
	if err != nil {
		return
	}
	switch n := node.(type) {
	case *leafNode:
		for _, kv := range n.Pairs {
			if kv.Key < lo {
				continue
			}
			if kv.Key >= hi {
				break
			}
			buf := make([]byte, len(kv.Value))
			copy(buf, kv.Value)
			*out = append(*out, KV{Key: kv.Key, Value: buf})
		}
	case *internalNode:
		for i, c := range n.Children {
			if c.MinKey >= hi {
				break
			}
			// skip children whose entire key range is below lo
			if i+1 < len(n.Children) && n.Children[i+1].MinKey <= lo {
				continue
			}
			idx.descendRange(c.Hash, lo, hi, out)
		}
	}
}

type leafNode struct {
	Pairs []KV
}

type internalNode struct {
	Children []childRef
}

func (idx *Index) loadNode(h Hash) (any, error) {
	raw, ok := idx.Store.Get(h)
	if !ok {
		return nil, fmt.Errorf("node %s missing from store", h)
	}
	return parseNode(raw)
}

func parseNode(raw []byte) (any, error) {
	if len(raw) < 1 {
		return nil, fmt.Errorf("node too short")
	}
	kind := raw[0]
	p := raw[1:]
	switch kind {
	case kindLeaf:
		n, r := binary.Uvarint(p)
		if r <= 0 {
			return nil, fmt.Errorf("bad leaf count")
		}
		p = p[r:]
		pairs := make([]KV, 0, n)
		for range n {
			kl, r := binary.Uvarint(p)
			if r <= 0 || uint64(len(p[r:])) < kl {
				return nil, fmt.Errorf("bad key len")
			}
			p = p[r:]
			key := string(p[:kl])
			p = p[kl:]
			vl, r2 := binary.Uvarint(p)
			if r2 <= 0 || uint64(len(p[r2:])) < vl {
				return nil, fmt.Errorf("bad value len")
			}
			p = p[r2:]
			val := make([]byte, vl)
			copy(val, p[:vl])
			p = p[vl:]
			pairs = append(pairs, KV{Key: key, Value: val})
		}
		return &leafNode{Pairs: pairs}, nil
	case kindInternal:
		n, r := binary.Uvarint(p)
		if r <= 0 {
			return nil, fmt.Errorf("bad internal count")
		}
		p = p[r:]
		children := make([]childRef, 0, n)
		for range n {
			kl, r := binary.Uvarint(p)
			if r <= 0 || uint64(len(p[r:])) < kl+32 {
				return nil, fmt.Errorf("bad child ref")
			}
			p = p[r:]
			key := string(p[:kl])
			p = p[kl:]
			var h Hash
			copy(h[:], p[:32])
			p = p[32:]
			children = append(children, childRef{MinKey: key, Hash: h})
		}
		return &internalNode{Children: children}, nil
	}
	return nil, fmt.Errorf("unknown node kind %d", kind)
}
