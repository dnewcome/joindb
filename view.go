package joindb

// NodeKind labels a parsed index node.
type NodeKind int

const (
	NodeLeaf NodeKind = iota + 1
	NodeInternal
)

func (k NodeKind) String() string {
	switch k {
	case NodeLeaf:
		return "leaf"
	case NodeInternal:
		return "internal"
	}
	return "?"
}

// NodeInfo is a read-only view of a single index node, exported so tools
// outside the package (demos, visualizers) can walk the Merkle tree
// without depending on the internal parsed types.
type NodeInfo struct {
	Hash     Hash
	Kind     NodeKind
	Pairs    []KV        // populated when Kind == NodeLeaf
	Children []ChildInfo // populated when Kind == NodeInternal
	RawSize  int
}

// ChildInfo is a reference from an internal node to one of its children.
type ChildInfo struct {
	MinKey string
	Hash   Hash
}

// LoadNode fetches and parses a single index node from store.
func LoadNode(store NodeStore, h Hash) (NodeInfo, error) {
	raw, ok := store.Get(h)
	if !ok {
		return NodeInfo{}, errNodeMissing{h}
	}
	parsed, err := parseNode(raw)
	if err != nil {
		return NodeInfo{}, err
	}
	info := NodeInfo{Hash: h, RawSize: len(raw)}
	switch n := parsed.(type) {
	case *leafNode:
		info.Kind = NodeLeaf
		info.Pairs = n.Pairs
	case *internalNode:
		info.Kind = NodeInternal
		info.Children = make([]ChildInfo, len(n.Children))
		for i, c := range n.Children {
			info.Children[i] = ChildInfo{MinKey: c.MinKey, Hash: c.Hash}
		}
	}
	return info, nil
}

type errNodeMissing struct{ h Hash }

func (e errNodeMissing) Error() string { return "node " + e.h.String() + " missing from store" }
