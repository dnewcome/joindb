package joindb

import "fmt"

// SyncStats records what a single Sync call pulled from source.
type SyncStats struct {
	ChunksFetched int
	BytesFetched  int
}

// Sync pulls the subtree rooted at root from source into local. It walks
// top-down: if local already has a node's hash, the whole subtree is
// identical and is skipped. Otherwise the node is fetched, stored, parsed,
// and its children are visited recursively.
//
// This is the minimum viable anti-entropy: N chunks changed → O(N + depth)
// hashes walked, O(N + depth) nodes fetched.
func Sync(local NodeStore, source NodeStore, root Hash) (SyncStats, error) {
	var stats SyncStats
	if root.IsZero() {
		return stats, nil
	}
	if err := syncWalk(local, source, root, &stats); err != nil {
		return stats, err
	}
	return stats, nil
}

func syncWalk(local, source NodeStore, h Hash, stats *SyncStats) error {
	if local.Has(h) {
		return nil
	}
	raw, ok := source.Get(h)
	if !ok {
		return fmt.Errorf("source missing node %s", h)
	}
	stats.ChunksFetched++
	stats.BytesFetched += len(raw)
	local.Put(h, raw)

	node, err := parseNode(raw)
	if err != nil {
		return fmt.Errorf("parse %s: %w", h, err)
	}
	if in, ok := node.(*internalNode); ok {
		for _, c := range in.Children {
			if err := syncWalk(local, source, c.Hash, stats); err != nil {
				return err
			}
		}
	}
	return nil
}
