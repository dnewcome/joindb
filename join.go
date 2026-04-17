package joindb

// JoinResult is an inner-join tuple on a shared key.
type JoinResult struct {
	Key    string
	ValueA []byte
	ValueB []byte
}

// InnerJoinKeys inner-joins bPairs against dataset A on equal key. If
// aLocal is non-nil every A lookup hits the replicated index locally,
// so remoteA sees zero Get calls at query time. Otherwise each lookup
// falls back to remoteA.Get.
//
// This is the M5 demo: "clients can form an extended database node, and
// operations locally work as if the remote database was local."
func InnerJoinKeys(bPairs []KV, aLocal *Index, remoteA *Server) []JoinResult {
	out := make([]JoinResult, 0, len(bPairs))
	for _, kv := range bPairs {
		var av []byte
		var ok bool
		if aLocal != nil {
			av, ok = aLocal.Get(kv.Key)
		} else {
			av, ok = remoteA.Get(kv.Key)
		}
		if ok {
			bCopy := make([]byte, len(kv.Value))
			copy(bCopy, kv.Value)
			out = append(out, JoinResult{Key: kv.Key, ValueA: av, ValueB: bCopy})
		}
	}
	return out
}
