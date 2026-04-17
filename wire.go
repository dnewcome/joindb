package joindb

// Wire request/response types for shard operations. Transport-agnostic:
// v0 uses in-process calls; M6 will serialize these over TCP. Keep them
// plain structs with exported fields so any codec (encoding/gob, JSON,
// or a handwritten framing) can round-trip them.

type GetReq struct {
	Key string
}

type GetResp struct {
	Value []byte
	Found bool
}

type PutReq struct {
	Key   string
	Value []byte
}

type PutResp struct{}

type DeleteReq struct {
	Key string
}

type DeleteResp struct {
	Found bool
}

type RangeReq struct {
	Lo, Hi string
}

type RangeResp struct {
	Pairs []KV
}
