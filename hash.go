package joindb

import (
	"crypto/sha256"
	"encoding/hex"
)

// Hash is the content-address of a stored chunk (node). SHA-256.
type Hash [32]byte

func hashOf(data []byte) Hash {
	return sha256.Sum256(data)
}

// String returns a short hex prefix for logs/tests.
func (h Hash) String() string {
	return hex.EncodeToString(h[:8])
}

// IsZero reports whether h is the zero hash (used to mark an empty index).
func (h Hash) IsZero() bool {
	return h == Hash{}
}
