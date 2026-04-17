package joindb

import "fmt"

// Stamp is a per-write Lamport stamp with an origin ID for deterministic
// tie-breaking. Two writes to the same key resolve last-writer-wins by
// comparing Stamps: higher Lamport wins; ties broken by Origin.
//
// The zero Stamp (Lamport=0, Origin="") dominates nothing; it's used to
// mean "no prior writer" when a key has never been written.
type Stamp struct {
	Lamport uint64
	Origin  string
}

// Dominates reports whether s should overwrite prior. Strict; a Stamp
// does not dominate itself.
func (s Stamp) Dominates(prior Stamp) bool {
	if s.Lamport != prior.Lamport {
		return s.Lamport > prior.Lamport
	}
	return s.Origin > prior.Origin
}

func (s Stamp) IsZero() bool { return s.Lamport == 0 && s.Origin == "" }

func (s Stamp) String() string { return fmt.Sprintf("%s@%d", s.Origin, s.Lamport) }
