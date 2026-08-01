package watch

import "sync"

// shared is the hub state common to the sender and every receiver. One mutex
// guards all of it: Send fans a write across the receivers watching a key and
// each read takes its own slot, so a single lock keeps accept/write/read
// consistent without per-receiver locking races.
type shared[K comparable, V any] struct {
	mu     sync.Mutex
	accept Accept[V] // nil = accept every value

	// index is the send-side lookup: only the receivers watching a key are
	// touched by a Send for it. receivers is the whole set, for Hub.Close and
	// for the live count. Both are mutated at the same sites.
	index     map[K]map[*Receiver[K, V]]struct{}
	receivers map[*Receiver[K, V]]struct{}

	txClosed  bool
	hubClosed bool
}

// registerLocked adds rx to both maps. Caller holds s.mu.
func (s *shared[K, V]) registerLocked(rx *Receiver[K, V]) {
	set := s.index[rx.key]
	if set == nil {
		set = make(map[*Receiver[K, V]]struct{})
		s.index[rx.key] = set
	}
	set[rx] = struct{}{}
	s.receivers[rx] = struct{}{}
}

// deregisterLocked drops rx from both maps, removing the key entirely once no
// receiver watches it — which is what bounds hub memory by the live watch set.
// It rides with Receiver.Close and with every terminal verdict. Caller holds
// s.mu.
func (s *shared[K, V]) deregisterLocked(rx *Receiver[K, V]) {
	if set := s.index[rx.key]; set != nil {
		delete(set, rx)
		if len(set) == 0 {
			delete(s.index, rx.key)
		}
	}
	delete(s.receivers, rx)
}
