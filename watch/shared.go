package watch

import (
	"sync"
	"sync/atomic"
)

// sendPoisoned is the liveReceivers value meaning "the send side is closed".
// It is negative so it can never be mistaken for a receiver count.
const sendPoisoned = -1

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

	// liveReceivers is a lock-free copy of len(receivers), written only under
	// mu at every site that mutates that map and read without mu by the send
	// side. A publisher reading zero may skip the lock: there is nobody to fan
	// out to, and sendLocked has no other effect.
	//
	// Only one direction of error is safe. Over-reporting is free — the
	// publisher takes the lock and finds nothing. Under-reporting loses a value
	// permanently, because a state bus has no replay: the next send for that
	// key overwrites a slot the subscriber was never told had been skipped.
	// That asymmetry is why the count is derived from len(receivers) rather
	// than incremented and decremented — a derived value cannot drift below the
	// truth.
	//
	// Closedness is folded into this one field rather than mirrored into a
	// second atomic: Sender.Close and Hub.Close store sendPoisoned and no later
	// write clears it. That is what lets txClosed stay a plain bool with a
	// single access discipline, read only under mu, on the path the poison
	// guarantees a closed hub takes.
	liveReceivers atomic.Int64

	// forTestingBeforeSendLock, if non-nil, runs on every send path about to
	// take mu, after SendContext has taken ctx's Done channel and before the
	// lock is acquired. A test can therefore land a cancellation in the window
	// where the send is waiting for the lock. Hub-wide and read outside mu — it
	// has to be, since the window it opens is the wait for mu itself — so arm
	// it only while no send is in flight. nil in production.
	forTestingBeforeSendLock func()
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
	s.syncLiveLocked()
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
	s.syncLiveLocked()
}

// syncLiveLocked refreshes the lock-free count from the map that owns the
// truth. Call it at every site that mutates s.receivers. Caller holds s.mu.
//
// The early return is load-bearing, not defensive: a Receiver.Close after a
// Sender.Close deregisters through here, and without the guard it would write
// a zero over the poison and hand a closed hub back to the fast path.
func (s *shared[K, V]) syncLiveLocked() {
	if s.liveReceivers.Load() == sendPoisoned {
		return
	}
	s.liveReceivers.Store(int64(len(s.receivers)))
}

// poisonLiveLocked retires the send fast path for the life of the hub, so every
// later send reaches sendLocked and is answered from txClosed. Caller holds
// s.mu.
func (s *shared[K, V]) poisonLiveLocked() { s.liveReceivers.Store(sendPoisoned) }
