package watch

import (
	"sync"

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/internal/buscore"
)

// Receiver is a receive-side handle watching exactly one key, intended for one
// consumer goroutine. It holds a single slot rather than a queue: a value that
// Accept takes overwrites the one before it, so a slow reader skips to the
// current value instead of building a backlog.
type Receiver[K comparable, V any] struct {
	s   *shared[K, V]
	key K

	// val is the current value and version counts the values Accept has taken.
	// lastSeen is the read position: val is unread exactly while version >
	// lastSeen. Watch seeds them equal, so the caller's own initial is never
	// delivered back to it.
	//
	// lastSeen lives here under s.mu rather than in the reading goroutine: a
	// receiver with a Chan feeder is read by two goroutines, so single-consumer
	// ownership is an intent, not an invariant.
	val      V
	version  uint64
	lastSeen uint64

	notify  chan struct{} // closed+replaced to wake this receiver's parked readers
	waiters int           // parked readers; gates notify allocation
	done    buscore.CloseOnce

	chOnce sync.Once
	ch     chan gobus.Event[K, V]
}

// unreadLocked reports whether the slot holds a value this receiver has not
// taken. Caller holds s.mu.
func (rx *Receiver[K, V]) unreadLocked() bool { return rx.version > rx.lastSeen }

// takeLocked marks the slot read and returns its value. Caller holds s.mu, and
// must have found unreadLocked true.
func (rx *Receiver[K, V]) takeLocked() gobus.Event[K, V] {
	rx.lastSeen = rx.version
	return gobus.Event[K, V]{Key: rx.key, Value: rx.val}
}

// TryRecv returns the current value if this receiver has not taken it,
// [gobus.ErrEmpty] if nothing has changed since it subscribed, or
// [gobus.ErrClosed] if the receiver or hub is closed.
func (rx *Receiver[K, V]) TryRecv() (gobus.Event[K, V], error) {
	var zero gobus.Event[K, V]
	if rx.done.IsClosed() {
		return zero, gobus.ErrClosed
	}
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	// rx.done can flip between the lock-free check above and acquiring mu;
	// Close holds mu before closing done, so a re-check here is race-free.
	if rx.done.IsClosed() {
		return zero, gobus.ErrClosed
	}
	if rx.unreadLocked() {
		return rx.takeLocked(), nil
	}
	return zero, gobus.ErrEmpty
}

// Close is the unwatch: it closes this handle, discards any unread value and
// drops the key from the hub once no other receiver watches it. Other
// receivers and the sender are unaffected. Idempotent.
func (rx *Receiver[K, V]) Close() {
	// Close under mu so a concurrent read that acquired mu first cannot hand
	// back a value to a now-closed receiver: readers re-check rx.done after
	// taking mu, and that check is stable only while Close serializes here too.
	rx.s.mu.Lock()
	rx.done.Close()
	rx.s.deregisterLocked(rx)
	rx.s.mu.Unlock()
}
