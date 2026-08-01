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
