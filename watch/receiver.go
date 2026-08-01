package watch

import (
	"context"
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

	// forTestingBeforeRecvLock and forTestingBeforeTryRecvLock, if non-nil, run
	// after the lock-free closed check and before taking s.mu, so tests can
	// exercise the close-wins-the-race re-check under the lock. nil in
	// production.
	forTestingBeforeRecvLock    func()
	forTestingBeforeTryRecvLock func()
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
	if rx.forTestingBeforeTryRecvLock != nil {
		rx.forTestingBeforeTryRecvLock()
	}
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	// rx.done can flip between the lock-free check above and acquiring mu;
	// Close holds mu before closing done, so a re-check here is race-free.
	if rx.done.IsClosed() {
		return zero, gobus.ErrClosed
	}
	// Drained-and-closed is terminal however it is observed, so this verdict
	// carries the same tear-down the blocking path's does, under the lock that
	// decided it.
	if rx.drainedLocked() {
		rx.s.deregisterLocked(rx)
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

// drainedLocked reports whether the stream has ended: the sender is gone and
// this receiver has taken the final value, so nothing can arrive again. It is
// exactly when the read below would fail. Caller holds s.mu.
func (rx *Receiver[K, V]) drainedLocked() bool { return rx.s.txClosed && !rx.unreadLocked() }

// Recv blocks until a value this receiver has not taken is available, then
// returns it. It returns [gobus.ErrClosed] once the receiver or hub is closed,
// or once the sender is closed and the final value has been taken.
func (rx *Receiver[K, V]) Recv() (gobus.Event[K, V], error) {
	return rx.recvLoop(context.Background())
}

// recvLoop is the shared blocking-recv implementation. Recv passes
// context.Background() to opt out of cancellation — Background's Done()
// returns nil, and a nil channel in a select arm is never ready, so the
// cancellation check falls straight through to its default on that path.
//
// The whole closed > cancelled > value precedence is evaluated in one ordered
// run under s.mu rather than split between the lock-free probe and the locked
// body. Two reasons. The terminal exit carries a tear-down obligation —
// dropping this receiver, and its key with it — that has to happen under the
// same lock that decided it was terminal. And the cancellation check must sit
// above the read, or the only cancellation arm would be the <-ctxDone below,
// reachable only once parked, so a receiver looping on RecvContext against a
// publisher fast enough to keep a value always unread would take the value
// every iteration and never observe its own shutdown.
func (rx *Receiver[K, V]) recvLoop(ctx context.Context) (gobus.Event[K, V], error) {
	var zero gobus.Event[K, V]
	s := rx.s
	ctxDone := ctx.Done()
	parked := false
	defer func() {
		if parked {
			s.mu.Lock()
			rx.waiters--
			s.mu.Unlock()
		}
	}()
	for {
		if rx.done.IsClosed() {
			return zero, gobus.ErrClosed
		}
		if rx.forTestingBeforeRecvLock != nil {
			rx.forTestingBeforeRecvLock()
		}
		s.mu.Lock()
		if parked {
			rx.waiters--
			parked = false
		}
		// Re-check closed under the lock: Close serializes through s.mu, so a
		// Close that won the race against the pre-lock check is visible here
		// and cannot be handed a value.
		if rx.done.IsClosed() {
			s.mu.Unlock()
			return zero, gobus.ErrClosed
		}
		// closed: nothing can arrive again.
		if rx.drainedLocked() {
			s.deregisterLocked(rx)
			s.mu.Unlock()
			return zero, gobus.ErrClosed
		}
		// cancelled: above the read, so an unread value cannot starve it.
		select {
		case <-ctxDone:
			s.mu.Unlock()
			return zero, ctx.Err()
		default:
		}
		// value: the current value, if this receiver has not taken it.
		if rx.unreadLocked() {
			ev := rx.takeLocked()
			s.mu.Unlock()
			return ev, nil
		}
		rx.waiters++
		parked = true
		notify := rx.notify
		s.mu.Unlock()
		// Every arm falls through to the top rather than deciding here. A wake
		// carries no verdict — only "state changed, look again" — so the
		// ordered run above stays the single place the precedence is
		// evaluated. Returning ErrClosed or ctx.Err() from these arms would
		// hand the decision to the select: with a close and a cancellation both
		// landing on a parked reader, both arms are ready and Go picks
		// uniformly, which would also skip the terminal tear-down.
		select {
		case <-notify:
		case <-rx.done.Done():
		case <-ctxDone:
		}
	}
}

// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled
// first. Cancellation does not close this receiver.
//
// It implements the closed > cancelled > value precedence documented on
// [gobus.Receiver] — including that a cancelled read never consumes the value
// it declined, and that reaching ctx.Err() neither closes nor deregisters the
// receiver. What is watch-specific is the cost of ignoring the latter: an
// abandoned handle holds its key against the hub for the hub's lifetime.
// `defer rx.Close()` covers it, as it does for any abandoned receiver.
func (rx *Receiver[K, V]) RecvContext(ctx context.Context) (gobus.Event[K, V], error) {
	return rx.recvLoop(ctx)
}
