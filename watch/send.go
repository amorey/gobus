package watch

import "github.com/amorey/gobus"

// Send publishes v as the value of k to every receiver watching k. Never
// blocks. A Send for a key nobody watches is discarded: there is no receiver
// and therefore no buffer, so a later [Hub.Watch] never sees it.
//
// For each watching receiver the hub's [Accept] decides whether v replaces
// that receiver's current value. A rejected value changes nothing, and the
// receiver is not told. Because Accept is evaluated per receiver against that
// receiver's own slot, one value can be new for a receiver that subscribed
// early and stale for one that subscribed late.
//
// Returns [gobus.ErrClosed] if the sender or hub has been closed.
func (tx *Sender[K, V]) Send(k K, v V) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.sendLocked(nil, k, v)
	return err
}

// TrySend is equivalent to Send: Send never blocks, so there is no separate
// non-blocking path. Provided to satisfy [gobus.Sender].
func (tx *Sender[K, V]) TrySend(k K, v V) error { return tx.Send(k, v) }

// sendLocked is the shared send core: the one place the send-side precedence
// is evaluated and a value is fanned out. Caller holds s.mu, so closedness and
// cancellation are read from a single consistent view rather than a pre-check
// that could go stale before the fan-out.
//
// Send opts out of cancellation with a nil ctxDone — a nil channel is never
// ready in a select, the same trick recvLoop takes from context.Background's
// nil Done() — which keeps the ordering here rather than in one of its callers.
//
// It reports cancellation rather than resolving it: the caller turns a true
// cancelled into ctx.Err() after releasing s.mu. A caller-supplied context is
// arbitrary code whose Err may take application locks, and calling it under
// s.mu would invert the lock order against any goroutine that takes those
// locks before entering the bus.
func (s *shared[K, V]) sendLocked(ctxDone <-chan struct{}, k K, v V) (cancelled bool, err error) {
	// closed > cancelled: ErrClosed is the durable answer, so it outranks a
	// ctx a retry could replace.
	if s.txClosed {
		return false, gobus.ErrClosed
	}
	select {
	case <-ctxDone:
		return true, nil
	default:
	}
	for rx := range s.index[k] {
		rx.offerLocked(v)
	}
	return false, nil
}

// offerLocked puts v to this receiver's Accept and takes it on a true result.
// A panic out of Accept leaves the receivers already visited holding v and the
// rest untouched; the caller's deferred unlock keeps the hub usable. Caller
// holds s.mu.
func (rx *Receiver[K, V]) offerLocked(v V) {
	if rx.s.accept != nil && !rx.s.accept(rx.val, v) {
		return
	}
	rx.val = v
	rx.version++
	rx.signalLocked()
}

// signalLocked wakes this receiver's parked readers, if any. Caller holds s.mu.
func (rx *Receiver[K, V]) signalLocked() {
	if rx.waiters == 0 {
		return
	}
	close(rx.notify)
	rx.notify = make(chan struct{})
}

// Close closes the sender. A receiver holding an unread value reads it once
// more before subsequent reads return [gobus.ErrClosed]; a receiver already
// caught up sees ErrClosed at once. Further sends return ErrClosed.
// Idempotent.
//
// Do not call it concurrently with an active Send from another goroutine.
func (tx *Sender[K, V]) Close() {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed {
		return
	}
	s.txClosed = true
	for rx := range s.receivers {
		rx.signalLocked()
	}
}
