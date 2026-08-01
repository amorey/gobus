package watch

import "github.com/amorey/gobus"

// Chan returns a per-receiver channel yielding values as they become current.
// It is unbuffered, so a fast publisher builds no backlog: while the consumer
// is not reading, further sends only update the slot, and the value waiting to
// be delivered is replaced by the current one. Repeated calls return the same
// channel.
//
// The channel closes when the feeder observes receiver-close, or
// sender/hub-close with nothing left to drain.
//
// Reading it is not a guarantee that every value read is current at the moment
// it is read. Once the feeder has committed to a delivery, a newer value
// arriving makes both arms of its select ready, and Go chooses between ready
// arms at random — so a superseded value is sometimes delivered, with the
// newer one immediately behind it. What holds is that values arrive in order
// and that a consumer which keeps reading converges on the current value.
//
// Abandoning the channel without calling [Receiver.Close] pins the feeder
// goroutine — it parks forever waiting for the next value. Always Close the
// receiver when you stop reading.
func (rx *Receiver[K, V]) Chan() <-chan gobus.Event[K, V] {
	rx.chOnce.Do(func() {
		rx.ch = make(chan gobus.Event[K, V])
		go rx.feed()
	})
	return rx.ch
}

// feed is the Chan goroutine. It snapshots under the lock and delivers outside
// it, and it marks the value read only once the consumer has taken it — so a
// newer value arriving mid-delivery makes the feeder re-snapshot rather than
// let the consumer observe a stale one. That is what keeps a Chan consumer on
// the same latest-value footing as a Recv caller.
func (rx *Receiver[K, V]) feed() {
	defer close(rx.ch)
	if rx.forTestingFeederExit != nil {
		defer rx.forTestingFeederExit()
	}
	s := rx.s
	parked := false
	// delivered carries a version the consumer has taken across to the next
	// iteration, where it is written to lastSeen under the lock. Every field
	// mutation stays inside s.mu, since the feeder is not this receiver's only
	// reader.
	var delivered uint64
	defer func() {
		if parked {
			s.mu.Lock()
			rx.waiters--
			s.mu.Unlock()
		}
	}()
	for {
		if rx.done.IsClosed() {
			return
		}
		if rx.forTestingFeederBeforeLock != nil {
			rx.forTestingFeederBeforeLock()
		}
		s.mu.Lock()
		if parked {
			rx.waiters--
			parked = false
		}
		if delivered > rx.lastSeen {
			rx.lastSeen = delivered
		}
		delivered = 0
		// Re-check under the lock, as recvLoop does: Close serializes through
		// s.mu, so a Close that won the race against the pre-lock check cannot
		// see the feeder deliver one more value.
		if rx.done.IsClosed() {
			s.mu.Unlock()
			return
		}
		if rx.unreadLocked() {
			ev := gobus.Event[K, V]{Key: rx.key, Value: rx.val}
			ver := rx.version
			notify := rx.notify
			rx.waiters++
			parked = true
			s.mu.Unlock()
			if rx.forTestingFeederParked != nil {
				rx.forTestingFeederParked()
			}
			select {
			case rx.ch <- ev:
				delivered = ver
			case <-notify:
				// A newer value landed, or the sender closed. Loop to
				// re-snapshot so the consumer's next read is the current
				// value, not this one.
			case <-rx.done.Done():
				return
			}
			continue
		}
		if rx.drainedLocked() {
			s.deregisterLocked(rx)
			s.mu.Unlock()
			return
		}
		rx.waiters++
		parked = true
		notify := rx.notify
		s.mu.Unlock()
		select {
		case <-notify:
		case <-rx.done.Done():
			return
		}
	}
}
