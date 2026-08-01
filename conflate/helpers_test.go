package conflate

// lenForTest reports the number of pending keys on this receiver. Test-only.
func (rx *Receiver[K, V]) lenForTest() int {
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	return rx.order.Len()
}

// forTestingReceiverCount returns the number of receivers currently registered
// with the hub. Tests use it to verify deregistration on terminal ErrClosed.
func (h *Hub[K, V]) forTestingReceiverCount() int {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return len(h.s.receivers)
}

// forTestingLiveReceivers returns the lock-free receiver count that gates the
// send fast path. It takes s.mu so a test reads it and len(s.receivers) as one
// consistent pair; the fast path itself reads the field without the lock, which
// is the whole point of the field.
func (h *Hub[K, V]) forTestingLiveReceivers() int64 {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return h.s.live.Load()
}

// forTestingLivePoisoned reports whether the send fast path has been retired
// for the life of the hub. The sentinel value belongs to buscore; what this
// package's tests care about is that a closed hub can never read as idle
// again, whichever close path got there.
func (h *Hub[K, V]) forTestingLivePoisoned() bool {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return h.s.live.Load() < 0
}
