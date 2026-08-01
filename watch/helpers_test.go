package watch

// forTestingReceiverCount returns the number of receivers registered with the
// hub. Tests use it to verify deregistration on close and on terminal
// ErrClosed.
func (h *Hub[K, V]) forTestingReceiverCount() int {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return len(h.s.receivers)
}

// forTestingKeyCount returns the number of keys the hub holds state for. This
// is the R5 guarantee: it must fall to zero when the last watcher of a key
// leaves, by either exit path.
func (h *Hub[K, V]) forTestingKeyCount() int {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return len(h.s.index)
}

// forTestingLiveReceivers returns the lock-free count that gates the send fast
// path. It takes s.mu so a test reads it and len(s.receivers) as one
// consistent pair; the fast path itself reads the count without the lock,
// which is the whole point of it.
func (h *Hub[K, V]) forTestingLiveReceivers() int64 {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return h.s.live.Load()
}
