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

// forTestingWildcardCount returns the number of Hub.WatchAcross receivers the hub
// routes to. It is the leak a receiver count cannot see: a wildcard receiver
// dropped from s.receivers but left in s.wildcard is still offered every value
// and still pins its slot, while every read on it reports ErrClosed — so the
// hub grows and no assertion on the handle notices.
func (h *Hub[K, V]) forTestingWildcardCount() int {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return len(h.s.wildcard)
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
