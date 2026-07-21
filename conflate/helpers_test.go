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
