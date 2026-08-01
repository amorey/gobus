package buscore

import "sync/atomic"

// poisoned is the LiveCount value meaning "the send side is closed". Negative
// so it can never be mistaken for a receiver count.
const poisoned = -1

// LiveCount is the lock-free receiver count that gates a bus's send fast path.
// A publisher reading zero may skip the bus lock: there is nobody to fan out
// to, and the locked path has no other effect.
//
// Only one direction of error is safe. Over-reporting is free — the publisher
// takes the lock and finds nothing. Under-reporting loses a value permanently,
// because these buses have no replay: the next send for that key overwrites a
// slot the subscriber was never told had been skipped. That asymmetry is why
// [LiveCount.Sync] takes the length of the map that owns the truth rather than
// offering an increment — a derived value cannot drift below the truth.
//
// Closedness is folded into this one field rather than mirrored into a second
// atomic. [LiveCount.Poison] is never cleared, which is what lets a bus keep
// its closed flag a plain bool with a single access discipline, read only
// under the bus lock, on the path the poison guarantees a closed bus takes.
//
// Every method is safe to call concurrently, but Sync is meant to be called
// under the bus lock, at every site that mutates the receiver map.
type LiveCount struct {
	n atomic.Int64
}

// Sync refreshes the count from the map that owns the truth. It is a no-op
// once the count is poisoned, which is load-bearing rather than defensive: a
// receiver closing after the sender has closed syncs through here, and without
// the guard it would write a zero over the poison and hand a closed bus back
// to the fast path.
func (c *LiveCount) Sync(n int) {
	if c.n.Load() == poisoned {
		return
	}
	c.n.Store(int64(n))
}

// Poison retires the fast path for the life of the bus, so every later send
// reaches the locked path and is answered from the bus's own closed flag.
func (c *LiveCount) Poison() { c.n.Store(poisoned) }

// Idle reports whether there is no receiver *and* the send side is open —
// the one state in which a publisher may return early. It asks whether there
// is any work, never whether the bus is alive.
func (c *LiveCount) Idle() bool { return c.n.Load() == 0 }

// Load returns the raw count. Intended for tests that assert the fast path's
// state; use [LiveCount.Idle] on the send path.
func (c *LiveCount) Load() int64 { return c.n.Load() }
