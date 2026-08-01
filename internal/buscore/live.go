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
// Every method is safe to call concurrently, including Sync against a
// concurrent Poison — see Sync for why that costs a compare-and-swap. Buses
// call Sync under their own lock anyway, at every site that mutates the
// receiver map, but the poison invariant does not depend on their doing so.
type LiveCount struct {
	n atomic.Int64

	// forTestingBeforeStore, if non-nil, runs inside Sync between reading the
	// current value and writing the new one, so a test can land a Poison in
	// that window without relying on timing. nil in production.
	forTestingBeforeStore func()
}

// Sync refreshes the count from the map that owns the truth. It is a no-op
// once the count is poisoned, which is load-bearing rather than defensive: a
// receiver closing after the sender has closed syncs through here, and a plain
// store would put a zero over the poison and hand a closed bus back to the
// fast path.
//
// The compare-and-swap is what makes that guard hold under concurrency. A
// load-then-store leaves a window for a Poison to land between the two halves
// and be overwritten, and that is the one error this type cannot tolerate: an
// idle count on a closed bus makes every later send return nil instead of
// ErrClosed, dropping values on a bus with no replay. The loop retries only
// when the value moved under it, so the uncontended path — which is all a bus
// holding its own lock ever takes — is one CAS.
func (c *LiveCount) Sync(n int) {
	for {
		cur := c.n.Load()
		if cur == poisoned {
			return
		}
		if c.forTestingBeforeStore != nil {
			c.forTestingBeforeStore()
		}
		if c.n.CompareAndSwap(cur, int64(n)) {
			return
		}
	}
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
