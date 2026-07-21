// Package conflate provides a single-producer, multi-consumer keyed
// latest-value fan-out bus.
//
// A [Hub] hands out a singleton [Sender] and any number of [Receiver]s.
// Where [github.com/amorey/gochan/watch] keeps one latest-value slot and
// [github.com/amorey/gochan/broadcast] keeps a fixed ring of every value
// (returning ErrLagged on overflow), conflate keeps the latest value *per
// key*: each receiver holds one slot per key plus an insertion-ordered queue,
// and a [Sender.Send] for a key that is already pending coalesces into that
// slot rather than appending. A slow receiver therefore never lags-as-loss —
// it catches up to the latest value of every key, in first-touch order, and
// its memory stays bounded by the live key set rather than by write volume.
//
// Coalescing policy is supplied by the caller as a [Merge] function so the bus
// stays domain-agnostic: Merge decides how an undelivered pending value
// combines with a newly sent one, and may annihilate the slot entirely (e.g. a
// create followed by a delete the consumer never observed).
//
// # Typical uses
//
// Streaming resource state to watchers (Kubernetes-style informers), UI or
// dashboard update feeds where only the current state of each entity matters,
// cache invalidation fan-out, incremental index maintenance, and any
// "coalesce writes per entity, deliver at consumer pace" pipeline.
//
// # Semantics
//
// Latest-value-per-key delivery. Each receiver owns an insertion-ordered
// queue of keys plus one value slot per key. A Send for a key with no pending
// slot appends the key at the back of the queue; a Send for a key that is
// already pending coalesces into the existing slot via [Merge] and leaves the
// key's queue position unchanged. Delivery order is therefore first-touch
// order, and a hot key does not starve a cold one by repeatedly jumping the
// queue.
//
// Bounded by the key set, not the write volume. A thousand sends across four
// keys leave at most four pending entries. This is what makes conflate safe
// for an unbounded producer feeding a slow consumer: there is no capacity
// argument and no lag error because there is no unbounded backlog to grow.
//
// Merge may annihilate. Returning keep == false drops the key entirely —
// queue entry and value slot both — so a create/delete pair the consumer
// never observed leaves no residue at all.
//
// Send never blocks. Slow receivers cannot apply backpressure to the
// publisher; they simply coalesce more aggressively.
//
// Per-receiver policy overrides, passed as options to [Hub.Receiver].
// [Hub.WithKeyFilter] filters keys at enqueue time, so a receiver interested
// in one key out of a high-cardinality producer stays bounded by the keys it
// actually wants. [Hub.WithMerge] lets a single consumer coalesce by its own
// policy without affecting the rest of the bus — necessary when consumers of
// the same producer disagree about what may be dropped, since one hub-wide
// Merge cannot express that. The options compose.
//
// Sender close drains. [Sender.Close] lets each receiver drain its pending
// values once before subsequent reads report [gobus.ErrClosed]. [Hub.Close]
// is hard tear-down with no drain.
//
// A single [Receiver] is intended for one consumer goroutine.
package conflate

import (
	"container/list"
	"context"
	"sync"

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/internal/buscore"
)

// Merge combines an undelivered pending value with a newly sent value for the
// same key. It is invoked only when the key already has a pending (not yet
// delivered) slot. It returns the surviving value and whether to keep the slot;
// keep == false drops the key entirely (annihilation).
//
// Merge is called under the bus lock, so it must not call back into the hub.
type Merge[V any] func(prev, next V) (merged V, keep bool)

// ReceiverOption configures a receiver minted by [Hub.Receiver]. Options are
// built by the hub's own [Hub.WithKeyFilter] and [Hub.WithMerge] methods,
// which fix K and V from the hub — so option call sites need no type arguments
// and a mismatched option fails to compile rather than at run time.
//
// The set of options is closed: the parameter type is unexported, so code
// outside this package cannot name it to write one of its own.
type ReceiverOption[K comparable, V any] func(*receiverConfig[K, V])

// receiverConfig accumulates the options applied to one receiver. A zero
// receiverConfig is the default receiver: every key, the hub's shared merge.
type receiverConfig[K comparable, V any] struct {
	keep  func(K) bool
	merge Merge[V]
}

// shared is the hub state common to the sender and every receiver. A single
// mutex guards all receivers' queues: Send fans a write across them and each
// read pops from its own, so one lock keeps enqueue/coalesce/pop consistent
// without per-receiver locking races.
type shared[K comparable, V any] struct {
	mu        sync.Mutex
	merge     Merge[V]
	receivers map[*Receiver[K, V]]struct{}
	txClosed  bool
	hubClosed bool
}

// Hub is the construction handle for a conflate pipeline.
type Hub[K comparable, V any] struct {
	s  *shared[K, V]
	tx *Sender[K, V]
}

// Sender is the singleton send-side handle. Safe to share across goroutines.
type Sender[K comparable, V any] struct{ s *shared[K, V] }

// Receiver is a receive-side handle, intended for one consumer goroutine. It
// holds an insertion-ordered key queue plus a per-key value slot; coalescing
// happens on Send into these structures, so a read is a plain pop under the
// lock.
type Receiver[K comparable, V any] struct {
	s       *shared[K, V]
	keep    func(K) bool        // nil = accept all keys; else enqueue only matching keys
	merge   Merge[V]            // nil = use the hub's shared merge; else this receiver's own
	order   *list.List          // K in first-touch order; bounded by live keys
	elems   map[K]*list.Element // key -> its order element, for O(1) coalesce/remove
	pending map[K]V             // key -> latest undelivered value
	notify  chan struct{}       // closed+replaced to wake this receiver's parked readers
	waiters int                 // parked readers on this receiver; gates notify allocation
	done    buscore.CloseOnce

	chOnce sync.Once
	ch     chan gobus.Event[K, V]

	// forTestingBeforeRecvLock and forTestingBeforeTryRecvLock, if non-nil, run
	// after the lock-free closed check and before taking s.mu, so tests can
	// exercise the close-wins-the-race re-check under the lock. nil in
	// production.
	forTestingBeforeRecvLock    func()
	forTestingBeforeTryRecvLock func()

	// forTestingFeederBeforeLock, forTestingFeederParked and
	// forTestingFeederExit, if non-nil, are invoked by the Chan feeder:
	// respectively after its lock-free closed check and before taking s.mu,
	// after it pops an event and before it enters the delivery select, and on
	// the way out just before it closes the channel. All nil in production.
	forTestingFeederBeforeLock func()
	forTestingFeederParked     func()
	forTestingFeederExit       func()
}

// New creates a hub whose receivers coalesce per key using merge. It panics if
// merge is nil — the coalescing policy is the whole point of the bus, so there
// is no implicit default.
func New[K comparable, V any](merge Merge[V]) *Hub[K, V] {
	if merge == nil {
		panic("gobus: conflate.New requires a non-nil Merge")
	}
	s := &shared[K, V]{merge: merge, receivers: make(map[*Receiver[K, V]]struct{})}
	return &Hub[K, V]{s: s, tx: &Sender[K, V]{s: s}}
}

// Sender returns the singleton send-side handle. Repeated calls return the
// same handle. After the hub has been closed the returned handle reports
// [gobus.ErrClosed] on use.
func (h *Hub[K, V]) Sender() *Sender[K, V] { return h.tx }

// Receiver returns a new receiver bound to the hub, configured by opts. A
// receiver starts empty: it observes values sent after it was created, not the
// producer's history. If the hub is already closed the receiver is pre-closed
// and reports [gobus.ErrClosed] on use.
//
// Options are minted by the hub itself — [Hub.WithKeyFilter] and
// [Hub.WithMerge] — and compose freely:
//
//	rx := hub.Receiver()                           // every key, hub's merge
//	rx := hub.Receiver(hub.WithKeyFilter(wanted))  // one key subset
//	rx := hub.Receiver(hub.WithMerge(stricter))    // own coalescing policy
//	rx := hub.Receiver(hub.WithKeyFilter(wanted), hub.WithMerge(stricter))
//
// Later options win over earlier ones for the same setting.
func (h *Hub[K, V]) Receiver(opts ...ReceiverOption[K, V]) *Receiver[K, V] {
	return h.receiver(opts)
}

// WithKeyFilter returns an option restricting a receiver to keys for which
// keep returns true; all other keys are dropped at Send time and never
// buffered. Filtering at enqueue keeps a selective receiver's memory bounded
// by the keys it actually wants rather than the producer's whole key space —
// important for a receiver interested in a single key out of a
// high-cardinality producer. keep is called under the bus lock, so it must not
// call back into the hub. Panics if keep is nil.
//
// It is a method on Hub rather than a package-level function so that K and V
// are fixed by the hub: callers write no type arguments, and an option built
// for the wrong key type is a compile error.
func (h *Hub[K, V]) WithKeyFilter(keep func(K) bool) ReceiverOption[K, V] {
	if keep == nil {
		panic("gobus: conflate.Hub.WithKeyFilter requires a non-nil keep func")
	}
	return func(c *receiverConfig[K, V]) { c.keep = keep }
}

// WithMerge returns an option making a receiver coalesce with its own merge
// instead of the hub's shared one. This lets a single consumer apply a
// different policy — e.g. annihilating pending values others must retain —
// without affecting the rest of the bus. Use it when consumers of the same
// producer have genuinely different requirements about what may be dropped,
// which a single hub-wide Merge cannot express. merge is called under the bus
// lock, like the shared one. Panics if merge is nil.
func (h *Hub[K, V]) WithMerge(merge Merge[V]) ReceiverOption[K, V] {
	if merge == nil {
		panic("gobus: conflate.Hub.WithMerge requires a non-nil Merge")
	}
	return func(c *receiverConfig[K, V]) { c.merge = merge }
}

func (h *Hub[K, V]) receiver(opts []ReceiverOption[K, V]) *Receiver[K, V] {
	var cfg receiverConfig[K, V]
	for _, opt := range opts {
		if opt == nil {
			panic("gobus: conflate.Hub.Receiver received a nil ReceiverOption")
		}
		opt(&cfg)
	}
	rx := &Receiver[K, V]{
		s:       h.s,
		keep:    cfg.keep,
		merge:   cfg.merge,
		order:   list.New(),
		elems:   make(map[K]*list.Element),
		pending: make(map[K]V),
		notify:  make(chan struct{}),
	}
	rx.done.Init()
	h.s.mu.Lock()
	if h.s.hubClosed {
		rx.done.Close()
	} else {
		h.s.receivers[rx] = struct{}{}
	}
	h.s.mu.Unlock()
	return rx
}

// Close is hard tear-down: the sender and every live receiver are closed
// immediately, with no final drain of pending values. Use [Sender.Close] for
// the soft path. Future [Hub.Receiver] calls return pre-closed handles.
// Idempotent.
func (h *Hub[K, V]) Close() {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hubClosed {
		return
	}
	s.hubClosed = true
	s.txClosed = true
	for rx := range s.receivers {
		rx.done.Close()
	}
	s.receivers = nil
}

// Send publishes v under key k to every receiver. Never blocks. For a receiver
// that already has k pending, the caller's [Merge] coalesces into that slot;
// otherwise k is appended at the back of the receiver's queue.
func (tx *Sender[K, V]) Send(k K, v V) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed {
		return gobus.ErrClosed
	}
	for rx := range s.receivers {
		rx.enqueueLocked(k, v)
	}
	return nil
}

// TrySend is equivalent to Send for conflate: Send never blocks, so there is
// no separate non-blocking path. Provided to satisfy the common
// [gobus.Sender] interface.
func (tx *Sender[K, V]) TrySend(k K, v V) error { return tx.Send(k, v) }

// SendContext returns ctx.Err() if ctx is already cancelled; otherwise behaves
// like Send. Send never blocks, so the context is only checked at entry.
func (tx *Sender[K, V]) SendContext(ctx context.Context, k K, v V) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.Send(k, v)
}

// Close closes the sender. Receivers drain their pending values once before
// subsequent reads return [gobus.ErrClosed], and their Chan feeders close the
// channel after the same drain. Further Send calls return ErrClosed.
// Idempotent.
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

// enqueueLocked merges or appends v for key k. Caller holds s.mu.
func (rx *Receiver[K, V]) enqueueLocked(k K, v V) {
	if rx.keep != nil && !rx.keep(k) {
		return // not a key this receiver wants; never buffer it
	}
	if e, ok := rx.elems[k]; ok {
		merge := rx.merge
		if merge == nil {
			merge = rx.s.merge
		}
		merged, keep := merge(rx.pending[k], v)
		if keep {
			rx.pending[k] = merged // coalesce in place; queue position unchanged
		} else {
			rx.order.Remove(e) // annihilate: no residue in queue or slot
			delete(rx.elems, k)
			delete(rx.pending, k)
		}
	} else {
		rx.elems[k] = rx.order.PushBack(k)
		rx.pending[k] = v
	}
	rx.signalLocked()
}

// popLocked removes and returns the oldest pending event. Caller holds s.mu.
func (rx *Receiver[K, V]) popLocked() (gobus.Event[K, V], bool) {
	e := rx.order.Front()
	if e == nil {
		return gobus.Event[K, V]{}, false
	}
	k := e.Value.(K)
	rx.order.Remove(e)
	delete(rx.elems, k)
	v := rx.pending[k]
	delete(rx.pending, k)
	return gobus.Event[K, V]{Key: k, Value: v}, true
}

// signalLocked wakes this receiver's parked readers, if any. Caller holds s.mu.
func (rx *Receiver[K, V]) signalLocked() {
	if rx.waiters == 0 {
		return
	}
	close(rx.notify)
	rx.notify = make(chan struct{})
}

// Recv blocks until an event is pending, then pops and returns the oldest
// one: the key that was touched least recently, carrying its latest merged
// value. It returns [gobus.ErrClosed] once the receiver or hub is closed
// (with the sender's soft close, after the pending values have drained).
func (rx *Receiver[K, V]) Recv() (gobus.Event[K, V], error) {
	return rx.recvLoop(context.Background())
}

// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled
// first.
func (rx *Receiver[K, V]) RecvContext(ctx context.Context) (gobus.Event[K, V], error) {
	return rx.recvLoop(ctx)
}

// recvLoop is the shared blocking-recv implementation. Recv passes
// context.Background() to opt out of cancellation — Background's Done()
// returns nil, and a nil channel in a select arm is never selected, so the
// cancellation arm is a no-op on that path.
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
		// and cannot be handed a pending value.
		if rx.done.IsClosed() {
			s.mu.Unlock()
			return zero, gobus.ErrClosed
		}
		if ev, ok := rx.popLocked(); ok {
			s.mu.Unlock()
			return ev, nil
		}
		if s.txClosed {
			// Terminal: deregister so a long-lived hub doesn't pin a drained
			// receiver after a Sender.Close soft close.
			delete(s.receivers, rx)
			s.mu.Unlock()
			return zero, gobus.ErrClosed
		}
		rx.waiters++
		parked = true
		notify := rx.notify
		s.mu.Unlock()
		select {
		case <-notify:
		case <-rx.done.Done():
			return zero, gobus.ErrClosed
		case <-ctxDone:
			return zero, ctx.Err()
		}
	}
}

// TryRecv pops the oldest pending event without blocking. It returns
// [gobus.ErrEmpty] if nothing is pending, or [gobus.ErrClosed] if the receiver
// or hub is closed (or the sender is closed and the pending values have
// drained).
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
	if ev, ok := rx.popLocked(); ok {
		return ev, nil
	}
	if rx.s.txClosed {
		delete(rx.s.receivers, rx)
		return zero, gobus.ErrClosed
	}
	return zero, gobus.ErrEmpty
}

// Chan returns a per-receiver native channel that yields pending events in
// first-touch key order, carrying the same [gobus.Event] values the Recv
// methods return. The channel is unbuffered: coalescing continues to
// happen in the receiver's slots while the consumer is busy, so a fast
// publisher produces no backlog beyond the live key set. Repeated calls on the
// same receiver return the same channel.
//
// Note that an event already handed to the feeder has left the receiver's
// slots, so a Send for that key while the feeder is parked on delivery
// enqueues the key afresh rather than coalescing into the in-flight event.
//
// The channel is closed when the feeder observes receiver-close, or
// sender/hub-close with nothing left to drain. Abandoning the channel without
// calling [Receiver.Close] pins the feeder goroutine — it will park forever
// waiting for the next event. Always Close the receiver when you stop reading.
func (rx *Receiver[K, V]) Chan() <-chan gobus.Event[K, V] {
	rx.chOnce.Do(func() {
		rx.ch = make(chan gobus.Event[K, V])
		go rx.feed()
	})
	return rx.ch
}

// feed is the Chan goroutine. It pops under the lock and delivers outside it,
// watching rx.done so a Close while parked on the send tears the feeder down
// instead of leaking it.
func (rx *Receiver[K, V]) feed() {
	defer close(rx.ch)
	if rx.forTestingFeederExit != nil {
		defer rx.forTestingFeederExit()
	}
	s := rx.s
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
		// Re-check under the lock, as recvLoop does: Close serializes through
		// s.mu, so a Close that won the race against the pre-lock check cannot
		// see the feeder deliver one more event.
		if rx.done.IsClosed() {
			s.mu.Unlock()
			return
		}
		if ev, ok := rx.popLocked(); ok {
			s.mu.Unlock()
			if rx.forTestingFeederParked != nil {
				rx.forTestingFeederParked()
			}
			select {
			case rx.ch <- ev:
			case <-rx.done.Done():
				return
			}
			continue
		}
		if s.txClosed {
			delete(s.receivers, rx)
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

// Close closes this receiver only; other receivers and the sender are
// unaffected. Any pending values are abandoned and the Chan feeder, if
// started, shuts down and closes the channel. Idempotent.
func (rx *Receiver[K, V]) Close() {
	// Close under mu so a concurrent read that has acquired mu first cannot
	// hand back a pending value to a now-closed receiver: the readers re-check
	// rx.done after taking mu, and that check is stable as long as Close
	// serializes through mu too.
	rx.s.mu.Lock()
	rx.done.Close()
	delete(rx.s.receivers, rx)
	rx.s.mu.Unlock()
}
