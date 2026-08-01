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
// A send with no receivers is cheap. [Sender.Send] returns without taking the
// bus lock when no receiver is registered, so a hot producer does not pay for a
// hub nobody is watching. The result is the same one it always gave: nil, with
// nothing published. The corollary is a subscriber-side ordering requirement —
// register with [Hub.Receiver] before taking a snapshot of the producer's
// state, never after — because a value published in that gap reaches no
// receiver and conflate has no replay.
//
// Per-receiver policy overrides, passed as options to [Hub.Receiver].
// [Hub.WithKeyFilter] filters keys at enqueue time, so a receiver interested
// in one key out of a high-cardinality producer stays bounded by the keys it
// actually wants. [Hub.WithMerge] lets a single consumer coalesce by its own
// policy without affecting the rest of the bus — necessary when consumers of
// the same producer disagree about what may be dropped, since one hub-wide
// Merge cannot express that. The options compose.
//
// The backlog head is observable without consuming it. [Receiver.Peek]
// returns the oldest pending event and leaves it in place, under the same
// closed > value precedence the popping paths use.
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
	"sync/atomic"

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/internal/buscore"
)

// sendPoisoned is the value liveReceivers holds once the send side has closed.
// It is not a count: it only has to be non-zero, so that a closed hub always
// takes the locked path, where sendLocked reads txClosed and reports ErrClosed.
//
// Without it, Hub.Close emptying s.receivers — or the last receiver closing
// after Sender.Close — would leave a zero count on a closed hub, and the send
// fast path would answer nil where ErrClosed is the durable answer.
//
// The type is explicit so the comparison and the store need no conversion.
const sendPoisoned int64 = -1

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

	// liveReceivers is a lock-free copy of len(receivers). It is written only
	// under mu, at every site that mutates that map, and read without mu by the
	// send side. A publisher may skip the lock entirely when it reads zero: there
	// is nobody to fan out to, and sendLocked has no other effect.
	//
	// Only one direction of error is safe. Over-reporting is free — the publisher
	// takes the lock and finds nothing, which is what it did before this field
	// existed. Under-reporting loses a value permanently, because a conflated bus
	// has no retry: the next send for that key coalesces into a slot the
	// subscriber was never told had been skipped. That asymmetry is why the count
	// is *derived* from len(receivers) by syncLiveLocked rather than incremented
	// and decremented — a derived value cannot drift below the truth.
	//
	// Closedness is deliberately folded into this one field rather than mirrored
	// into a second atomic: Sender.Close and Hub.Close store sendPoisoned, and no
	// later write clears it. That is what lets txClosed stay a plain bool with a
	// single access discipline — it is read only under mu, on the path the poison
	// guarantees a closed hub takes.
	liveReceivers atomic.Int64

	// forTestingBeforeSendLock, if non-nil, runs on every send path that is about
	// to take mu — Send, TrySend and SendContext — after SendContext has taken
	// ctx's Done channel and before the lock is acquired. A test can therefore
	// land a cancellation in the window where the send is waiting for the lock,
	// and can count lock acquisitions from the send side. The send-side twin of
	// forTestingBeforeRecvLock. nil in production.
	//
	// Unlike the receiver seams, this one is hub-wide and is read outside mu —
	// it has to be, since the window it opens is the wait for mu itself. It is
	// therefore safe to arm only while no SendContext is in flight: the
	// receiver hooks get their synchronization from a Receiver's
	// single-consumer ownership, but a Sender is shared, so arming this one
	// alongside a concurrent send is an unsynchronized write against the read
	// in SendContext. Sequence the test through the hook (it runs on the
	// sending goroutine) rather than by writing the field from a second one.
	// The same scope means every concurrent SendContext on the hub runs it, so
	// a multi-sender test must discriminate inside the func, not by arming.
	forTestingBeforeSendLock func()
}

// syncLiveLocked refreshes the lock-free receiver count from the map that owns
// the truth. Call it at every site that mutates s.receivers. Caller holds s.mu.
//
// The early return is load-bearing, not defensive: a Receiver.Close after a
// Sender.Close deregisters through here, and without the guard it would write a
// zero over the poison and hand a closed hub back to the fast path.
func (s *shared[K, V]) syncLiveLocked() {
	if s.liveReceivers.Load() == sendPoisoned {
		return
	}
	s.liveReceivers.Store(int64(len(s.receivers)))
}

// poisonLiveLocked retires the send fast path for the life of the hub, so every
// later send reaches sendLocked and is answered from txClosed. Caller holds
// s.mu.
func (s *shared[K, V]) poisonLiveLocked() { s.liveReceivers.Store(sendPoisoned) }

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

	// forTestingBeforeRecvLock, forTestingBeforeTryRecvLock and
	// forTestingBeforePeekLock, if non-nil, run after the lock-free closed
	// check and before taking s.mu, so tests can exercise the
	// close-wins-the-race re-check under the lock. nil in production.
	forTestingBeforeRecvLock    func()
	forTestingBeforeTryRecvLock func()
	forTestingBeforePeekLock    func()

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
		h.s.syncLiveLocked()
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
	s.poisonLiveLocked()
	for rx := range s.receivers {
		rx.done.Close()
	}
	s.receivers = nil
}

// Send publishes v under key k to every receiver. Never blocks. For a receiver
// that already has k pending, the caller's [Merge] coalesces into that slot;
// otherwise k is appended at the back of the receiver's queue.
//
// A Send to a hub with no live receiver does nothing and returns nil. That has
// always been the answer — there is nobody to fan out to — but it is now
// reached without taking the bus lock at all, so a hot producer pays nothing
// for a bus nobody is reading. Only the cost changes, never the result.
//
// This makes one existing requirement load-bearing in a place a reader would
// not think to look. A subscriber must call [Hub.Receiver] *before* it takes
// its own snapshot of the producer's state, never after: a value published in
// the gap reaches no receiver, and conflate has no replay to recover it. The
// requirement is not new — a receiver has always observed only what was sent
// after it was created — but a publisher that skips the lock cannot notice a
// subscriber that arrives late, so getting the order wrong loses the value
// permanently rather than merely racily.
func (tx *Sender[K, V]) Send(k K, v V) error {
	s := tx.s
	// Zero means no receiver *and* an open send side, since close poisons the
	// count. There is nothing to fan out to and no other answer to give, so the
	// hub-wide lock is pure cost here.
	if s.liveReceivers.Load() == 0 {
		return nil
	}
	if s.forTestingBeforeSendLock != nil {
		s.forTestingBeforeSendLock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.sendLocked(nil, k, v)
	return err
}

// sendLocked is the shared send core: the one place the send-side precedence
// is evaluated and a value is fanned out. Caller holds s.mu, so both tests see
// a single consistent view of txClosed rather than a pre-check that could go
// stale before the fan-out.
//
// Send opts out of cancellation by passing a nil ctxDone — a nil channel is
// never ready in a select, the same trick recvLoop takes from
// context.Background's nil Done(), and what keeps the ordering in the shared
// core instead of in one of its callers.
//
// It reports cancellation rather than resolving it: the caller turns a true
// cancelled into ctx.Err() *after* releasing s.mu. A caller-supplied
// context.Context is arbitrary code, and its Err may take application locks —
// held under s.mu that is a lock-order inversion against any goroutine that
// takes those locks before entering the bus. Reading the already-obtained
// Done channel calls nothing. recvLoop's cancellation arm is the same shape.
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
	for rx := range s.receivers {
		rx.enqueueLocked(k, v)
	}
	return false, nil
}

// TrySend is equivalent to Send for conflate: Send never blocks, so there is
// no separate non-blocking path. Provided to satisfy the common
// [gobus.Sender] interface.
func (tx *Sender[K, V]) TrySend(k K, v V) error { return tx.Send(k, v) }

// SendContext behaves like Send, but reports a cancelled ctx instead of
// publishing. Send never blocks, so ctx is consulted exactly once — there is
// no parked state in which a cancellation could arrive — and gates the call on
// nothing beyond reaching the point where the send is resolved.
//
// That single check is made where the send is *resolved*, not on entry. On a
// hub with no live receiver the send resolves at the lock-free receiver-count
// read, and a cancelled ctx there still reports ctx.Err() rather than the nil
// the fast path would otherwise return. Everywhere else the send resolves under
// s.mu, and the rest of this comment is about that path.
//
// So a ctx that was live when SendContext was called but is cancelled
// by the time this send reaches the front of the lock reports ctx.Err() and
// publishes nothing. This is deliberate: a caller passing a context is asking
// for the publish to be bounded by it, and the wait for s.mu is real work —
// the caller's own Merge and key filters run under that lock. Enqueueing a
// value on behalf of a context that has since expired, on the grounds that it
// was live a moment earlier, is the weaker answer. It also keeps every
// verdict in this package derived from state read at the decision point:
// txClosed and rx.done are re-read under the lock for the same reason, and
// recvLoop places its own cancellation check under s.mu rather than trusting
// an entry-time snapshot.
//
// A cancellation racing this call may therefore land either side of the lock,
// and the two outcomes — published, or ctx.Err() — are both correct
// resolutions of that race; the caller cannot have been relying on which.
//
// Precedence is closed > cancelled: a closed sender reports [gobus.ErrClosed]
// even for an already-cancelled ctx, since that is the durable answer and a
// retry with a fresh context would only return it again. A cancelled ctx on a
// live sender still reports ctx.Err().
//
// Only ctx's Done channel is consulted under the bus lock; ctx.Err() is called
// after it is released, so a context implementation that locks cannot deadlock
// against another goroutine's Send or Close. See sendLocked.
func (tx *Sender[K, V]) SendContext(ctx context.Context, k K, v V) error {
	s := tx.s
	ctxDone := ctx.Done()
	// The no-receiver fast path, with the cancellation check inside it. A zero
	// count means the send side was open at the load, so closed > cancelled is
	// already settled and only the cancellation is left to resolve. The load is
	// where this send resolves, exactly as the lock is on the path below.
	if s.liveReceivers.Load() == 0 {
		select {
		case <-ctxDone:
			return ctx.Err()
		default:
			return nil
		}
	}
	if s.forTestingBeforeSendLock != nil {
		s.forTestingBeforeSendLock()
	}
	// The locked section is a closure so the unlock is deferred, matching Send:
	// sendLocked runs the caller's key filter and Merge, and a panic out of
	// either must still release s.mu or a recovering caller would find the
	// whole hub wedged. Resolving ctx.Err() needs to happen after the unlock,
	// which is what rules out simply deferring it in the method body.
	cancelled, err := func() (bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.sendLocked(ctxDone, k, v)
	}()
	if cancelled {
		return ctx.Err()
	}
	return err
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
	s.poisonLiveLocked()
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

// peekLocked returns the oldest pending event without removing it. Caller
// holds s.mu.
//
// popLocked is deliberately not routed through this: it already holds the list
// element it removes, so delegating would cost it a second elems lookup on the
// pop path under s.mu to recover what it had. Four duplicated lines are the
// cheaper trade.
func (rx *Receiver[K, V]) peekLocked() (gobus.Event[K, V], bool) {
	e := rx.order.Front()
	if e == nil {
		return gobus.Event[K, V]{}, false
	}
	k := e.Value.(K)
	return gobus.Event[K, V]{Key: k, Value: rx.pending[k]}, true
}

// drainedLocked reports the terminal condition shared by every receive path:
// the sender is closed and this receiver's queue is empty, so nothing can ever
// arrive again. It is the single definition of "this stream is over" —
// recvLoop, TryRecv, Peek and feed each have their own body by design, but
// must not each carry their own idea of when to stop. Caller holds s.mu.
//
// order.Len() == 0 is exactly when popLocked fails, so testing it before the
// pop is equivalent to the post-pop form and lets the check be ordered above
// the cancellation one. A closed sender with events still queued is *not*
// terminal: it drains first, which is Sender.Close's soft-drain contract.
func (rx *Receiver[K, V]) drainedLocked() bool {
	return rx.s.txClosed && rx.order.Len() == 0
}

// deregisterLocked drops rx from the hub so a long-lived hub does not pin a
// drained receiver. It rides with every terminal verdict, and with
// [Receiver.Close]. Caller holds s.mu.
func (rx *Receiver[K, V]) deregisterLocked() {
	delete(rx.s.receivers, rx)
	rx.s.syncLiveLocked()
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
// first. Cancellation does not close this receiver.
//
// It implements the closed > cancelled > value precedence documented on
// [gobus.Receiver] — including that a cancelled ctx never consumes the event
// it declined, and that reaching ctx.Err() neither closes nor deregisters the
// receiver. What is conflate-specific is the cost of ignoring the latter: an
// abandoned handle keeps coalescing, so it holds one slot per live key for the
// hub's lifetime. `defer rx.Close()` covers it, as it does for any abandoned
// receiver.
//
// To consume what is left before closing, loop on [Receiver.TryRecv] until it
// reports any error, then Close. The flush alone is not a substitute for the
// Close: against a still-open sender it ends on ErrEmpty, which is not
// terminal and does not deregister — only a drain that reaches ErrClosed does
// that on its own.
func (rx *Receiver[K, V]) RecvContext(ctx context.Context) (gobus.Event[K, V], error) {
	return rx.recvLoop(ctx)
}

// recvLoop is the shared blocking-recv implementation. Recv passes
// context.Background() to opt out of cancellation — Background's Done()
// returns nil, and a nil channel in a select arm is never selected, so the
// cancellation arm is a no-op on that path and the loop-top cancellation
// check falls straight through to its default.
//
// The whole closed > cancelled > value precedence is evaluated in one ordered
// run under s.mu, rather than split between the lock-free probe and the locked
// body. Two reasons. The terminal exit carries a tear-down obligation —
// dropping this receiver from s.receivers — that has to happen under the same
// lock that decided it was terminal. And the cancellation check must sit above
// the pop, or the only cancellation arm would be the <-ctxDone below,
// reachable only once parked — so a receiver looping on RecvContext against a
// publisher fast enough to keep an event always pending would take the value
// return every iteration and never observe its own shutdown signal.
//
// Waking consumes nothing here — the event stays in the receiver's slots until
// it is popped — so a wake that races a cancellation re-derives the same
// ordered answer on the next iteration rather than resolving at random.
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
		// closed: nothing can arrive again.
		if rx.drainedLocked() {
			rx.deregisterLocked()
			s.mu.Unlock()
			return zero, gobus.ErrClosed
		}
		// cancelled: above the pop, so a pending event cannot starve it.
		select {
		case <-ctxDone:
			s.mu.Unlock()
			return zero, ctx.Err()
		default:
		}
		// value: the oldest pending event.
		if ev, ok := rx.popLocked(); ok {
			s.mu.Unlock()
			return ev, nil
		}
		rx.waiters++
		parked = true
		notify := rx.notify
		s.mu.Unlock()
		// Every arm falls through to the top of the loop rather than deciding
		// the answer here. A wake carries no value and no verdict — it only
		// says "state changed, look again" — so the ordered run above is the
		// single place the precedence is evaluated, on the parked path as much
		// as at entry. Returning ErrClosed / ctx.Err() directly from these arms
		// would hand the decision to the select: with a close and a
		// cancellation both landing on a parked receiver, both arms are ready,
		// Go picks uniformly at random, and the documented closed > cancelled
		// order would hold at entry but be a coin flip here — which would also
		// skip the terminal deregistration on the ctx side. The extra lap is free: the
		// acquisition it takes is the one the deferred waiters-- would have taken
		// anyway.
		//
		// The feeder's mirror of this select needs no such change: it has no
		// context, so its two arms cannot disagree about the verdict.
		select {
		case <-notify:
		case <-rx.done.Done():
		case <-ctxDone:
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
	if rx.drainedLocked() {
		rx.deregisterLocked()
		return zero, gobus.ErrClosed
	}
	if ev, ok := rx.popLocked(); ok {
		return ev, nil
	}
	return zero, gobus.ErrEmpty
}

// Peek returns the oldest pending event without removing it, so a subsequent
// Recv or TryRecv still returns it. It returns [gobus.ErrEmpty] if nothing is
// pending, or [gobus.ErrClosed] if the receiver or hub is closed (or the sender
// is closed and the pending values have drained) — the same precedence
// [Receiver.TryRecv] applies. Peek is TryRecv without the pop, not a raw read
// of the queue: a closed handle reports ErrClosed even with a value at the
// head. Note that ErrClosed is therefore not a statement that the backlog was
// empty: [Hub.Close] and [Receiver.Close] abandon whatever is still queued.
//
// The returned value is the current merged contents of the head key's slot. A
// Send that coalesces into that slot between two Peeks changes what the second
// Peek reports but leaves the key's queue position — and so its identity as the
// head — unchanged. A Send whose [Merge] annihilates the head key removes it,
// so the next Peek reports a different key: the head key is stable under
// coalescing, not under annihilation.
//
// Peek takes the hub lock, the same one that serializes the whole Send
// fan-out, so polling it in a loop slows every publisher and every other
// receiver on the hub. Call it once per unit of work, not as a spin.
//
// Peek is safe to call from any goroutine, but like the rest of the receive
// side it is only meaningful on the receiver's single consuming goroutine: a
// concurrent Recv, TryRecv or [Receiver.Chan] feeder may take the peeked event
// before the caller can act on it. An event already handed to the Chan feeder
// has left the queue, so Peek reports ErrEmpty while it is in flight.
func (rx *Receiver[K, V]) Peek() (gobus.Event[K, V], error) {
	var zero gobus.Event[K, V]
	if rx.done.IsClosed() {
		return zero, gobus.ErrClosed
	}
	if rx.forTestingBeforePeekLock != nil {
		rx.forTestingBeforePeekLock()
	}
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	// rx.done can flip between the lock-free check above and acquiring mu;
	// Close holds mu before closing done, so a re-check here is race-free.
	if rx.done.IsClosed() {
		return zero, gobus.ErrClosed
	}
	// Drained-and-closed is terminal however it is observed, so this verdict
	// carries the same tear-down the popping paths' does, under the lock that
	// decided it.
	if rx.drainedLocked() {
		rx.deregisterLocked()
		return zero, gobus.ErrClosed
	}
	if ev, ok := rx.peekLocked(); ok {
		return ev, nil
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
		if rx.drainedLocked() {
			rx.deregisterLocked()
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
	rx.deregisterLocked()
	rx.s.mu.Unlock()
}
