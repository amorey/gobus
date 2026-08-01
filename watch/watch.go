// Package watch provides a keyed latest-value state bus.
//
// A [Hub] hands out a singleton [Sender] and any number of [Receiver]s. Each
// receiver watches exactly one key, and [Hub.Watch] is how one is made:
// [Receiver.Close] is the matching unwatch. A [Sender.Send] for a key reaches
// every receiver watching it, and a receiver that falls behind skips to the
// current value rather than replaying what it missed.
//
// # Registration is the snapshot
//
// [Hub.Watch] takes the value the caller has just read, and that value is the
// baseline every later value is measured against. This is the opposite of
// github.com/amorey/gochan/watch, whose hub holds one seed and whose
// registration deliberately does *not* snapshot. A reader arriving from the
// sister package must not carry that rule across.
//
// The bus does not deliver the baseline back: it is the caller's own argument,
// and a receiver reads a value only once a [Sender.Send] supersedes it.
//
// # One key for each receiver
//
// There is no Unwatch and no mutable key set — the constraint is structural. A
// consumer watching N keys therefore holds N receivers and, if it uses
// [Receiver.Chan], N goroutines, so this package is deliberately unsuited to
// wide subscriptions. A wide change-stream consumer wants
// github.com/amorey/gobus/conflate, which has the annihilation a
// create-then-delete pair needs.
package watch

import (
	"context"
	"sync"

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/internal/buscore"
)

// Accept reports whether next replaces prev in a receiver's slot. It is the
// caller's rule for which of two values wins.
//
// Accept runs under the bus lock, once for each receiver watching the key,
// with that receiver's own current value as prev. It must not call back into
// the hub, and it must not take any lock a caller may hold while calling
// [Hub.Watch], [Sender.Send] or any Close — Watch is expressly safe to call
// under a producer's lock, so an Accept that takes that same lock inverts the
// two orders and deadlocks. Reading its two arguments and nothing else is
// always safe.
//
// A panic out of Accept leaves a partial fan-out: the receivers already
// reached keep the value, the rest are untouched, the send is not retried and
// the hub stays usable.
type Accept[V any] func(prev, next V) bool

// config accumulates the options applied to a hub. A zero config is the
// default hub: every value replaces the one before it.
type config[V any] struct {
	accept Accept[V]
}

// Option configures a hub built by [New].
//
// It carries V alone, not K, so [WithAccept] infers V from its argument and a
// call site passing one spells only K. Adding a K-dependent option would force
// both type arguments at every such call site; do not add one without meaning
// to. This is also why the option is a package-level function rather than a
// method on the hub, as conflate's per-receiver options must be: those
// configure a handle whose hub has already fixed both types, while this one
// has to be built before the hub exists.
type Option[V any] func(*config[V])

// WithAccept sets the rule deciding whether a value replaces the one in a
// receiver's slot. Without it every value is accepted, which is
// last-writer-wins. Panics if fn is nil.
func WithAccept[V any](fn Accept[V]) Option[V] {
	if fn == nil {
		panic("gobus: watch.WithAccept requires a non-nil Accept")
	}
	return func(c *config[V]) { c.accept = fn }
}

// shared is the hub state common to the sender and every receiver. One mutex
// guards all of it: Send fans a write across the receivers watching a key and
// each read takes its own slot, so a single lock keeps accept/write/read
// consistent without per-receiver locking races.
type shared[K comparable, V any] struct {
	mu     sync.Mutex
	accept Accept[V] // nil = accept every value

	// index is the send-side lookup, so a Send touches only the receivers
	// watching its key. receivers is the whole set, for Hub.Close and for the
	// O(1) length live is synced from. Both are mutated at the same sites.
	index     map[K]map[*Receiver[K, V]]struct{}
	receivers map[*Receiver[K, V]]struct{}

	txClosed  bool
	hubClosed bool

	// live is the lock-free receiver count gating the send fast path, synced
	// only under mu at every site that mutates receivers. See
	// [buscore.LiveCount] for why it is derived rather than incremented, and
	// why closedness is folded into it — that is what lets txClosed stay a
	// plain bool read only under mu.
	live buscore.LiveCount

	// forTestingBeforeSendLock, if non-nil, runs on every send path about to
	// take mu, after SendContext has taken ctx's Done channel and before the
	// lock is acquired. A test can therefore land a cancellation in the window
	// where the send waits for the lock, and count the sends that reach it.
	// Hub-wide and read outside mu — it has to be, since the window it opens is
	// the wait for mu itself — so arm it only while no send is in flight. nil
	// in production.
	forTestingBeforeSendLock func()
}

// registerLocked adds rx to both maps. Caller holds s.mu.
func (s *shared[K, V]) registerLocked(rx *Receiver[K, V]) {
	set := s.index[rx.key]
	if set == nil {
		set = make(map[*Receiver[K, V]]struct{})
		s.index[rx.key] = set
	}
	set[rx] = struct{}{}
	s.receivers[rx] = struct{}{}
	s.live.Sync(len(s.receivers))
}

// deregisterLocked drops rx from both maps, removing the key entirely once no
// receiver watches it — which is what bounds hub memory by the live watch set.
// It rides with [Receiver.Close] and with every terminal verdict, so a key
// costs nothing once its last watcher has gone by either path. Caller holds
// s.mu.
func (s *shared[K, V]) deregisterLocked(rx *Receiver[K, V]) {
	if set := s.index[rx.key]; set != nil {
		delete(set, rx)
		if len(set) == 0 {
			delete(s.index, rx.key)
		}
	}
	delete(s.receivers, rx)
	s.live.Sync(len(s.receivers))
}

// Hub is the construction handle for a watch pipeline.
type Hub[K comparable, V any] struct {
	s  *shared[K, V]
	tx *Sender[K, V]
}

// New creates a hub. Panics if any option is nil.
func New[K comparable, V any](opts ...Option[V]) *Hub[K, V] {
	var cfg config[V]
	for _, opt := range opts {
		if opt == nil {
			panic("gobus: watch.New received a nil Option")
		}
		opt(&cfg)
	}
	s := &shared[K, V]{
		accept:    cfg.accept,
		index:     make(map[K]map[*Receiver[K, V]]struct{}),
		receivers: make(map[*Receiver[K, V]]struct{}),
	}
	return &Hub[K, V]{s: s, tx: &Sender[K, V]{s: s}}
}

// Sender returns the singleton send-side handle. Repeated calls return the
// same handle. After the hub is closed it reports [gobus.ErrClosed] on use.
func (h *Hub[K, V]) Sender() *Sender[K, V] { return h.tx }

// Watch makes a receiver for k, seeded with initial as the value the caller
// has just read. The receiver watches k for its whole life; [Receiver.Close]
// is the unwatch.
//
// initial is the baseline, not a delivery: it is the prev of the first
// [Accept] call, and it is never handed back through a receive. A receiver
// reads a value only once a [Sender.Send] supersedes the baseline.
//
// Watch calls no caller code, so it is safe to call while holding the
// producer's own lock — which is how a subscriber reads its state and
// registers in one critical section, with no value lost in between. See
// [Accept] for the rule an Accept must obey to keep that safe.
//
// After [Hub.Close] the returned handle is pre-closed. After [Sender.Close] it
// is live but holds nothing unread, so its first read is terminal.
func (h *Hub[K, V]) Watch(k K, initial V) *Receiver[K, V] {
	rx := &Receiver[K, V]{s: h.s, key: k, val: initial, notify: make(chan struct{})}
	rx.done.Init()
	h.s.mu.Lock()
	if h.s.hubClosed {
		rx.done.Close()
	} else {
		h.s.registerLocked(rx)
	}
	h.s.mu.Unlock()
	return rx
}

// Close is hard tear-down: the sender and every live receiver are closed
// immediately, with no final drain. Use [Sender.Close] for the soft path.
// Future [Hub.Watch] calls return pre-closed handles. Idempotent.
//
// "No drain" is a statement about the reading methods, which report
// [gobus.ErrClosed] at once. A [Receiver.Chan] consumer can still receive one
// value after Close returns: if the feeder had already committed to a delivery
// the close makes both arms of its select ready, and Go picks between ready
// arms at random. The channel closes immediately after. See [Receiver.Chan].
func (h *Hub[K, V]) Close() {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hubClosed {
		return
	}
	s.hubClosed = true
	s.txClosed = true
	s.live.Poison()
	for rx := range s.receivers {
		rx.done.Close()
	}
	s.receivers = nil
	s.index = nil
}

// Sender is the singleton send-side handle. Safe to share across goroutines.
type Sender[K comparable, V any] struct{ s *shared[K, V] }

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
	// Idle means no receiver *and* an open send side, since close poisons the
	// count. There is nothing to fan out to and no other answer to give, so
	// the hub-wide lock is pure cost here. See [buscore.LiveCount].
	if s.live.Idle() {
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

// TrySend is equivalent to Send: Send never blocks, so there is no separate
// non-blocking path. Provided to satisfy [gobus.Sender].
func (tx *Sender[K, V]) TrySend(k K, v V) error { return tx.Send(k, v) }

// SendContext behaves like Send but reports a cancelled ctx instead of
// publishing. Send never blocks, so ctx is consulted once — at the point the
// send is resolved, under the bus lock, rather than on entry. A cancellation
// landing while the call waits for that lock is therefore honoured, and
// nothing is published for a ctx that has since expired.
//
// Precedence is closed > cancelled: a sender already closed reports
// [gobus.ErrClosed] even for an already-cancelled ctx, since that is the
// durable answer and a retry with a fresh context would only return it again.
//
// Only ctx's Done channel is read under the bus lock; ctx.Err() is called
// after it is released, so a context implementation that locks cannot deadlock
// against another goroutine's Send or Close. See sendLocked.
func (tx *Sender[K, V]) SendContext(ctx context.Context, k K, v V) error {
	s := tx.s
	ctxDone := ctx.Done()
	// The no-receiver fast path answers only nil, and that restriction is what
	// keeps it correct. A live ctx and an idle count resolve the whole call at
	// the load. A cancelled ctx cannot be answered here, because the count and
	// ctxDone are read at two different moments: a Sender.Close landing between
	// them would make ctx.Err() right at neither — nil at the load, ErrClosed
	// by the select. Falling through costs one acquisition and derives closed >
	// cancelled from a single consistent view.
	if s.live.Idle() {
		select {
		case <-ctxDone:
		default:
			return nil
		}
	}
	if s.forTestingBeforeSendLock != nil {
		s.forTestingBeforeSendLock()
	}
	// The locked section is a closure so the unlock is deferred: sendLocked
	// runs the caller's Accept, and a panic out of it must still release s.mu
	// or a recovering caller finds the whole hub wedged. Resolving ctx.Err()
	// has to happen after the unlock, which rules out deferring in the body.
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
	s.live.Poison()
	for rx := range s.receivers {
		rx.signalLocked()
	}
}

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

// Receiver is a receive-side handle watching exactly one key, intended for one
// consumer goroutine. It holds a single slot rather than a queue: a value that
// [Accept] takes overwrites the one before it, so a slow reader skips to the
// current value instead of building a backlog.
type Receiver[K comparable, V any] struct {
	s   *shared[K, V]
	key K

	// val is the current value and version counts the values Accept has taken.
	// lastSeen is the read position: val is unread exactly while version >
	// lastSeen. Watch seeds them equal, so the caller's own initial is never
	// delivered back to it.
	//
	// lastSeen lives here under s.mu rather than in the reading goroutine: a
	// receiver with a Chan feeder is read by two goroutines, so single-consumer
	// ownership is an intent, not an invariant.
	val      V
	version  uint64
	lastSeen uint64

	notify  chan struct{} // closed+replaced to wake parked readers
	waiters int           // parked readers; zero means no wake is needed
	done    buscore.CloseOnce

	chOnce sync.Once
	ch     chan gobus.Event[K, V]

	// forTestingBeforeRecvLock, forTestingBeforeTryRecvLock and
	// forTestingFeederBeforeLock, if non-nil, run after a lock-free closed
	// check and before taking s.mu, so tests can exercise the
	// close-wins-the-race re-check under the lock. forTestingFeederParked runs
	// after the feeder snapshots a value and before it enters the delivery
	// select, so a test can land a newer value inside that window.
	//
	// forTestingFeederExit runs on the way out, just before the channel is
	// closed. It is what lets a test wait for the feeder without reading the
	// channel — which matters because a reader would make the delivery select's
	// "deliver vs. closed" arms both ready, and Go would pick between them at
	// random. Waiting here instead keeps that arm deterministic.
	//
	// All nil in production, and all must be armed before Chan starts the
	// feeder, or the write races the feeder's read.
	forTestingBeforeRecvLock    func()
	forTestingBeforeTryRecvLock func()
	forTestingFeederBeforeLock  func()
	forTestingFeederParked      func()
	forTestingFeederExit        func()
}

// offerLocked puts v to this receiver's Accept and takes it on a true result.
// Caller holds s.mu.
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

// unreadLocked reports whether the slot holds a value this receiver has not
// taken. Caller holds s.mu.
func (rx *Receiver[K, V]) unreadLocked() bool { return rx.version > rx.lastSeen }

// takeLocked marks the slot read and returns its value. Caller holds s.mu, and
// must have found unreadLocked true.
func (rx *Receiver[K, V]) takeLocked() gobus.Event[K, V] {
	rx.lastSeen = rx.version
	return gobus.Event[K, V]{Key: rx.key, Value: rx.val}
}

// drainedLocked reports whether the stream has ended: the sender is gone and
// this receiver has taken the final value, so nothing can arrive again. It is
// exactly when the read would fail. Caller holds s.mu.
func (rx *Receiver[K, V]) drainedLocked() bool { return rx.s.txClosed && !rx.unreadLocked() }

// terminalLocked reports whether this receiver is finished, and carries the
// tear-down the drained verdict owes under the lock that decided it. It is the
// shared prefix of the reading paths' ordered run, not a reordering of it: the
// cancellation and value steps stay at their call sites.
//
// The done re-check is why this runs under the lock at all. Close serializes
// through s.mu, so a Close that won the race against a caller's lock-free
// pre-check is visible here and cannot be handed a value.
//
// The feeder does not use it: it checks unread *before* drained, so a
// sender-close still delivers the final value.
func (rx *Receiver[K, V]) terminalLocked() bool {
	if rx.done.IsClosed() {
		return true
	}
	if rx.drainedLocked() {
		rx.s.deregisterLocked(rx)
		return true
	}
	return false
}

// TryRecv returns the current value if this receiver has not taken it,
// [gobus.ErrEmpty] if nothing has changed since it subscribed, or
// [gobus.ErrClosed] if the receiver or hub is closed, or the sender is closed
// and the final value has been taken.
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
	if rx.terminalLocked() {
		return zero, gobus.ErrClosed
	}
	if rx.unreadLocked() {
		return rx.takeLocked(), nil
	}
	return zero, gobus.ErrEmpty
}

// Recv blocks until a value this receiver has not taken is available, then
// returns it. It returns [gobus.ErrClosed] once the receiver or hub is closed,
// or once the sender is closed and the final value has been taken.
func (rx *Receiver[K, V]) Recv() (gobus.Event[K, V], error) {
	return rx.recvLoop(context.Background())
}

// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled
// first. Cancellation does not close this receiver.
//
// It implements the closed > cancelled > value precedence documented on
// [gobus.Receiver] — including that a cancelled read never consumes the value
// it declined, and that reaching ctx.Err() neither closes nor deregisters the
// receiver. What is watch-specific is the cost of ignoring the latter: an
// abandoned handle holds its key against the hub for the hub's lifetime.
// `defer rx.Close()` covers it, as it does for any abandoned receiver.
func (rx *Receiver[K, V]) RecvContext(ctx context.Context) (gobus.Event[K, V], error) {
	return rx.recvLoop(ctx)
}

// recvLoop is the shared blocking-recv implementation. Recv passes
// context.Background() to opt out of cancellation — Background's Done()
// returns nil, and a nil channel in a select arm is never ready, so the
// cancellation check falls straight through to its default on that path.
//
// The whole closed > cancelled > value precedence is evaluated in one ordered
// run under s.mu rather than split between the lock-free probe and the locked
// body. Two reasons. The terminal exit carries a tear-down obligation —
// dropping this receiver, and its key with it — that has to happen under the
// same lock that decided it was terminal. And the cancellation check must sit
// above the read, or the only cancellation arm would be the <-ctxDone below,
// reachable only once parked, so a receiver looping on RecvContext against a
// publisher fast enough to keep a value always unread would take the value
// every iteration and never observe its own shutdown.
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
		// closed: this handle, the hub, or a drained sender-close.
		if rx.terminalLocked() {
			s.mu.Unlock()
			return zero, gobus.ErrClosed
		}
		// cancelled: above the read, so an unread value cannot starve it.
		select {
		case <-ctxDone:
			s.mu.Unlock()
			return zero, ctx.Err()
		default:
		}
		// value: the current value, if this receiver has not taken it.
		if rx.unreadLocked() {
			ev := rx.takeLocked()
			s.mu.Unlock()
			return ev, nil
		}
		rx.waiters++
		parked = true
		notify := rx.notify
		s.mu.Unlock()
		// Every arm falls through to the top rather than deciding here. A wake
		// carries no verdict — only "state changed, look again" — so the
		// ordered run above stays the single place the precedence is
		// evaluated. Returning ErrClosed or ctx.Err() from these arms would
		// hand the decision to the select: with a close and a cancellation both
		// landing on a parked reader, both arms are ready and Go picks
		// uniformly, which would also skip the terminal tear-down.
		select {
		case <-notify:
		case <-rx.done.Done():
		case <-ctxDone:
		}
	}
}

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
// it is read. Once the feeder has committed to a delivery, anything that makes
// its select's other arms ready is racing that delivery, and Go chooses
// between ready arms at random. Two consequences:
//
//   - A newer value arriving can lose the race, so a superseded value is
//     sometimes delivered, with the newer one immediately behind it. Values
//     still arrive in order, and a consumer that keeps reading converges on
//     the current value.
//   - A [Receiver.Close] or [Hub.Close] can lose it too, so one value can
//     still be received after either returns, even though both are documented
//     as abandoning what is unread. The channel closes immediately after.
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
// it, and marks the value read only once the consumer has taken it — so a
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
	// iteration, where it is written to lastSeen under the lock. Committing it
	// inside the delivery select would cost a second acquisition per value,
	// and every field mutation belongs under s.mu since the feeder is not this
	// receiver's only reader.
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
		// max, not assignment: a concurrent Recv on the same handle can have
		// moved lastSeen past what this feeder delivered.
		rx.lastSeen = max(rx.lastSeen, delivered)
		delivered = 0
		// Re-check under the lock, as terminalLocked does for the reading
		// paths: Close serializes through s.mu, so a Close that won the race
		// against the pre-lock check cannot see one more value delivered.
		if rx.done.IsClosed() {
			s.mu.Unlock()
			return
		}
		// unread before drained, unlike the reading paths: a sender-close must
		// still hand over the final value before the channel closes.
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

// Close is the unwatch: it closes this handle, discards any unread value and
// drops the key from the hub once no other receiver watches it. Other
// receivers and the sender are unaffected. Idempotent.
//
// A [Receiver.Chan] consumer can still receive one value after Close returns;
// see Chan for why.
func (rx *Receiver[K, V]) Close() {
	// Close under mu so a concurrent read that acquired mu first cannot hand
	// back a value to a now-closed receiver: readers re-check rx.done after
	// taking mu, and that check is stable only while Close serializes here too.
	rx.s.mu.Lock()
	rx.done.Close()
	rx.s.deregisterLocked(rx)
	rx.s.mu.Unlock()
}
