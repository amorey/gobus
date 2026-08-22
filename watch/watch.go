// Package watch provides a keyed latest-value state bus.
//
// A [Hub] hands out a singleton [Sender] and any number of [Receiver]s. A
// receiver watches exactly one key, made by [Hub.Watch], or every key, made by
// [Hub.WatchAcross]; [Receiver.Close] is the matching unwatch for either. A
// [Sender.Send] for a key reaches every receiver watching it, and a receiver
// that falls behind skips to the current value rather than replaying what it
// missed.
//
// # Registration is the snapshot
//
// [Hub.WithBaseline] takes the value the caller has just read, and that value
// is the baseline every later value is measured against. It is per receiver,
// since each consumer reads at its own instant. This is the opposite of
// github.com/amorey/gochan/watch, whose hub holds one seed and whose
// registration deliberately does *not* snapshot. A reader arriving from the
// sister package must not carry that rule across.
//
// The bus does not deliver the baseline back: it is the caller's own argument,
// and a receiver reads a value only once a [Sender.Send] supersedes it.
// [Receiver.Peek] shows that unread value without taking it, under the same
// closed > value precedence the taking paths use — so it too reports nothing
// for a receiver still on its baseline.
//
// A receiver registered without a baseline has read nothing and holds nothing,
// so its first value is taken whatever it is: there is no prev to give
// [Accept], and the zero V would be a value the caller never held. Accept runs
// on every value after that. Omit the baseline when any current value will do,
// supply one when the consumer already knows the state it is improving on.
//
// # One key for each receiver, or all of them
//
// [Hub.Watch] binds a receiver to its key for life. There is no Unwatch and no
// mutable key set — the constraint is structural, so a consumer watching N
// particular keys holds N receivers and, if it uses [Receiver.Chan], N
// goroutines.
//
// [Hub.WatchAcross] is the one alternative: a receiver that watches every key,
// including keys nobody has published under yet and keys the consumer cannot
// name. It is not a way to subscribe to many keys cheaply — it still holds one
// slot, so a burst across many keys collapses to a single pending value naming
// the last key to land. That collapse is the point. It serves a consumer whose
// reaction to any change is the same, typically "go re-read the store", and it
// serves it in one wake-up rather than one per key.
//
// A consumer that needs each key's own latest value, or the annihilation a
// create-then-delete pair needs, wants github.com/amorey/gobus/conflate, which
// keeps a slot per key and filters at enqueue.
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
// with that receiver's own current value as prev. A receiver holding nothing —
// no [Hub.WithBaseline] and no value yet — takes its first value without
// consulting Accept, since there is no prev to pass. It must not call back into
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

// WatchOption configures a receiver minted by [Hub.Watch] or
// [Hub.WatchAcross]. Options are built by the hub's own [Hub.WithBaseline]
// method, which fixes K and V from the hub — so option call sites need no type
// arguments and a mismatched option fails to compile rather than at run time.
//
// The set of options is closed: the parameter type is unexported, so code
// outside this package cannot name it to write one of its own.
type WatchOption[K comparable, V any] func(*watchConfig[V])

// watchConfig accumulates the options applied to one receiver. A zero
// watchConfig is a receiver with no baseline, whose first value is taken
// unjudged.
//
// hasBaseline carries the "set" bit because the zero V is a usable baseline.
type watchConfig[V any] struct {
	baseline    V
	hasBaseline bool
}

// shared is the hub state common to the sender and every receiver. One mutex
// guards all of it: Send fans a write across the receivers watching a key and
// each read takes its own slot, so a single lock keeps accept/write/read
// consistent without per-receiver locking races.
type shared[K comparable, V any] struct {
	mu     sync.Mutex
	accept Accept[V] // nil = accept every value

	// index is the send-side lookup, so a Send touches only the receivers
	// watching its key. wildcard holds the receivers minted by [Hub.WatchAcross],
	// which watch every key and so cannot be indexed by one; a send fans out to
	// index[k] and then to all of wildcard. receivers is the whole set, for
	// Hub.Close and for the O(1) length live is synced from. All three are
	// mutated at the same sites, and a receiver is in exactly one of index and
	// wildcard.
	//
	// wildcard is a second map rather than a reserved entry in index because
	// index's keys are the hub's live key set — the thing deregisterLocked
	// bounds and forTestingKeyCount reads. A wildcard receiver pins no key, so
	// it must not be able to appear in that set.
	index     map[K]map[*Receiver[K, V]]struct{}
	wildcard  map[*Receiver[K, V]]struct{}
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

// registerLocked adds rx to the whole-set map and to whichever send-side map
// routes to it. Caller holds s.mu.
func (s *shared[K, V]) registerLocked(rx *Receiver[K, V]) {
	if rx.wildcard {
		s.wildcard[rx] = struct{}{}
	} else {
		set := s.index[rx.key]
		if set == nil {
			set = make(map[*Receiver[K, V]]struct{})
			s.index[rx.key] = set
		}
		set[rx] = struct{}{}
	}
	s.receivers[rx] = struct{}{}
	s.live.Sync(len(s.receivers))
}

// deregisterLocked drops rx from the whole-set map and from whichever send-side
// map holds it, removing the key entirely once no receiver watches it — which
// is what bounds hub memory by the live watch set. It rides with
// [Receiver.Close] and with every terminal verdict, so a key costs nothing once
// its last watcher has gone by either path. A wildcard receiver holds no key,
// so it neither keeps one alive nor releases one here. Caller holds s.mu.
func (s *shared[K, V]) deregisterLocked(rx *Receiver[K, V]) {
	if rx.wildcard {
		delete(s.wildcard, rx)
	} else if set := s.index[rx.key]; set != nil {
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
		wildcard:  make(map[*Receiver[K, V]]struct{}),
		receivers: make(map[*Receiver[K, V]]struct{}),
	}
	return &Hub[K, V]{s: s, tx: &Sender[K, V]{s: s}}
}

// Sender returns the singleton send-side handle. Repeated calls return the
// same handle. After the hub is closed it reports [gobus.ErrClosed] on use.
func (h *Hub[K, V]) Sender() *Sender[K, V] { return h.tx }

// Watch makes a receiver for k. The receiver watches k for its whole life;
// [Receiver.Close] is the unwatch.
//
// Without options the receiver has no baseline and its first value is taken
// whatever it is — there is nothing yet for [Accept] to judge it against, and
// nothing the consumer has read that it could fail to improve on. Accept runs
// on every offer after that.
//
// [Hub.WithBaseline] supplies the value the caller has just read, making it the
// prev of the first Accept. It is a baseline, not a delivery: it is never
// handed back through a receive, so a receiver given one reads a value only
// once a [Sender.Send] supersedes it.
//
// Watch calls no caller code, so it is safe to call while holding the
// producer's own lock — which is how a subscriber reads its state and
// registers in one critical section, with no value lost in between. See
// [Accept] for the rule an Accept must obey to keep that safe.
//
// Panics if any option is nil. After [Hub.Close] the returned handle is
// pre-closed. After [Sender.Close] it is live but holds nothing unread, so its
// first read is terminal.
func (h *Hub[K, V]) Watch(k K, opts ...WatchOption[K, V]) *Receiver[K, V] {
	return h.watch(k, false, opts)
}

// WithBaseline makes cur the receiver's starting value: the prev of its first
// [Accept], and never a delivery. Use it when the caller has just read the
// current state and wants the bus to measure against that read rather than
// take the next value on trust. The zero V is a usable baseline.
//
// It is per receiver, not per hub, because each consumer's baseline is the
// value it read at its own instant. [WithAccept], the rule those values are
// judged by, is hub-wide for the opposite reason. See
// docs/adr/2026-08-22-watch-optional-baseline.md.
func (h *Hub[K, V]) WithBaseline(cur V) WatchOption[K, V] {
	return func(c *watchConfig[V]) {
		c.baseline, c.hasBaseline = cur, true
	}
}

// WatchAcross makes a receiver watching every key — every key the hub carries
// now and every key it ever will — holding one slot, like every other receiver
// on this bus. That slot is the latest value published under *any* key, plus
// the key it came from. It takes the same options as [Hub.Watch].
//
// A wildcard subscription in the MQTT or NATS sense is the closest model most
// callers arrive with, and it differs in the two ways most likely to be assumed
// rather than checked:
//
//   - It does not deliver every matching value. One slot means a value that
//     lands while an earlier one is still unread replaces it, so what a
//     consumer reads is the current value, never the sequence. A burst across
//     fifty keys is one wake-up, not fifty. If that loses information the
//     consumer needs, this is the wrong method — see below.
//   - There is no pattern language. It matches every key or nothing; there is
//     no prefix, glob or hierarchy, and there is nowhere to pass one. K is
//     merely comparable, so it carries no structure to match against and no
//     later release can add one.
//
// One slot is the contract, not an artifact of how the slot is written. A burst
// across many keys leaves exactly one value pending, so a consumer whose whole
// reaction is "something changed, go re-read the store" wakes once rather than
// once per key. A consumer that needs each key's own latest value wants one
// [Hub.Watch] per key, or github.com/amorey/gobus/conflate — which keeps a slot
// per key and has the annihilation a create-then-delete pair needs.
//
// [gobus.Event.Key] names the key the slot's value was published under,
// assigned when a value lands. A value the hub's [Accept] rejects changes
// neither the value nor the key, since the slot still holds what it held
// before. A receiver still on its baseline has no key at all — it has read
// nothing — which is unobservable, because every read reports [gobus.ErrEmpty]
// until a value lands.
//
// Everything else matches [Hub.Watch]: without options the first value is taken
// unjudged, a [Hub.WithBaseline] value is never delivered back, no caller code
// runs during registration, and the close behavior of all three Close methods
// is the same. A wildcard baseline is a prior value with no key attached, since
// the caller read it before knowing which key would move next. It differs in
// holding no key against the hub — a wildcard receiver keeps no per-key state
// alive, so a key still costs nothing once its last [Hub.Watch] receiver has
// gone.
//
// Panics if any option is nil.
func (h *Hub[K, V]) WatchAcross(opts ...WatchOption[K, V]) *Receiver[K, V] {
	var zero K
	return h.watch(zero, true, opts)
}

// watch mints and registers a receiver. It is the one place a handle is built,
// so the two constructors cannot drift on seeding, on the pre-closed case, or
// on the ordering that makes registration a snapshot.
func (h *Hub[K, V]) watch(k K, wildcard bool, opts []WatchOption[K, V]) *Receiver[K, V] {
	var cfg watchConfig[V]
	for _, opt := range opts {
		if opt == nil {
			panic("gobus: watch.Hub.Watch received a nil WatchOption")
		}
		opt(&cfg)
	}
	rx := &Receiver[K, V]{
		s:        h.s,
		key:      k,
		wildcard: wildcard,
		val:      cfg.baseline,
		hasValue: cfg.hasBaseline,
		notify:   make(chan struct{}),
	}
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
//
// Unlike [Sender.Close], this one keeps the close-versus-send discipline: do
// not call it concurrently with an active Send from another goroutine. It
// tears down the receivers a send fans out to, so a racing send can deliver a
// value into a receiver that is being closed — the value is not lost racily so
// much as delivered to a handle that will never be read again, which is a
// harder thing for a caller to reason about than the sender-close case.
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
	s.wildcard = nil
}

// Sender is the singleton send-side handle. Safe to share across goroutines.
type Sender[K comparable, V any] struct{ s *shared[K, V] }

// Send publishes v as the value of k to every receiver watching k, and to every
// receiver from [Hub.WatchAcross]. Never blocks. A Send for a key nobody watches
// is discarded: there is no receiver and therefore no buffer, so a later
// [Hub.Watch] never sees it. A wildcard receiver is a watcher of every key, so
// a hub with one has no unwatched key.
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
// Safe to call concurrently with a [Sender.Send] or [Sender.SendContext] from
// another goroutine. The two serialize on the one state every send path reads
// without the bus lock — the poisoned live count — so a racing send resolves to
// exactly one of the two orderings: it publishes and returns nil, or it returns
// ErrClosed and publishes nothing. There is no third outcome and no partial
// one. *Which* ordering wins is unspecified, so a caller that needs a send to
// be visible before shutdown must order the two itself; a caller shutting down
// and not caring whether the last value lands need not fence anything.
//
// This is a promise about watch specifically, and it holds because Send never
// parks: the whole of Close runs under s.mu, and the only step of a send that
// runs outside it is the atomic load Close poisons. Do not read it as a
// module-wide rule — [Hub.Close] keeps the close-versus-send discipline stated
// in its own doc, since it tears down receivers a send is fanning out to.
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
	// The key's own watchers, then every wildcard. A receiver is in exactly one
	// of the two maps, so neither loop can offer the same value twice, and a
	// hub with no wildcard receiver pays one empty range for the second.
	for rx := range s.index[k] {
		rx.offerLocked(k, v)
	}
	for rx := range s.wildcard {
		rx.offerLocked(k, v)
	}
	return false, nil
}

// Receiver is a receive-side handle, intended for one consumer goroutine. It
// watches exactly one key, or every key when minted by [Hub.WatchAcross]. Either
// way it holds a single slot rather than a queue: a value that [Accept] takes
// overwrites the one before it, so a slow reader skips to the current value
// instead of building a backlog.
type Receiver[K comparable, V any] struct {
	s *shared[K, V]

	// key is the key this receiver watches, and wildcard says which of the two
	// kinds it is. For a wildcard receiver key is instead the key of the value
	// currently in the slot, written by offerLocked as the value lands and
	// meaningless until one does; wildcard itself is set at construction and
	// never written again, so no read of it needs s.mu.
	key      K
	wildcard bool

	// val is the current value and version counts the values Accept has taken.
	// lastSeen is the read position: val is unread exactly while version >
	// lastSeen. Registration seeds them equal, so a baseline the caller
	// supplied is never delivered back to it.
	//
	// hasValue says whether val means anything yet. It is false on a receiver
	// registered without [Hub.WithBaseline] and true from the first value that
	// lands, which is what lets offerLocked skip Accept exactly once: there is
	// no prev to pass it, and the zero V would be a value the caller never
	// held.
	//
	// lastSeen lives here under s.mu rather than in the reading goroutine: a
	// receiver with a Chan feeder is read by two goroutines, so single-consumer
	// ownership is an intent, not an invariant.
	val      V
	hasValue bool
	version  uint64
	lastSeen uint64

	notify  chan struct{} // closed+replaced to wake parked readers
	waiters int           // parked readers; zero means no wake is needed
	done    buscore.CloseOnce

	chOnce sync.Once
	ch     chan gobus.Event[K, V]

	// forTestingBeforeRecvLock, forTestingBeforeTryRecvLock,
	// forTestingBeforePeekLock and forTestingFeederBeforeLock, if non-nil, run
	// after a lock-free closed check and before taking s.mu, so tests can
	// exercise the close-wins-the-race re-check under the lock.
	// forTestingFeederParked runs after the feeder snapshots a value and
	// before it enters the delivery select, so a test can land a newer value
	// inside that window.
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
	forTestingBeforePeekLock    func()
	forTestingFeederBeforeLock  func()
	forTestingFeederParked      func()
	forTestingFeederExit        func()
}

// offerLocked puts v, published under k, to this receiver's Accept and takes it
// on a true result. Caller holds s.mu.
//
// A wildcard receiver's key travels with its value: the key is written here,
// alongside the value it names, and only when Accept takes it. A rejected value
// leaves the slot exactly as it was, key included, so the pair a read hands
// back is always the one that landed together. For a single-key receiver k is
// its own key by construction and the write is a no-op.
func (rx *Receiver[K, V]) offerLocked(k K, v V) {
	// An empty slot has no prev to give Accept, and a receiver that has read
	// nothing has nothing this value could fail to improve on, so the first
	// value lands unjudged. From then on the slot always holds a value and
	// Accept runs on every offer.
	if rx.hasValue && rx.s.accept != nil && !rx.s.accept(rx.val, v) {
		return
	}
	rx.key = k
	rx.val = v
	rx.hasValue = true
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

// eventLocked returns the slot's current contents without marking it read. It
// is the one place an Event is built from a slot, so the peeking and the
// taking paths cannot drift about what a receiver's value is. Caller holds
// s.mu.
func (rx *Receiver[K, V]) eventLocked() gobus.Event[K, V] {
	return gobus.Event[K, V]{Key: rx.key, Value: rx.val}
}

// takeLocked marks the slot read and returns its value. Caller holds s.mu, and
// must have found unreadLocked true.
func (rx *Receiver[K, V]) takeLocked() gobus.Event[K, V] {
	rx.lastSeen = rx.version
	return rx.eventLocked()
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

// Peek returns the value a receive would hand back, without taking it: a
// subsequent [Receiver.Recv] or [Receiver.TryRecv] still returns it. It is
// TryRecv minus the take, and shares its precedence exactly —
// [gobus.ErrClosed] if the receiver or hub is closed, or the sender is closed
// and the final value has been taken; [gobus.ErrEmpty] if nothing has
// superseded what this receiver has already seen.
//
// It is therefore *not* a read of the key's current state: a receiver that has
// caught up reports ErrEmpty even though its slot holds a perfectly good
// value, and a closed handle reports ErrClosed with one waiting. Keep your own
// copy of the last value read if you need the current state on demand — that
// is what the reading goroutine already has, and it costs no lock.
//
// Between two Peeks the value is not fixed: a [Sender.Send] this receiver's
// [Accept] takes replaces the slot, so the second Peek reports the newer value
// and the older one is never handed back by either path. That is the same
// skip-ahead every read on this bus is subject to, only visible without
// consuming. For a receiver from [Hub.Watch] the key *is* fixed, since it
// watches one key for its whole life; for one from [Hub.WatchAcross] the key
// travels with the value, so a replacing value can change it too.
//
// Peek takes the hub lock, the same one that serializes the whole Send
// fan-out, so polling it in a loop slows every publisher and every other
// receiver on the hub. Call it once per unit of work, not as a spin.
//
// Peek is safe to call from any goroutine, but like the rest of the receive
// side it is only meaningful on the receiver's single consuming goroutine: a
// concurrent Recv, TryRecv or [Receiver.Chan] feeder may take the value before
// the caller can act on it. Unlike conflate's Peek, a value already handed to
// the feeder is still visible here — the feeder marks it read only once the
// consumer has taken it, so Peek reports the value in flight rather than
// ErrEmpty.
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
	// The same ordered run TryRecv makes, minus the take. A peek is a read like
	// any other, so a receiver that reaches its terminal answer here owes the
	// same tear-down, which terminalLocked carries.
	if rx.terminalLocked() {
		return zero, gobus.ErrClosed
	}
	if rx.unreadLocked() {
		return rx.eventLocked(), nil
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
			ev := rx.eventLocked()
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
