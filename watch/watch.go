// Package watch provides a keyed latest-value state bus.
//
// A [Hub] hands out a singleton [Sender] and any number of [Receiver]s. Each
// receiver watches exactly one key, and [Hub.Watch] is how one is made:
// [Receiver.Close] is the matching unwatch. A [Send] for a key reaches every
// receiver watching it, and a receiver that falls behind skips to the current
// value rather than replaying what it missed.
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
// and a receiver reads a value only once a [Send] supersedes it.
package watch

// Accept reports whether next replaces prev in a receiver's slot. It is the
// caller's rule for which of two values wins.
//
// Accept runs under the bus lock, once for each receiver watching the key, with
// that receiver's own current value as prev. It must not call back into the
// hub, and it must not take any lock a caller may hold while calling
// [Hub.Watch], [Sender.Send] or any Close — Watch is expressly safe to call
// under a producer's lock, so an Accept that takes that same lock inverts the
// two orders and deadlocks. Reading its two arguments and nothing else is
// always safe.
type Accept[V any] func(prev, next V) bool

// config accumulates the options applied to a hub. A zero config is the
// default hub: every value replaces the one before it.
type config[V any] struct {
	accept Accept[V]
}

// Option configures a hub built by [New].
//
// It carries V alone, not K, so WithAccept infers V from its argument and a
// call site spells only K. Adding a K-dependent option would force both type
// arguments on every call site; do not add one without meaning to.
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

// Hub is the construction handle for a watch pipeline.
type Hub[K comparable, V any] struct {
	s  *shared[K, V]
	tx *Sender[K, V]
}

// Sender is the singleton send-side handle. Safe to share across goroutines.
type Sender[K comparable, V any] struct{ s *shared[K, V] }

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
