package gobus

import "context"

// Event is the unit of delivery on a bus: a value together with the key it was
// published under. Every receive path returns one — [Receiver.Recv],
// [Receiver.TryRecv], [Receiver.RecvContext] and [Receiver.Chan] all deal in
// Events, so a handler written against one works with all of them.
type Event[K comparable, V any] struct {
	Key   K
	Value V
}

// Sender is the common send-side interface implemented by every bus type
// in this module.
type Sender[K comparable, V any] interface {
	// Send publishes v under key k. Bus-style publishing never applies
	// backpressure: Send returns as soon as the value has been routed to
	// every interested receiver's buffer. Returns ErrClosed if the sender
	// or hub has been closed.
	Send(k K, v V) error
	// TrySend is the non-blocking form. On buses whose Send never blocks
	// it is equivalent to Send; it exists so call sites can be swapped
	// between architectures. Returns nil on success, or one of: ErrFull
	// (no room to buffer), ErrClosed (sender/hub closed).
	TrySend(k K, v V) error
	// SendContext behaves like Send but returns ctx.Err() if ctx is
	// cancelled. On buses whose Send never blocks, ctx is only consulted
	// at entry.
	SendContext(ctx context.Context, k K, v V) error
	// Close is idempotent.
	Close()
}

// Receiver is the common receive-side interface implemented by every bus
// type in this module.
type Receiver[K comparable, V any] interface {
	// Recv blocks until an event is available or the receiver is closed.
	// On error the returned Event is the zero value.
	Recv() (Event[K, V], error)
	// TryRecv returns immediately without blocking. Returns the next
	// event, or one of: ErrEmpty (nothing buffered), ErrClosed
	// (sender/hub closed and nothing left to drain).
	TryRecv() (Event[K, V], error)
	// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled.
	RecvContext(ctx context.Context) (Event[K, V], error)
	// Chan returns a native channel of events for use with select. It
	// carries the same Event values the Recv methods return.
	Chan() <-chan Event[K, V]
	// Close is idempotent.
	Close()
}
