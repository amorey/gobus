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
	// SendContext behaves like Send but reports a cancelled ctx instead of
	// publishing. On buses whose Send never blocks, ctx is consulted
	// exactly once — there is no parked state for a cancellation to arrive
	// in — at the point the send is resolved rather than on entry. A
	// cancellation that lands while the call is waiting for the bus's own
	// lock is therefore honoured, and nothing is published on behalf of a
	// ctx that has since expired.
	//
	// Precedence is closed > cancelled, and every bus type in this module
	// implements it: a sender already closed on entry returns ErrClosed
	// even for an already-cancelled ctx, since ErrClosed is the durable
	// answer and a retry with a fresh context would only return it again.
	// A cancelled ctx on a live sender returns ctx.Err().
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
	// RecvContext blocks like Recv but reports a cancelled ctx instead of
	// an event. Cancellation does not close the receiver.
	//
	// Precedence is closed > cancelled > value, and every bus type in this
	// module implements it. A termination visible when the answer is
	// derived — this receiver closed, the hub closed, or the sender closed
	// with nothing left to drain — returns ErrClosed even for an
	// already-cancelled ctx, so a shutdown loop that cancels its own
	// context can still drain to ErrClosed rather than spinning on
	// ctx.Err(). Otherwise a cancelled ctx returns ctx.Err() *even when an
	// event is available*, which is what keeps cancellation observable
	// against a publisher fast enough to keep one always ready.
	//
	// A cancelled receive never consumes-and-discards: the event is left
	// for a later receive. Sender-close likewise does not pre-empt what is
	// already buffered — it is a graceful end-of-stream, so buffered
	// events drain first and ErrClosed follows once nothing is left.
	//
	// ctx.Err() is therefore not an end-of-stream and does not deregister
	// the receiver. A caller that stops on it must Close the handle.
	RecvContext(ctx context.Context) (Event[K, V], error)
	// Chan returns a native channel of events for use with select. It
	// carries the same Event values the Recv methods return.
	Chan() <-chan Event[K, V]
	// Close is idempotent.
	Close()
}
