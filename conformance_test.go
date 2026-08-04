// Package gobus_test holds the cross-architecture conformance suite: the
// behavior the [gobus.Sender] and [gobus.Receiver] doc comments promise on
// behalf of *every* bus type in this module.
//
// These tests deliberately drive the handles through the shared interfaces
// rather than a package's concrete types. A bus type that satisfies the
// interface but resolves close/cancel/value in its own order would be silently
// non-substitutable for another on the same interface — the failure the
// per-package tests cannot catch, because a bus that got the ordering backwards
// would still look internally consistent. Adding a bus type means adding a row
// to architectures; if it cannot pass these, the interface doc is wrong and
// needs changing along with it.
package gobus_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
)

// architecture is one bus type under test. newPair returns a live sender and
// receiver on a fresh hub, already registered with each other.
//
// key is the key the suite publishes under. It exists because a bus may bind a
// receiver to a single key at registration — watch does — and a suite that
// published elsewhere would make the negative assertions pass vacuously
// against a bus that simply delivers nothing. Every test body sends to a.key
// rather than a literal, and newPair is handed the same key, so neither side
// can drift out from under a single-key architecture.
type architecture struct {
	name    string
	key     int
	newPair func(t *testing.T, key int) (gobus.Sender[int, int], gobus.Receiver[int, int])
}

var architectures = []architecture{
	{
		name: "conflate",
		key:  1,
		newPair: func(t *testing.T, _ int) (gobus.Sender[int, int], gobus.Receiver[int, int]) {
			t.Helper()
			// latest-wins, never annihilating: the simplest policy that keeps
			// these tests about precedence rather than about coalescing. A
			// conflate receiver takes every key, so it ignores the key.
			h := conflate.New[int](func(_, next int) (int, bool) { return next, true })
			t.Cleanup(h.Close)
			return h.Sender(), h.Receiver()
		},
	},
	{
		name: "watch",
		key:  1,
		newPair: func(t *testing.T, key int) (gobus.Sender[int, int], gobus.Receiver[int, int]) {
			t.Helper()
			// No Accept: latest-wins, the simplest policy, which keeps these
			// tests about precedence. A watch receiver is bound to one key at
			// registration, which is why the key is threaded in rather than
			// written twice. Watch does not deliver its seed, so the receiver
			// starts with nothing unread and the suite's first TryRecv sees
			// ErrEmpty as it does for conflate.
			h := watch.New[int, int]()
			t.Cleanup(h.Close)
			return h.Sender(), h.Watch(key, 0)
		},
	},
	{
		// A second row for one package, which the rule "a bus type is a row"
		// does not by itself call for. watch.Hub.WatchAcross mints a receiver of a
		// different kind — bound to every key rather than one — reached by its
		// own registration and send-routing paths, and it is handed to callers
		// as the same gobus.Receiver. The precedence it owes is therefore the
		// interface's, not watch's, and this is where that is stated. A row is
		// the cheapest way to keep the two kinds from drifting apart.
		name: "watch(across)",
		key:  1,
		newPair: func(t *testing.T, _ int) (gobus.Sender[int, int], gobus.Receiver[int, int]) {
			t.Helper()
			h := watch.New[int, int]()
			t.Cleanup(h.Close)
			// The key is ignored: a wildcard receiver takes whatever the suite
			// publishes, which is what makes the negative assertions here
			// non-vacuous without threading a key in.
			return h.Sender(), h.WatchAcross(0)
		},
	},
}

// TestSendContextPrecedenceConformance pins the send side: closed > cancelled.
// There is no third rank — a send has no ready value competing with the
// cancellation.
func TestSendContextPrecedenceConformance(t *testing.T) {
	for _, a := range architectures {
		t.Run(a.name, func(t *testing.T) {
			t.Run("closed beats cancelled", func(t *testing.T) {
				tx, _ := a.newPair(t, a.key)
				tx.Close()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				assert.ErrorIs(t, tx.SendContext(ctx, a.key, 10), gobus.ErrClosed)
			})
			t.Run("cancelled on a live sender", func(t *testing.T) {
				tx, rx := a.newPair(t, a.key)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				require.ErrorIs(t, tx.SendContext(ctx, a.key, 10), context.Canceled)
				_, err := rx.TryRecv()
				assert.ErrorIs(t, err, gobus.ErrEmpty, "a cancelled send published anyway")
			})
		})
	}
}

// TestRecvContextPrecedenceConformance pins the receive side: closed >
// cancelled > value, in all three of its ranks.
func TestRecvContextPrecedenceConformance(t *testing.T) {
	for _, a := range architectures {
		t.Run(a.name, func(t *testing.T) {
			t.Run("closed receiver beats cancelled", func(t *testing.T) {
				_, rx := a.newPair(t, a.key)
				rx.Close()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := rx.RecvContext(ctx)
				assert.ErrorIs(t, err, gobus.ErrClosed)
			})
			t.Run("drained sender-close beats cancelled", func(t *testing.T) {
				// The receiver is live, so the hard-termination rank falls
				// through; the stream is terminal only because the sender has
				// closed with nothing buffered. A shutdown loop that cancels
				// its own context still has to reach ErrClosed here rather
				// than spin on ctx.Err().
				tx, rx := a.newPair(t, a.key)
				tx.Close()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := rx.RecvContext(ctx)
				assert.ErrorIs(t, err, gobus.ErrClosed)
			})
			t.Run("cancellation is not terminal", func(t *testing.T) {
				// The clause a new bus type is likeliest to get wrong: tearing
				// the handle down on cancellation is the natural
				// implementation, and every other rank here would still pass
				// if it did. Checked through the interface alone — a live
				// handle is one that still delivers on a fresh context.
				tx, rx := a.newPair(t, a.key)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := rx.RecvContext(ctx)
				require.ErrorIs(t, err, context.Canceled)

				// TryRecv, not Recv: Send has already delivered by the time it
				// returns, so this is deterministic — and a bus that did tear
				// the handle down fails here immediately instead of blocking
				// until the package timeout.
				require.NoError(t, tx.Send(a.key, 10))
				ev, err := rx.TryRecv()
				require.NoError(t, err, "cancellation closed the receiver")
				assert.Equal(t, gobus.Event[int, int]{Key: a.key, Value: 10}, ev)
			})
			t.Run("cancelled beats a ready value", func(t *testing.T) {
				// The rank that keeps cancellation observable under load, and
				// the no-discard rule that bounds its cost.
				tx, rx := a.newPair(t, a.key)
				require.NoError(t, tx.Send(a.key, 10))
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := rx.RecvContext(ctx)
				require.ErrorIs(t, err, context.Canceled)

				ev, err := rx.TryRecv()
				require.NoError(t, err, "a cancelled receive consumed the value")
				assert.Equal(t, gobus.Event[int, int]{Key: a.key, Value: 10}, ev)
			})
		})
	}
}

// TestSenderCloseDrainsBeforeClosedConformance pins the one case where a close
// does *not* outrank a buffered value: sender-close is a graceful
// end-of-stream, so what was already sent is delivered first and ErrClosed
// follows only once nothing is left.
//
// This is the direction easiest to get backwards, and getting it backwards is
// invisible per-package — a bus that let sender-close pre-empt the buffer would
// still look internally consistent. Here it fails against its siblings.
func TestSenderCloseDrainsBeforeClosedConformance(t *testing.T) {
	for _, a := range architectures {
		t.Run(a.name, func(t *testing.T) {
			tx, rx := a.newPair(t, a.key)
			require.NoError(t, tx.Send(a.key, 42))
			tx.Close()

			// The value survives the close that raced it.
			ev, err := rx.Recv()
			require.NoError(t, err, "sender-close pre-empted a buffered value")
			assert.Equal(t, gobus.Event[int, int]{Key: a.key, Value: 42}, ev)

			// ...and only then is the stream terminal.
			_, err = rx.Recv()
			assert.ErrorIs(t, err, gobus.ErrClosed)
		})
	}
}
