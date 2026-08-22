package conflate

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gobus"
)

// latestWins is a test merge: the newer value supersedes the older, and a
// negative value annihilates the key (keep=false). It exercises both the
// coalesce and the annihilate paths with plain ints.
func latestWins(_, next int) (int, bool) { return next, next >= 0 }

// assertRecv pops the next event and asserts it carries exactly wantKey and
// wantVal. Asserting the whole Event at once also pins the key/value pairing,
// which two separate assertions would let drift apart.
func assertRecv[K comparable, V comparable](t *testing.T, rx *Receiver[K, V], wantKey K, wantVal V) {
	t.Helper()
	ev, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[K, V]{Key: wantKey, Value: wantVal}, ev)
}

// assertEmpty asserts nothing is pending. TryRecv answers that question
// synchronously, so no timeout is needed to establish "the queue is drained".
func assertEmpty[K comparable, V any](t *testing.T, rx *Receiver[K, V]) {
	t.Helper()
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

// waitParked spins until the receiver has a reader parked in its blocking
// select, so a subsequent Close/Send is guaranteed to land in-select rather
// than being observed by one of the pre-park checks.
//
// waiters is incremented under s.mu just *before* the goroutine enters the
// select, so a return here proves the reader is committed to parking, not that
// it has already reached the select. That gap is harmless and cannot be closed
// by polling harder: notify is captured under the same lock that raised the
// count, so a close-and-replace landing in the gap is still observed by the
// select the reader is about to enter. Tests that need more than "committed to
// park" must gate on s.mu rather than on this — see
// TestParkedCloseAndCancelResolveToClosed.
func waitParked[K comparable, V any](t *testing.T, rx *Receiver[K, V]) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rx.s.mu.Lock()
		parked := rx.waiters > 0
		rx.s.mu.Unlock()
		if parked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("reader never parked")
		}
		runtime.Gosched()
	}
}

// parkedRecv starts a RecvContext on rx in its own goroutine and returns once
// that reader has parked, so the caller can fire a Send/Close/cancel at a
// reader that is provably past the entry-time checks. The error channel is
// buffered so the reader never blocks handing its result back.
//
// Pass context.Background() for the Recv (no cancellation) case: recvLoop
// takes that path itself, so it is the same code under test.
func parkedRecv[K comparable, V any](t *testing.T, rx *Receiver[K, V], ctx context.Context) <-chan error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		_, err := rx.RecvContext(ctx)
		errCh <- err
	}()
	waitParked(t, rx)
	return errCh
}

func TestImplementsCommonInterfaces(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	var _ gobus.Sender[int, int] = h.Sender()
	var _ gobus.Receiver[int, int] = h.Receiver()
}

// TestEventIsTheSingleCurrency pins the property that makes Event worth having:
// every receive path hands back the same type, so one handler serves them all.
func TestEventIsTheSingleCurrency(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	defer rx.Close()
	tx := h.Sender()

	seen := make([]gobus.Event[int, int], 0, 3)
	handle := func(ev gobus.Event[int, int]) { seen = append(seen, ev) }

	require.NoError(t, tx.Send(1, 10))
	ev, err := rx.Recv()
	require.NoError(t, err)
	handle(ev)

	require.NoError(t, tx.Send(2, 20))
	ev, err = rx.TryRecv()
	require.NoError(t, err)
	handle(ev)

	require.NoError(t, tx.Send(3, 30))
	ev, err = rx.RecvContext(context.Background())
	require.NoError(t, err)
	handle(ev)

	assert.Equal(t, []gobus.Event[int, int]{{Key: 1, Value: 10}, {Key: 2, Value: 20}, {Key: 3, Value: 30}}, seen)
}

func TestChanCarriesTheSameEventType(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	defer rx.Close()
	require.NoError(t, h.Sender().Send(1, 10))
	// The same handler signature the Recv methods feed also drains Chan.
	handle := func(ev gobus.Event[int, int]) gobus.Event[int, int] { return ev }
	assert.Equal(t, gobus.Event[int, int]{Key: 1, Value: 10}, handle(<-rx.Chan()))
}

func TestNewRejectsNilOption(t *testing.T) {
	assert.Panics(t, func() { New[int, int](nil) })
}

func TestWithDefaultMergeRejectsNilMerge(t *testing.T) {
	assert.Panics(t, func() { WithDefaultMerge[int](nil) })
}

// TestDefaultMergeIsLatestWins pins what omitting WithDefaultMerge means: a
// Send for a key already pending replaces the undelivered value and the slot
// survives. The last value is negative because latestWins annihilates on one —
// a hub that picked that merge up fails here rather than passing by accident.
func TestDefaultMergeIsLatestWins(t *testing.T) {
	h := New[int, int]()
	rx := h.Receiver()
	defer rx.Close()
	tx := h.Sender()

	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.Send(1, 20))
	require.NoError(t, tx.Send(1, -1))
	assertRecv(t, rx, 1, -1)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

// TestWithDefaultMergeOverridesTheDefault pins the option as the hub-wide
// policy, annihilation included.
func TestWithDefaultMergeOverridesTheDefault(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	defer rx.Close()
	tx := h.Sender()

	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.Send(1, -1)) // latestWins annihilates on a negative
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

// TestLastDefaultMergeWins pins the option-application order New promises.
func TestLastDefaultMergeWins(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins), WithDefaultMerge(func(prev, next int) (int, bool) {
		return prev + next, true
	}))
	rx := h.Receiver()
	defer rx.Close()
	tx := h.Sender()

	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.Send(1, -1)) // the earlier merge would annihilate
	assertRecv(t, rx, 1, 9)
}

// TestReceiverMergeOverridesTheDefaultedHubMerge pins the fallback on a hub
// that named no merge: the receiver without its own resolves to latest, not to
// nil.
func TestReceiverMergeOverridesTheDefaultedHubMerge(t *testing.T) {
	h := New[int, int]()
	plain := h.Receiver()
	defer plain.Close()
	summing := h.Receiver(h.WithMerge(func(prev, next int) (int, bool) { return prev + next, true }))
	defer summing.Close()
	tx := h.Sender()

	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.Send(1, 5))
	assertRecv(t, plain, 1, 5)    // hub default: latest wins
	assertRecv(t, summing, 1, 15) // its own merge
}

func TestSenderSingleton(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	assert.Same(t, h.Sender(), h.Sender())
}

func TestBasicDelivery(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 100))
	assertRecv(t, rx, 1, 100)
	assertEmpty(t, rx)
}

func TestRecvWakesParkedReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	got := make(chan gobus.Event[int, int], 1)
	go func() {
		ev, err := rx.Recv()
		assert.NoError(t, err)
		got <- ev
	}()
	// The goroutine parks (queue empty); the Send must wake it.
	waitParked(t, rx)
	require.NoError(t, h.Sender().Send(7, 42))
	assert.Equal(t, gobus.Event[int, int]{Key: 7, Value: 42}, <-got)
}

// TestCloseWakesParkedReceiver covers the in-select close path: a receiver
// already parked in the blocking select must wake with ErrClosed when Close
// fires (distinct from a Close observed by the pre-lock check).
func TestCloseWakesParkedReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	errCh := parkedRecv(t, rx, context.Background())
	rx.Close()
	assert.ErrorIs(t, <-errCh, gobus.ErrClosed)
}

func TestRecvContextCancel(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := parkedRecv(t, rx, ctx)
	cancel()
	assert.ErrorIs(t, <-errCh, context.Canceled)
}

// TestRecvContextCancelBeatsPendingValue pins the middle rank of recvLoop's
// closed > cancelled > value precedence at entry time: a cancelled ctx wins
// over an event that is already pending, and does not consume it. Without
// this, a consumer looping on RecvContext against a publisher fast enough to
// keep something always queued would never observe its own shutdown signal.
func TestRecvContextCancelBeatsPendingValue(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, h.Sender().Send(1, 1))

	_, err := rx.RecvContext(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// The event was left queued, not consumed and discarded.
	assertRecv(t, rx, 1, 1)
}

// TestRecvContextCancelBeatsValueDeliveredWhileParked covers the same
// precedence on the parked path rather than the entry-time one. The receiver
// is already blocked in the parking select when the event and the cancellation
// both land, so the wake races between the <-notify and <-ctxDone arms. Either
// way the next loop iteration re-derives the answer from state and must return
// ctx.Err() with the event still queued: waking consumes nothing, and the
// loop-top ctx check sits above the pop.
func TestRecvContextCancelBeatsValueDeliveredWhileParked(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := parkedRecv(t, rx, ctx)

	// Cancel from the pre-lock hook rather than before the Send, so the parked
	// receiver is woken by <-notify with an event genuinely queued and only
	// then observes the cancellation. Sending first and cancelling after would
	// let <-ctxDone win the wake outright, and the value path — the one this
	// test exists to cover — would never run.
	rx.forTestingBeforeRecvLock = func() { cancel() }
	require.NoError(t, h.Sender().Send(7, 42))

	require.ErrorIs(t, <-errCh, context.Canceled)
	rx.forTestingBeforeRecvLock = nil
	assertRecv(t, rx, 7, 42) // not consumed by the cancelled parked receive
}

// TestParkedCloseAndCancelResolveToClosed covers the case the hook-driven
// tests below structurally cannot: both parked arms ready at once, with the
// select — not the precedence — free to pick either.
//
// Simultaneity comes from s.mu rather than from a hook, so the test depends
// only on the invariant it is asserting (the answer is derived under the lock)
// and not on where any test seam sits. A woken receiver cannot get past
// s.mu.Lock() until this block ends, so both terminations are guaranteed
// visible whenever it does derive its answer, however the scheduler ordered
// the wake.
//
// Repeated because the failure it guards against is probabilistic: with the
// arms deciding for themselves, Go's uniform select choice returns ctx.Err()
// about half the time — and leaves the receiver registered when it does. One
// iteration would be a coin flip; this many makes the old behavior a certain
// failure.
func TestParkedCloseAndCancelResolveToClosed(t *testing.T) {
	const iters = 200
	for i := 0; i < iters; i++ {
		h := New[int](WithDefaultMerge(latestWins))
		rx := h.Receiver()
		ctx, cancel := context.WithCancel(context.Background())
		errCh := parkedRecv(t, rx, ctx)

		// Sender.Close can't be used here: it takes the lock this test is
		// holding precisely to fuse the two events. Setting the flag and
		// signalling under mu is exactly the state it would leave behind.
		// Order matters: cancel first, so <-ctxDone is armed before anything
		// can wake the receiver. Signalling first lets it leave the select on
		// <-notify while ctxDone is still un-armed, and the arm that has to
		// lose for this test to mean anything is never taken — a green test
		// covering nothing, which arming in this order is what prevents.
		s := rx.s
		s.mu.Lock()
		cancel()          // arms <-ctxDone: the arm that must not decide
		s.txClosed = true // ...against a termination it cannot see yet
		rx.signalLocked() // arms <-notify too, so either may win the wake
		s.mu.Unlock()

		require.ErrorIs(t, <-errCh, gobus.ErrClosed)
		assert.Equal(t, 0, h.forTestingReceiverCount(), "terminal exit skipped deregistration")
	}
}

// TestParkedCancelWakeStillLosesToClose pins that closed > cancelled holds on
// the parked path too, not just at entry. The cancellation is what wakes the
// parked receiver — it is the only ready arm, so no select roulette is
// involved — but a close becomes visible before the woken call re-derives its
// answer, and ErrClosed must win anyway, carrying the terminal deregistration
// with it. Both ranks of "closed" are covered: the hard receiver-close and the
// soft sender-close-with-nothing-to-drain.
//
// This is what forces the parking select's arms to fall through to the loop
// top instead of returning their own verdict. An arm that returned ctx.Err()
// directly would report cancellation here, and worse, would make the real race
// — a close and a cancellation both landing on a parked receiver, so both arms
// ready — resolve by coin flip, leaving the receiver registered half the time.
// TestParkedCloseAndCancelResolveToClosed covers that both-ready case.
func TestParkedCancelWakeStillLosesToClose(t *testing.T) {
	tests := []struct {
		name  string
		close func(h *Hub[int, int], rx *Receiver[int, int])
	}{
		{"sender close", func(h *Hub[int, int], _ *Receiver[int, int]) { h.Sender().Close() }},
		{"receiver close", func(_ *Hub[int, int], rx *Receiver[int, int]) { rx.Close() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New[int](WithDefaultMerge(latestWins))
			rx := h.Receiver()
			ctx, cancel := context.WithCancel(context.Background())
			errCh := parkedRecv(t, rx, ctx)

			// Runs on the woken iteration, after the wake and before s.mu — so
			// the close is visible exactly when the precedence is evaluated.
			// Assigning before cancel() gives the write a happens-before edge
			// to the wake.
			rx.forTestingBeforeRecvLock = func() { tt.close(h, rx) }
			cancel()

			require.ErrorIs(t, <-errCh, gobus.ErrClosed)
			// Either close deregisters: the terminal exit does it for the
			// sender path, Close itself for the receiver path.
			assert.Equal(t, 0, h.forTestingReceiverCount(), "close left the receiver registered")
		})
	}
}

// TestRecvContextCancelDoesNotCloseReceiver pins the other half of that
// contract: ctx.Err() is not an end-of-stream. A cancelled receive leaves the
// receiver live and registered — it never drains to ErrClosed on its own — so
// a caller that stops on ctx.Err() must Close the handle or leak it into the
// hub for the hub's lifetime. Also pins the waiters accounting across the
// cancelled exit: a parked reader that leaves via ctx must not leave the count
// raised, or later Sends would signal a receiver nobody is waiting on.
func TestRecvContextCancelDoesNotCloseReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := parkedRecv(t, rx, ctx)
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	rx.s.mu.Lock()
	waiters := rx.waiters
	rx.s.mu.Unlock()
	assert.Equal(t, 0, waiters, "cancelled exit left a phantom waiter")
	assert.Equal(t, 1, h.forTestingReceiverCount(), "cancellation deregistered a live receiver")

	// Still fully usable on a fresh context.
	require.NoError(t, h.Sender().Send(1, 1))
	assertRecv(t, rx, 1, 1)
}

// TestRecvContextDrainedSenderCloseBeatsCancel covers the arm the two tests
// above miss: the receiver is live — so the hard-termination check falls
// through — and the ctx is cancelled, but the sender has closed with the queue
// drained. That is as durably terminal as a closed handle, so it must report
// ErrClosed rather than ctx.Err(): a shutdown loop that cancels its own
// context still has to be able to drain to ErrClosed instead of spinning on
// ctx.Err() forever.
func TestRecvContextDrainedSenderCloseBeatsCancel(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	h.Sender().Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rx.RecvContext(ctx)
	require.ErrorIs(t, err, gobus.ErrClosed)
	assert.Equal(t, 0, h.forTestingReceiverCount()) // terminal exit deregisters
}

// TestRecvContextCancelBeatsUndrainedSenderClose is the boundary case: same
// closed sender, but with an event still queued the receive is not terminal
// yet, so the cancelled ctx wins and the event survives for a later drain.
func TestRecvContextCancelBeatsUndrainedSenderClose(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 1))
	tx.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rx.RecvContext(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// The cancelled exit is not terminal, so it does not deregister — even
	// here, where the closed sender means nothing can ever be enqueued again.
	// This is the one case the precedence change moved: before it, a cancelled
	// consumer draining a closed sender reached ErrClosed and deregistered
	// itself. Now the handle is retained (bounded: one registry entry plus the
	// events already queued) until it is drained on a live context or Closed.
	// Deregistering here instead would contradict the receiver staying usable
	// on a fresh context — see TestRecvContextCancelDoesNotCloseReceiver.
	assert.Equal(t, 1, h.forTestingReceiverCount())

	assertRecv(t, rx, 1, 1)
	_, err = rx.Recv()
	require.ErrorIs(t, err, gobus.ErrClosed)
	assert.Equal(t, 0, h.forTestingReceiverCount(), "drain to ErrClosed deregisters")
}

func TestCoalesceLatestWins(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 100))
	require.NoError(t, tx.Send(1, 200))
	require.NoError(t, tx.Send(1, 300))
	// Three sends to one key before any read collapse to one latest value.
	assertRecv(t, rx, 1, 300)
	assertEmpty(t, rx)
}

func TestAnnihilation(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 100)) // enqueue key 1
	require.NoError(t, tx.Send(1, -1))  // annihilate key 1 (negative)
	require.NoError(t, tx.Send(2, 50))  // key 2 survives
	// Key 1 was dropped entirely; only key 2 is delivered.
	assertRecv(t, rx, 2, 50)
	assertEmpty(t, rx)
	assert.Equal(t, 0, rx.lenForTest())
}

func TestWithKeyFilterFiltersAtEnqueue(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver(h.WithKeyFilter(func(k int) bool { return k == 7 }))
	tx := h.Sender()
	// Unwanted keys are dropped at Send, so they never occupy a buffer slot.
	require.NoError(t, tx.Send(1, 100))
	require.NoError(t, tx.Send(2, 200))
	assert.Equal(t, 0, rx.lenForTest())
	// The wanted key is buffered and delivered as usual.
	require.NoError(t, tx.Send(7, 42))
	require.NoError(t, tx.Send(3, 300))
	assert.Equal(t, 1, rx.lenForTest())
	assertRecv(t, rx, 7, 42)
	assertEmpty(t, rx)
}

func TestWithKeyFilterRequiresKeep(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	assert.Panics(t, func() { h.WithKeyFilter(nil) })
}

func TestWithMergeIsPerReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	shared := h.Receiver()
	// This receiver annihilates any coalesced pair (keep=false) instead of
	// taking the latest; the shared receiver is unaffected by the override.
	annihilate := h.Receiver(h.WithMerge(func(_, _ int) (int, bool) { return 0, false }))
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 100))
	require.NoError(t, tx.Send(1, 200)) // coalesce on both receivers
	// Shared receiver keeps the latest via the hub's merge.
	assertRecv(t, shared, 1, 200)
	// Override receiver dropped key 1 entirely.
	assert.Equal(t, 0, annihilate.lenForTest())
	assertEmpty(t, annihilate)
}

func TestWithMergeRequiresMerge(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	assert.Panics(t, func() { h.WithMerge(nil) })
}

// TestOptionsCompose covers the combination a constructor-per-variant API
// could not express at all: filter *and* a private merge on one receiver.
func TestOptionsCompose(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	// Keep only even keys, and annihilate on any coalesce rather than taking
	// the latest.
	rx := h.Receiver(
		h.WithKeyFilter(func(k int) bool { return k%2 == 0 }),
		h.WithMerge(func(_, _ int) (int, bool) { return 0, false }),
	)
	plain := h.Receiver()
	tx := h.Sender()

	require.NoError(t, tx.Send(1, 10)) // odd: filtered out of rx entirely
	require.NoError(t, tx.Send(2, 20)) // even: buffered by rx
	assert.Equal(t, 1, rx.lenForTest())
	require.NoError(t, tx.Send(2, 30)) // coalesce on rx -> annihilated
	assert.Equal(t, 0, rx.lenForTest())
	assertEmpty(t, rx)

	// The unmodified receiver saw all of it under the hub's own policy.
	assertRecv(t, plain, 1, 10)
	assertRecv(t, plain, 2, 30)
}

func TestReceiverRejectsNilOption(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	// A nil option is a caller bug; fail loudly rather than nil-dereferencing
	// inside the hub, matching the nil-policy panics elsewhere in the package.
	assert.Panics(t, func() { h.Receiver(nil) })
}

// TestOptionsLastWins pins the documented precedence for repeated options.
func TestOptionsLastWins(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver(
		h.WithKeyFilter(func(k int) bool { return k == 1 }),
		h.WithKeyFilter(func(k int) bool { return k == 2 }),
	)
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.Send(2, 20))
	assertRecv(t, rx, 2, 20)
	assertEmpty(t, rx)
}

func TestStableOrder(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.Send(2, 20))
	require.NoError(t, tx.Send(3, 30))
	assertRecv(t, rx, 1, 10)
	assertRecv(t, rx, 2, 20)
	assertRecv(t, rx, 3, 30)
}

func TestRetouchKeepsPosition(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.Send(2, 20))
	require.NoError(t, tx.Send(1, 99)) // re-touch key 1: keeps its (first) slot
	// Order stays 1,2 — key 1 is delivered first with its latest body.
	assertRecv(t, rx, 1, 99)
	assertRecv(t, rx, 2, 20)
}

func TestFanoutIsolation(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rxA := h.Receiver()
	rxB := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 7))
	// Each receiver gets its own copy.
	assertRecv(t, rxA, 1, 7)
	assertRecv(t, rxB, 1, 7)
	// rxB draining does not affect rxA's already-consumed stream.
	assertEmpty(t, rxA)
}

func TestLateReceiverSeesNoHistory(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 10))
	// A receiver created after the send starts empty — conflate keeps the
	// latest value per key per *receiver*, not a hub-wide replayable slot.
	rx := h.Receiver()
	assertEmpty(t, rx)
	require.NoError(t, tx.Send(1, 20))
	assertRecv(t, rx, 1, 20)
}

func TestBoundedUnderSlowConsumer(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	// 1000 writes across 4 keys, never read: pending stays bounded by the key
	// set, not the write count.
	for i := 0; i < 1000; i++ {
		require.NoError(t, tx.Send(i%4, i))
	}
	assert.Equal(t, 4, rx.lenForTest())
	// A re-touched-then-annihilated key leaves no residue.
	require.NoError(t, tx.Send(0, -1))
	assert.Equal(t, 3, rx.lenForTest())
}

func TestTrySendAndSendContext(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.TrySend(1, 10))
	require.NoError(t, tx.SendContext(context.Background(), 2, 20))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, tx.SendContext(ctx, 3, 30), context.Canceled)

	assertRecv(t, rx, 1, 10)
	assertRecv(t, rx, 2, 20)
	assertEmpty(t, rx) // the cancelled SendContext never enqueued
}

// lockProbeContext is an already-cancelled context that records, each time the
// bus asks it a question, whether the bus lock was held at that moment. A
// caller-supplied context is arbitrary code whose methods may take application
// locks, so calling one under s.mu inverts the lock order against any
// goroutine that enters the bus while holding those locks — a deadlock the
// bus's own tests would never hit, because the standard contexts don't do it.
//
// TryLock is the observation rather than a real deadlock: it answers "is s.mu
// held right now" on this goroutine, with no scheduling assumption and nothing
// to time out.
type lockProbeContext struct {
	context.Context
	mu           *sync.Mutex
	lockedInDone bool
	lockedInErr  bool
}

func (c *lockProbeContext) Done() <-chan struct{} {
	c.lockedInDone = c.lockedInDone || c.busLocked()
	return c.Context.Done()
}

func (c *lockProbeContext) Err() error {
	c.lockedInErr = c.lockedInErr || c.busLocked()
	return c.Context.Err()
}

func (c *lockProbeContext) busLocked() bool {
	if c.mu.TryLock() {
		c.mu.Unlock()
		return false
	}
	return true
}

func TestContextIsNeverConsultedUnderTheBusLock(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	tx := h.Sender()
	rx := h.Receiver()
	inner, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("SendContext", func(t *testing.T) {
		ctx := &lockProbeContext{Context: inner, mu: &h.s.mu}
		assert.ErrorIs(t, tx.SendContext(ctx, 1, 10), context.Canceled)
		assert.False(t, ctx.lockedInDone, "Done() called under s.mu")
		assert.False(t, ctx.lockedInErr, "Err() called under s.mu")
	})

	t.Run("RecvContext", func(t *testing.T) {
		require.NoError(t, tx.Send(1, 10)) // a pending value the cancellation must outrank
		ctx := &lockProbeContext{Context: inner, mu: &h.s.mu}
		_, err := rx.RecvContext(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		assert.False(t, ctx.lockedInDone, "Done() called under s.mu")
		assert.False(t, ctx.lockedInErr, "Err() called under s.mu")
	})
}

// TestPanickingCallbackReleasesTheBusLock pins that a panic out of the
// caller's Merge — which runs under s.mu — leaves the hub usable rather than
// wedged. Every send path must release the lock on the way out, so this is
// table-driven over all three: an unlock that is deferred on one path and
// explicit on another is exactly the asymmetry that regresses.
func TestPanickingCallbackReleasesTheBusLock(t *testing.T) {
	boom := errors.New("merge exploded")
	sends := map[string]func(tx *Sender[int, int], k, v int) error{
		"Send":        func(tx *Sender[int, int], k, v int) error { return tx.Send(k, v) },
		"TrySend":     func(tx *Sender[int, int], k, v int) error { return tx.TrySend(k, v) },
		"SendContext": func(tx *Sender[int, int], k, v int) error { return tx.SendContext(context.Background(), k, v) },
	}
	for name, send := range sends {
		t.Run(name, func(t *testing.T) {
			explode := true
			h := New[int](WithDefaultMerge(func(prev, next int) (int, bool) {
				if explode {
					panic(boom)
				}
				return next, true
			}))
			tx := h.Sender()
			rx := h.Receiver()
			require.NoError(t, send(tx, 1, 10)) // first touch: no Merge, no panic

			func() {
				defer func() { assert.Equal(t, boom, recover()) }()
				_ = send(tx, 1, 20) // coalesce: Merge panics under s.mu
				t.Fatal("send did not panic")
			}()

			// s.mu is free, so the hub still works. TryLock proves it directly;
			// a wedged mutex would otherwise deadlock the assertions below.
			require.True(t, h.s.mu.TryLock(), "s.mu still held after the panic")
			h.s.mu.Unlock()
			explode = false
			require.NoError(t, send(tx, 1, 30))
			assertRecv(t, rx, 1, 30)
		})
	}
}

// TestSendContextChecksCancellationAtTheLockNotAtEntry pins that SendContext
// resolves ctx where the send is decided — under s.mu — rather than from a
// snapshot taken on entry. The hook fires after ctx's Done channel has been
// taken and before the lock is acquired, which is exactly the window a
// contended send sits in while another send's Merge runs, so a cancellation
// landing there must still be honoured.
//
// An entry-time snapshot would have been taken before the hook ran and would
// publish the value regardless; that is the mutation this test exists to
// catch. Precedence is unaffected — a closed sender still outranks the
// cancellation, which the second half asserts.
func TestSendContextChecksCancellationAtTheLockNotAtEntry(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.s.forTestingBeforeSendLock = func() { cancel() }
	require.NoError(t, ctx.Err(), "ctx must be live when SendContext is entered")

	assert.ErrorIs(t, tx.SendContext(ctx, 1, 10), context.Canceled)
	assertEmpty(t, rx) // the value was not published on behalf of a dead ctx

	// closed > cancelled still holds when the cancellation lands in that same
	// window, rather than having arrived before the call. A second, still-live
	// context is what makes that the case under test: reusing the one above
	// would enter already cancelled, which is the plain entry-time precedence
	// the conformance suite pins (closed_beats_cancelled).
	//
	// This is a matter of the assertion meaning what it says, not of catching a
	// mutation the entry-time form misses — there is no such mutation. A ctx
	// cancelled on entry is also cancelled in the window and under the lock, so
	// every cancellation arm that fires here fires there too; the entry-time
	// form is if anything the stronger probe of the precedence itself. What it
	// is not is a probe of *this* window, which is what the function is about.
	closedCtx, cancelClosed := context.WithCancel(context.Background())
	defer cancelClosed()
	h.s.forTestingBeforeSendLock = func() { cancelClosed() }
	tx.Close()
	require.NoError(t, closedCtx.Err(), "ctx must be live when SendContext is entered")

	assert.ErrorIs(t, tx.SendContext(closedCtx, 2, 20), gobus.ErrClosed)
}

func TestTryRecvOnClosedReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	rx.Close()
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestTryRecvFlushTerminatesOnAnyError(t *testing.T) {
	for _, closeSender := range []bool{false, true} {
		name := "sender open"
		if closeSender {
			name = "sender closed"
		}
		t.Run(name, func(t *testing.T) {
			h := New[int](WithDefaultMerge(latestWins))
			rx := h.Receiver()
			tx := h.Sender()
			require.NoError(t, tx.Send(1, 11))
			require.NoError(t, tx.Send(2, 22))
			if closeSender {
				tx.Close()
			}
			// Bounded so a non-terminating loop fails instead of hanging.
			var err error
			for i := 0; i < 100; i++ {
				if _, err = rx.TryRecv(); err != nil {
					break
				}
			}
			require.Error(t, err, "flush loop never reached a terminal error")
			if closeSender {
				assert.ErrorIs(t, err, gobus.ErrClosed,
					"a closed sender ends the flush with ErrClosed, never ErrEmpty")
				// ErrClosed is terminal, so this flush deregisters on its own.
				assert.Equal(t, 0, h.forTestingReceiverCount())
			} else {
				assert.ErrorIs(t, err, gobus.ErrEmpty)
				// ErrEmpty is not terminal: the flush drained the queue but the
				// handle is still registered and will keep accumulating from
				// the live sender. Draining is not a substitute for Close —
				// pinned here because the docs previously implied it was.
				assert.Equal(t, 1, h.forTestingReceiverCount())
				require.NoError(t, tx.Send(3, 33))
				assert.Equal(t, 1, rx.lenForTest())

				rx.Close() // the step the caller actually has to take
				assert.Equal(t, 0, h.forTestingReceiverCount())
			}
		})
	}
}

func TestTryRecvCloseRaceBeforeLock(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 1))
	// Close wins the race between the lock-free done pre-check and taking mu;
	// the under-lock re-check must still return ErrClosed, not a value.
	rx.forTestingBeforeTryRecvLock = func() { rx.Close() }
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestPeekDoesNotConsume(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 11))

	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[int, int]{Key: 1, Value: 11}, ev)
	assert.Equal(t, 1, rx.lenForTest(), "Peek consumed the event")

	// The same event is still there for the consuming path, twice over: a
	// second Peek, then the TryRecv that actually takes it.
	again, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, ev, again)
	got, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, ev, got, "TryRecv did not return the peeked event")
	assert.Equal(t, 0, rx.lenForTest())
}

func TestPeekReportsTheHeadNotTheNewest(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	require.NoError(t, tx.Send(2, 22))

	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[int, int]{Key: 1, Value: 11}, ev, "Peek should report first-touch order")

	assertRecv(t, rx, 1, 11)
	ev, err = rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[int, int]{Key: 2, Value: 22}, ev, "the head should advance with the pop")
}

func TestPeekOnEmptyReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	ev, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
	assert.Zero(t, ev, "the error return should carry a zero Event")
}

// hardCloses are the two closes that abandon pending values, as opposed to
// Sender.Close's soft drain. Shared by the tests that assert what Peek reports
// once one of them has landed on a receiver with a value still queued.
var hardCloses = []struct {
	name  string
	close func(h *Hub[int, int], rx *Receiver[int, int])
}{
	{"receiver", func(_ *Hub[int, int], rx *Receiver[int, int]) { rx.Close() }},
	{"hub", func(h *Hub[int, int], _ *Receiver[int, int]) { h.Close() }},
}

// TestPeekPrecedenceMatchesTryRecv pins that Peek is not a raw-state read: a
// closed handle reports ErrClosed even with a value sitting at the head, which
// is what keeps it from becoming a back door around the close precedence.
func TestPeekPrecedenceMatchesTryRecv(t *testing.T) {
	for _, tt := range hardCloses {
		t.Run(tt.name, func(t *testing.T) {
			h := New[int](WithDefaultMerge(latestWins))
			rx := h.Receiver()
			require.NoError(t, h.Sender().Send(1, 11))
			tt.close(h, rx)
			ev, err := rx.Peek()
			assert.ErrorIs(t, err, gobus.ErrClosed, "a hard close outranks the queued value")
			assert.Zero(t, ev)
		})
	}
}

func TestPeekDrainsThenReportsClosed(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	tx.Close()

	// Sender.Close is the soft path, so the pending value is still peekable
	// and still receivable.
	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[int, int]{Key: 1, Value: 11}, ev)
	assertRecv(t, rx, 1, 11)

	// Drained and closed is terminal however it is observed, so the verdict
	// carries the same deregistration TryRecv's does.
	assert.Equal(t, 1, h.forTestingReceiverCount())
	_, err = rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.Equal(t, 0, h.forTestingReceiverCount(), "the terminal Peek skipped deregistration")
}

// TestPeekReflectsCoalescingWithoutMovingTheHead pins the half of the head-key
// contract that holds: coalescing changes the head's value and leaves its
// identity alone. A consumer folding an ordering quantity into V via Merge
// reads it off this head, so the key must not jump on a re-send.
func TestPeekReflectsCoalescingWithoutMovingTheHead(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	require.NoError(t, tx.Send(2, 22))
	require.NoError(t, tx.Send(1, 99)) // coalesces into key 1's existing slot

	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[int, int]{Key: 1, Value: 99}, ev,
		"the head key should be unchanged and carry the merged value")
	assert.Equal(t, 2, rx.lenForTest(), "a coalesce should not grow the queue")
}

// TestPeekSeesAnnihilation pins the other half: annihilation *does* move the
// head. The watermark that reads this head stays sound anyway, because the
// replacement head was first touched later — its ordering quantity is higher,
// so the cursor can only move conservatively.
func TestPeekSeesAnnihilation(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	require.NoError(t, tx.Send(2, 22))
	require.NoError(t, tx.Send(1, -1)) // negative annihilates key 1

	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[int, int]{Key: 2, Value: 22}, ev,
		"an annihilated head should be gone, not reported empty or stale")
	assert.Equal(t, 1, rx.lenForTest())
}

func TestPeekRespectsKeyFilter(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver(h.WithKeyFilter(func(k int) bool { return k == 7 }))
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11)) // filtered out: never buffered, so never the head
	require.NoError(t, tx.Send(7, 77))

	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[int, int]{Key: 7, Value: 77}, ev)
}

// TestPeekOnHardCloseWithQueuedKey is the observation a cursor-tracking
// consumer has to respect: ErrClosed from Peek does *not* mean the backlog was
// empty. Hub.Close and Receiver.Close abandon whatever is queued, so a consumer
// that reads ErrClosed as "nothing pending" and commits its watermark to the
// high end of the last batch silently skips these undelivered writes. Only
// Sender.Close, the soft drain, empties the queue first.
func TestPeekOnHardCloseWithQueuedKey(t *testing.T) {
	for _, tt := range hardCloses {
		t.Run(tt.name, func(t *testing.T) {
			h := New[int](WithDefaultMerge(latestWins))
			rx := h.Receiver()
			require.NoError(t, h.Sender().Send(1, 11))
			tt.close(h, rx)

			_, err := rx.Peek()
			require.ErrorIs(t, err, gobus.ErrClosed)
			assert.Equal(t, 1, rx.lenForTest(),
				"ErrClosed here coexists with a queued key, so it cannot be read as 'backlog empty'")
		})
	}
}

func TestPeekCloseRaceBeforeLock(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 1))
	// Close wins the race between the lock-free done pre-check and taking mu;
	// the under-lock re-check must still return ErrClosed, not the head value.
	rx.forTestingBeforePeekLock = func() { rx.Close() }
	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

// TestPeekReportsEmptyWhileFeederHoldsEvent pins the documented-but-surprising
// interaction with Chan: the feeder pops under s.mu and parks on delivery
// outside it, so a Chan consumer can see an empty backlog while exactly one
// event is in flight. Sequenced through the feeder's parked hook — the feeder
// has popped and not yet delivered when it runs — rather than through the
// channel, which would race the delivery.
func TestPeekReportsEmptyWhileFeederHoldsEvent(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	defer rx.Close()
	require.NoError(t, h.Sender().Send(1, 11))

	popped := make(chan struct{})
	release := make(chan struct{})
	rx.forTestingFeederParked = func() {
		close(popped)
		<-release
	}
	ch := rx.Chan()

	<-popped
	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty,
		"the in-flight event has left the queue, so Peek cannot see it")

	close(release)
	assert.Equal(t, gobus.Event[int, int]{Key: 1, Value: 11}, <-ch, "the event is delivered, not lost")
}

// peekSink is a package-level sink for the allocation test. Assigning the
// returned Event to a local would let the escape analysis of the test body,
// rather than Peek itself, decide the result.
var peekSink gobus.Event[int, int]

func TestPeekAllocatesNothing(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	for k := 0; k < 64; k++ {
		require.NoError(t, tx.Send(k, k))
	}
	// A list-head read plus one map lookup: nothing to allocate, and nothing
	// that grows with the pending key count.
	avg := testing.AllocsPerRun(100, func() { peekSink, _ = rx.Peek() })
	assert.Zero(t, avg, "Peek should allocate nothing")
	assert.Equal(t, gobus.Event[int, int]{Key: 0, Value: 0}, peekSink)
}

// TestTryRecvAllTakesTheWholeQueueInFirstTouchOrder pins the shape of a cut.
// The slice is asserted whole rather than by membership: order is part of the
// contract, and a duplicated key — the thing one entry per key rules out —
// would pass any membership check.
func TestTryRecvAllTakesTheWholeQueueInFirstTouchOrder(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	require.NoError(t, tx.Send(2, 22))
	require.NoError(t, tx.Send(1, 99)) // coalesces in place; key 1 keeps its position
	require.NoError(t, tx.Send(3, 33))

	evs, err := rx.TryRecvAll()
	require.NoError(t, err)
	assert.Equal(t, []gobus.Event[int, int]{
		{Key: 1, Value: 99}, {Key: 2, Value: 22}, {Key: 3, Value: 33},
	}, evs, "first-touch order, with the coalesced key merged at its original position")
}

func TestTryRecvAllOnEmptyReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()

	evs, err := rx.TryRecvAll()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
	assert.Empty(t, evs, "an error result carries no values")
}

// TestTryRecvAllEmptiesEveryStructure proves the cut cleared elems and pending,
// not just order. A stale elems entry would send the next Send for that key
// down the coalesce branch, which writes a slot without pushing the key back
// onto the queue — so the key would vanish rather than reappear at the tail.
func TestTryRecvAllEmptiesEveryStructure(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	require.NoError(t, tx.Send(2, 22))

	_, err := rx.TryRecvAll()
	require.NoError(t, err)
	assert.Zero(t, rx.lenForTest())
	assertEmpty(t, rx)

	// Re-send both drained keys in the opposite order: each is a first touch
	// again, so the new cut is ordered by the new touches, not the old ones.
	require.NoError(t, tx.Send(2, 222))
	require.NoError(t, tx.Send(1, 111))
	evs, err := rx.TryRecvAll()
	require.NoError(t, err)
	assert.Equal(t, []gobus.Event[int, int]{{Key: 2, Value: 222}, {Key: 1, Value: 111}}, evs)
}

// TestTryRecvAllPrecedenceMatchesTryRecv pins that a cut is not a raw-state
// read: a closed handle reports ErrClosed even with a full queue, which is what
// keeps TryRecvAll from becoming a back door around the close precedence.
func TestTryRecvAllPrecedenceMatchesTryRecv(t *testing.T) {
	for _, tt := range hardCloses {
		t.Run(tt.name, func(t *testing.T) {
			h := New[int](WithDefaultMerge(latestWins))
			rx := h.Receiver()
			require.NoError(t, h.Sender().Send(1, 11))
			tt.close(h, rx)

			evs, err := rx.TryRecvAll()
			assert.ErrorIs(t, err, gobus.ErrClosed, "a hard close outranks the queued values")
			assert.Empty(t, evs, "an error result carries no values")
		})
	}
}

// TestTryRecvAllOnHardCloseAbandonsTheBacklog is the corollary a cursor-tracking
// consumer has to respect: ErrClosed does not mean the backlog was empty.
// Hub.Close and Receiver.Close abandon what is queued, so a consumer that reads
// ErrClosed as "nothing pending" and commits the high end of its last batch
// silently skips these writes. Only Sender.Close drains first.
func TestTryRecvAllOnHardCloseAbandonsTheBacklog(t *testing.T) {
	for _, tt := range hardCloses {
		t.Run(tt.name, func(t *testing.T) {
			h := New[int](WithDefaultMerge(latestWins))
			rx := h.Receiver()
			require.NoError(t, h.Sender().Send(1, 11))
			require.NoError(t, h.Sender().Send(2, 22))
			tt.close(h, rx)

			_, err := rx.TryRecvAll()
			require.ErrorIs(t, err, gobus.ErrClosed)
			assert.Equal(t, 2, rx.lenForTest(),
				"ErrClosed here coexists with a queued backlog, so it cannot be read as 'backlog empty'")
		})
	}
}

// TestTryRecvAllDrainsThenReportsClosed pins the no-partial-results rule across
// the soft close: the whole queue comes back with a nil error, and only the
// *next* call is terminal. There is never "some values and ErrClosed".
func TestTryRecvAllDrainsThenReportsClosed(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	require.NoError(t, tx.Send(2, 22))
	tx.Close()

	evs, err := rx.TryRecvAll()
	require.NoError(t, err, "Sender.Close is the soft path: the queue drains first")
	assert.Equal(t, []gobus.Event[int, int]{{Key: 1, Value: 11}, {Key: 2, Value: 22}}, evs)

	// Drained and closed is terminal however it is observed, so the verdict
	// carries the same deregistration TryRecv's and Peek's do.
	assert.Equal(t, 1, h.forTestingReceiverCount())
	evs, err = rx.TryRecvAll()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.Empty(t, evs)
	assert.Equal(t, 0, h.forTestingReceiverCount(), "the terminal TryRecvAll skipped deregistration")
}

func TestTryRecvAllOnClosedReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	rx.Close()

	_, err := rx.TryRecvAll()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestTryRecvAllCloseRaceBeforeLock(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 11))
	// Close wins the race between the lock-free done pre-check and taking mu;
	// the under-lock re-check must still return ErrClosed, not the queue.
	rx.forTestingBeforeTryRecvAllLock = func() { rx.Close() }

	evs, err := rx.TryRecvAll()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.Empty(t, evs)
}

// TestTryRecvAllSeesAnnihilation pins that a cut reflects the Merge policy
// rather than a raw history: an annihilated key is absent entirely, not present
// carrying a tombstone value.
func TestTryRecvAllSeesAnnihilation(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	require.NoError(t, tx.Send(2, 22))
	require.NoError(t, tx.Send(1, -1)) // negative annihilates key 1

	evs, err := rx.TryRecvAll()
	require.NoError(t, err)
	assert.Equal(t, []gobus.Event[int, int]{{Key: 2, Value: 22}}, evs)
}

func TestTryRecvAllRespectsKeyFilter(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver(h.WithKeyFilter(func(k int) bool { return k == 7 }))
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11)) // filtered out: never buffered, so never in a cut
	require.NoError(t, tx.Send(7, 77))

	evs, err := rx.TryRecvAll()
	require.NoError(t, err)
	assert.Equal(t, []gobus.Event[int, int]{{Key: 7, Value: 77}}, evs)
}

// TestTryRecvAllExcludesTheEventInFlightToTheFeeder pins the Chan interaction
// the doc comment claims, and it is the one place a cut is not "everything the
// receiver holds": the feeder pops under s.mu and parks on delivery outside it,
// so one event can be in flight and invisible to the cut. Sequenced through the
// feeder's parked hook — it has popped and not yet delivered when it runs —
// rather than through the channel, which would race the delivery.
func TestTryRecvAllExcludesTheEventInFlightToTheFeeder(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	defer rx.Close()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))

	popped := make(chan struct{})
	release := make(chan struct{})
	rx.forTestingFeederParked = func() {
		close(popped)
		<-release
	}
	ch := rx.Chan()

	<-popped
	require.NoError(t, tx.Send(2, 22)) // arrives after the feeder took key 1
	evs, err := rx.TryRecvAll()
	require.NoError(t, err)
	assert.Equal(t, []gobus.Event[int, int]{{Key: 2, Value: 22}}, evs,
		"the in-flight event has left the queue, so a cut cannot see it")

	close(release)
	assert.Equal(t, gobus.Event[int, int]{Key: 1, Value: 11}, <-ch, "the event is delivered, not lost")
}

// tryRecvAllSink is a package-level sink for the allocation test, for the same
// escape-analysis reason peekSink is.
var tryRecvAllSink []gobus.Event[int, int]

// TestTryRecvAllOnEmptyAllocatesNothing pins the empty cut specifically. A
// non-empty one necessarily allocates its result slice — that is the method's
// whole job, and it is why the size of the critical section is documented — but
// a caller polling an idle receiver should pay nothing.
//
// It pins the outcome, not the code shape: a zero-capacity make is itself free,
// so hoisting the allocation above the empty check survives this test. What it
// does catch is an allocation whose size does not follow the pending count.
func TestTryRecvAllOnEmptyAllocatesNothing(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()

	avg := testing.AllocsPerRun(100, func() { tryRecvAllSink, _ = rx.TryRecvAll() })
	assert.Zero(t, avg, "an empty cut should allocate nothing")
	assert.Empty(t, tryRecvAllSink)
}

func TestReceiverClose(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	rx.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	rx.Close() // idempotent
}

func TestHubClose(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 1))
	h.Close()
	// Hard tear-down: ErrClosed without draining the pending value.
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	// Sends after hub close are rejected; new receivers are pre-closed.
	assert.ErrorIs(t, h.Sender().Send(2, 2), gobus.ErrClosed)
	_, err = h.Receiver().Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	h.Close() // idempotent
	// Sender.Close after Hub.Close is a no-op, not a re-signal.
	h.Sender().Close()
}

func TestSenderCloseDrainsThenErrClosed(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	require.NoError(t, tx.Send(2, 22))
	tx.Close()
	// Soft close: pending values drain first, then ErrClosed.
	assertRecv(t, rx, 1, 11)
	// TryRecv drains too, and reports the terminal state once empty.
	ev, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[int, int]{Key: 2, Value: 22}, ev)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.Equal(t, 0, h.forTestingReceiverCount(), "drained receiver should be deregistered")
	_, err = rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.ErrorIs(t, tx.Send(3, 33), gobus.ErrClosed)
	// Second Close is a harmless no-op (idempotent guard), not a re-signal/panic.
	tx.Close()
	assert.ErrorIs(t, tx.Send(4, 44), gobus.ErrClosed)
}

// TestSenderCloseIsSafeConcurrentWithSend pins the guarantee Sender.Close's
// doc makes: a send racing the close resolves to one of exactly two outcomes,
// published-and-nil or ErrClosed-and-nothing-published, with no third state.
//
// The seam is what makes that deterministic rather than a timing harness. It
// runs after the send has declined the fast path and before it takes s.mu, so
// a Close fired from there lands exactly in the window a concurrent
// Sender.Close would occupy — the one moment a send is committed to
// publishing but has not yet reached the lock that decides whether it may. A
// -race loop would only sample that window by luck, and would pass vacuously
// if it never hit it.
//
// Both directions are asserted, because the guarantee is two-sided: the losing
// send publishes nothing (the receiver's queue is still empty), and a send that
// wins the ordering is still drained by the soft close.
func TestSenderCloseIsSafeConcurrentWithSend(t *testing.T) {
	t.Run("close lands first", func(t *testing.T) {
		h := New[int](WithDefaultMerge(latestWins))
		rx := h.Receiver()
		tx := h.Sender()

		h.s.forTestingBeforeSendLock = func() { tx.Close() }
		assert.ErrorIs(t, tx.Send(1, 10), gobus.ErrClosed)
		h.s.forTestingBeforeSendLock = nil

		// Terminal at once, which is only true if nothing was published: a
		// queued key would drain before the receiver reported ErrClosed.
		_, err := rx.TryRecv()
		assert.ErrorIs(t, err, gobus.ErrClosed)
	})

	t.Run("send lands first", func(t *testing.T) {
		h := New[int](WithDefaultMerge(latestWins))
		rx := h.Receiver()
		tx := h.Sender()

		require.NoError(t, tx.Send(1, 10))
		tx.Close()

		assertRecv(t, rx, 1, 10)
	})
}

// TestSenderCloseIsSafeConcurrentWithSendContext is the SendContext twin. The
// promise covers both send paths, and SendContext reaches the lock by its own
// route — the fast path it declines is the one that also consults ctx.
func TestSenderCloseIsSafeConcurrentWithSendContext(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()

	h.s.forTestingBeforeSendLock = func() { tx.Close() }
	assert.ErrorIs(t, tx.SendContext(context.Background(), 1, 10), gobus.ErrClosed)
	h.s.forTestingBeforeSendLock = nil

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

// TestSenderCloseOnIdleHubRacesToNilOrClosed pins the other half of "no third
// outcome": with no receiver a send answers from the lock-free load, so the
// close it races is either already poisoned or not yet. Both answers are the
// contract; neither publishes anything, since there is nobody listening.
func TestSenderCloseOnIdleHubRacesToNilOrClosed(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	tx := h.Sender()
	require.Zero(t, h.forTestingReceiverCount(), "the fast path is what is under test")

	assert.NoError(t, tx.Send(1, 10), "load precedes the poison")
	tx.Close()
	assert.ErrorIs(t, tx.Send(1, 20), gobus.ErrClosed, "load follows the poison")
}

func TestSenderCloseWakesParkedReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	errCh := parkedRecv(t, rx, context.Background())
	h.Sender().Close()
	assert.ErrorIs(t, <-errCh, gobus.ErrClosed)
}

func TestCloseRaceBeforeLock(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	// Close wins the race between the lock-free done pre-check and taking mu;
	// the under-lock re-check must still return ErrClosed, not a value.
	require.NoError(t, h.Sender().Send(1, 1))
	rx.forTestingBeforeRecvLock = func() { rx.Close() }
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

// TestCloseBeatsValueDeliveredWhileParked is the close-side twin of
// TestRecvContextCancelBeatsValueDeliveredWhileParked: receiver-close outranks
// an event that lands at the same moment, on the parked path. Because a wake
// here carries no value — the event stays in the receiver's slots until it is
// popped — the next loop iteration re-derives the answer from state and
// ErrClosed wins deterministically rather than by select roulette.
func TestCloseBeatsValueDeliveredWhileParked(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	errCh := parkedRecv(t, rx, context.Background())

	// Close from the pre-lock hook so the parked reader is woken by <-notify
	// with an event genuinely queued, and only then observes the close.
	// Closing before the Send would let <-rx.done win the wake outright and
	// the value path this test exists to cover would never run.
	rx.forTestingBeforeRecvLock = func() { rx.Close() }
	require.NoError(t, h.Sender().Send(7, 42))

	assert.ErrorIs(t, <-errCh, gobus.ErrClosed)
}

// TestRecvContextCancelRacingSendLosesNoEvent is the conservation property
// behind the whole precedence, exercised as a real race rather than through a
// hook: whatever order a Send and a cancel land in, a RecvContext either
// returns the event or returns ctx.Err() with the event still queued. It must
// never consume-and-discard, which is what would make a cancelled shutdown
// silently drop the last update for a key.
func TestRecvContextCancelRacingSendLosesNoEvent(t *testing.T) {
	for i := 0; i < 500; i++ {
		h := New[int](WithDefaultMerge(latestWins))
		rx := h.Receiver()
		tx := h.Sender()

		ctx, cancel := context.WithCancel(context.Background())
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		var got gobus.Event[int, int]
		var recvErr error
		go func() {
			defer wg.Done()
			<-start
			got, recvErr = rx.RecvContext(ctx)
		}()
		go func() {
			defer wg.Done()
			<-start
			cancel()
		}()
		require.NoError(t, tx.Send(1, 99))
		close(start)
		wg.Wait()

		if recvErr == nil {
			require.Equal(t, gobus.Event[int, int]{Key: 1, Value: 99}, got)
			assertEmpty(t, rx)
		} else {
			require.ErrorIs(t, recvErr, context.Canceled)
			assertRecv(t, rx, 1, 99) // left queued, not discarded
		}
		rx.Close()
	}
}

func TestChanDeliversInOrderAndClosesOnSenderClose(t *testing.T) {
	h := New[int, string]()
	rx := h.Receiver()
	defer rx.Close()

	// Step the feeder one iteration at a time. An unbuffered gate in the
	// pre-lock hook means a send on it both releases the feeder and proves it
	// had come back around to the top of its loop; without that, the feeder
	// can wake between the two key-2 sends below and deliver them separately.
	gate := make(chan struct{})
	rx.forTestingFeederBeforeLock = func() { <-gate }

	ch := rx.Chan()
	assert.Equal(t, (<-chan gobus.Event[int, string])(rx.ch), rx.Chan(), "Chan should be memoized")

	tx := h.Sender()
	require.NoError(t, tx.Send(1, "a"))
	gate <- struct{}{}
	assert.Equal(t, gobus.Event[int, string]{Key: 1, Value: "a"}, <-ch)

	// Coalescing still applies behind the feeder. Let it park on notify with
	// an empty queue; the first send wakes it, but it cannot pop until it has
	// come back around through the gate, so both sends land in key 2's slot
	// and only the merged value is delivered.
	gate <- struct{}{}
	waitParked(t, rx)
	require.NoError(t, tx.Send(2, "b"))
	require.NoError(t, tx.Send(2, "c"))
	gate <- struct{}{}
	assert.Equal(t, gobus.Event[int, string]{Key: 2, Value: "c"}, <-ch)

	tx.Close()
	gate <- struct{}{}
	_, ok := <-ch
	assert.False(t, ok, "feeder should close the channel after draining")
	assert.Equal(t, 0, h.forTestingReceiverCount(), "drained feeder should deregister")
}

func TestChanClosesOnReceiverClose(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	ch := rx.Chan()
	waitParked(t, rx) // feeder is parked on notify
	rx.Close()
	_, ok := <-ch
	assert.False(t, ok)
}

func TestChanOnPreClosedReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	rx.Close()
	// The feeder's first check sees the closed receiver and exits immediately.
	_, ok := <-rx.Chan()
	assert.False(t, ok)
}

func TestChanFeederCloseRaceBeforeLock(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 1))
	// Close wins the race between the feeder's lock-free pre-check and the
	// lock; the under-lock re-check must abandon the pending event.
	rx.forTestingFeederBeforeLock = func() { rx.Close() }
	_, ok := <-rx.Chan()
	assert.False(t, ok)
}

func TestChanFeederCloseWhileDelivering(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 1))
	// Close lands while the feeder is parked on the (unbuffered, unread) send;
	// the in-flight event is abandoned and the channel closes. We must not
	// touch the channel until the feeder has committed to the done arm — a
	// reader waiting on ch would make both select arms ready and the choice
	// random — so wait for the feeder's exit hook first.
	exited := make(chan struct{})
	rx.forTestingFeederParked = func() { rx.Close() }
	rx.forTestingFeederExit = func() { close(exited) }
	ch := rx.Chan()
	<-exited
	_, ok := <-ch
	assert.False(t, ok)
}

// TestConcurrentSendersAndReceiver is a -race smoke test: many goroutines send
// across a small key space while one consumer drains, asserting nothing is
// lost to a data race and nothing deadlocks.
func TestConcurrentSendersAndReceiver(t *testing.T) {
	h := New[int, int]()
	rx := h.Receiver()
	tx := h.Sender()
	const senders, perSender, keys = 8, 200, 16
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perSender; i++ {
				_ = tx.Send((base+i)%keys, base+i)
			}
		}(s * perSender)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := rx.Recv(); err != nil {
				return
			}
		}
	}()
	wg.Wait()
	rx.Close()
	<-done
}

// TestConcurrentChanConsumer is a -race smoke test for the feeder path.
func TestConcurrentChanConsumer(t *testing.T) {
	h := New[int, int]()
	rx := h.Receiver()
	ch := rx.Chan()
	tx := h.Sender()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch { //nolint:revive // draining until the feeder closes
		}
	}()
	for i := 0; i < 1000; i++ {
		require.NoError(t, tx.Send(i%8, i))
	}
	tx.Close()
	<-done
}

// assertLiveCount asserts the lock-free receiver count agrees with the map that
// owns the truth. This is the structural invariant the whole send fast path
// rests on: the count may over-report (a publisher then takes the lock and
// finds nothing, which is the pre-existing behaviour), but a count that
// under-reports drops a value permanently, since a conflated bus has no retry.
//
// Asserting the pair rather than the count alone is what makes a missed
// syncLiveLocked call site fail here, at the site that is wrong, rather than
// somewhere downstream as a lost event.
func assertLiveCount[K comparable, V any](t *testing.T, h *Hub[K, V]) {
	t.Helper()
	assert.Equal(t, int64(h.forTestingReceiverCount()), h.forTestingLiveReceivers())
}

// TestLiveCountTracksTheReceiverSet pins the invariant at every site that
// mutates s.receivers. The happens-before property the fast path needs — a
// registration that completed before a Send is observed by that Send — cannot
// be made to fail deterministically in a Go test; this invariant is what that
// property rests on, and it can.
func TestLiveCountTracksTheReceiverSet(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	assertLiveCount(t, h) // a fresh hub: the zero value is already correct

	rx1 := h.Receiver()
	assertLiveCount(t, h)
	rx2 := h.Receiver()
	assertLiveCount(t, h)

	rx1.Close()
	assertLiveCount(t, h)

	tx := h.Sender()
	require.NoError(t, tx.Send(1, 10))

	// From here the count is poisoned rather than tracking the map. The pair no
	// longer matches, and must not: a zero on a closed hub would hand the fast
	// path an ErrClosed to answer as nil.
	tx.Close()
	require.True(t, h.forTestingLivePoisoned())

	// A drain to terminal ErrClosed deregisters the receiver itself, which is a
	// different route into deregisterLocked than Receiver.Close — and the one
	// that runs after the poison.
	assertRecv(t, rx2, 1, 10)
	_, err := rx2.Recv()
	require.ErrorIs(t, err, gobus.ErrClosed)
	require.Zero(t, h.forTestingReceiverCount())
	assert.True(t, h.forTestingLivePoisoned(), "deregistration cleared the poison")
}

// TestHubCloseAlsoPoisonsTheCount pins the second close path. Hub.Close empties
// s.receivers without going through deregisterLocked, so the poison is the only
// thing standing between a hard tear-down and a fast path that answers nil.
func TestHubCloseAlsoPoisonsTheCount(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	h.Receiver()
	h.Close()
	assert.True(t, h.forTestingLivePoisoned())
}

// countSendLocks arms the hub-wide send seam with a lock counter and returns
// it. The seam runs only on the path that takes s.mu, so a zero count is proof
// that a send skipped the lock — no timing and no second field.
//
// The counter is atomic because the seam runs on the *sending* goroutine. Every
// caller below is single-goroutine, where a plain int would do, but a later
// edit that adds a publisher must not turn this into a silent data race. Arming
// itself is still only safe while no send is in flight: the field is hub-wide
// and is read outside s.mu.
func countSendLocks[K comparable, V any](h *Hub[K, V]) *atomic.Int64 {
	var locks atomic.Int64
	h.s.forTestingBeforeSendLock = func() { locks.Add(1) }
	return &locks
}

// TestSendTakesTheBusLockWithAReceiver pins that the send seam reports the
// locked path from Send, and not only from SendContext. Without this, the
// no-receiver assertions below would be vacuous: a lock counter that Send never
// increments reads zero whether or not a fast path exists.
func TestSendTakesTheBusLockWithAReceiver(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	defer h.Close()
	rx := h.Receiver()
	tx := h.Sender()
	locks := countSendLocks(h)

	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.TrySend(2, 20))
	require.NoError(t, tx.SendContext(context.Background(), 3, 30))

	assert.Equal(t, int64(3), locks.Load())
	assertRecv(t, rx, 1, 10)
}

// TestSendWithNoReceiversSkipsTheBusLock pins the fast path itself. The lock is
// hub-wide — every pop, Recv, Peek, TryRecv and Close takes it — so a publisher
// on an unwatched hub otherwise contends with work it has no consumer for.
func TestSendWithNoReceiversSkipsTheBusLock(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	defer h.Close()
	tx := h.Sender()
	locks := countSendLocks(h)

	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.TrySend(2, 20))

	assert.Zero(t, locks.Load(), "Send took the bus lock with no receiver")
}

// TestFastPathFollowsTheReceiverSet pins that the fast path tracks the live
// receiver set in both directions, rather than being a one-way latch decided at
// the first send.
func TestFastPathFollowsTheReceiverSet(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	defer h.Close()
	tx := h.Sender()
	locks := countSendLocks(h)

	require.NoError(t, tx.Send(1, 10))
	require.Zero(t, locks.Load(), "no receiver: no lock")

	rx := h.Receiver()
	require.NoError(t, tx.Send(1, 10))
	require.Equal(t, int64(1), locks.Load(), "one receiver: one lock")

	rx.Close()
	require.NoError(t, tx.Send(1, 10))
	require.Equal(t, int64(1), locks.Load(), "last receiver closed: no lock")

	h.Receiver()
	require.NoError(t, tx.Send(1, 10))
	assert.Equal(t, int64(2), locks.Load(), "new receiver: lock again")
}

// TestClosedSenderReportsErrClosedWithNoReceivers pins the ordering trap of the
// fast path: a closed sender must keep reporting ErrClosed even once the
// receiver set is empty. A count-first check without the poison turns that
// durable answer into nil.
//
// The receiver is closed *after* the sender on purpose. That order is what
// makes the poison guard in syncLiveLocked load-bearing: the deregistration
// runs after the poison and must not write a zero over it.
func TestClosedSenderReportsErrClosedWithNoReceivers(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	rx := h.Receiver()
	tx := h.Sender()

	tx.Close()
	rx.Close()
	require.Zero(t, h.forTestingReceiverCount())

	assert.ErrorIs(t, tx.Send(1, 10), gobus.ErrClosed)
	assert.ErrorIs(t, tx.TrySend(1, 10), gobus.ErrClosed)
	assert.ErrorIs(t, tx.SendContext(context.Background(), 1, 10), gobus.ErrClosed)
}

// TestHubCloseReportsErrClosedWithNoReceivers is the same trap by the other
// route: Hub.Close empties the receiver map itself, so the count reaches zero
// without any receiver being closed by the caller.
func TestHubCloseReportsErrClosedWithNoReceivers(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	h.Receiver()
	tx := h.Sender()

	h.Close()
	require.Zero(t, h.forTestingReceiverCount())

	assert.ErrorIs(t, tx.Send(1, 10), gobus.ErrClosed)
}

// TestDrainToErrClosedKeepsTheSenderClosed covers the third route to an empty
// receiver set: the receiver deregisters *itself* on the terminal ErrClosed of
// a sender-close drain, rather than being closed by the caller.
func TestDrainToErrClosedKeepsTheSenderClosed(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	defer h.Close()
	rx := h.Receiver()
	tx := h.Sender()

	require.NoError(t, tx.Send(1, 10))
	tx.Close()
	assertRecv(t, rx, 1, 10) // the soft-close drain
	_, err := rx.Recv()
	require.ErrorIs(t, err, gobus.ErrClosed)
	require.Zero(t, h.forTestingReceiverCount(), "terminal ErrClosed deregisters")

	assert.ErrorIs(t, tx.Send(2, 20), gobus.ErrClosed)
}

// TestSendContextWithNoReceiversSkipsTheBusLock pins that SendContext inherits
// the fast path. Its own cancellation check is what keeps it from being a plain
// delegation to Send.
func TestSendContextWithNoReceiversSkipsTheBusLock(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	defer h.Close()
	tx := h.Sender()
	locks := countSendLocks(h)

	require.NoError(t, tx.SendContext(context.Background(), 1, 10))

	assert.Zero(t, locks.Load(), "SendContext took the bus lock with no receiver")
}

// TestSendContextCancelledWithNoReceiversReportsCancellation pins that the fast
// path does not swallow a cancellation. Written green: the locked path already
// reported ctx.Err() here, and this guards the branch that replaces it.
//
// Only the return value is assertable. A hub with no receiver publishes nowhere
// either way, and a receiver created afterwards observes no history, so there is
// no "the value was not published" to assert that would fail against a mutant.
func TestSendContextCancelledWithNoReceiversReportsCancellation(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	defer h.Close()
	tx := h.Sender()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, tx.SendContext(ctx, 1, 10), context.Canceled)
}

// TestRegistrationBeforeSendIsAlwaysObserved states the property the fast path
// rests on, in the shape a consumer meets it: a registration that completed
// before a Send is observed by that Send.
//
// Know its limit. It is a smoke test, not the pin. No loop count makes a
// memory-ordering violation deterministic, and this fails only if the store is
// dropped outright. TestLiveCountTracksTheReceiverSet is what pins the
// invariant the property rests on; this states the intent and runs under -race.
//
// The channel close is the happens-before edge between the registration and the
// publish. A consumer supplies that edge itself — in the motivating case, by
// registering and then reading its snapshot under the lock the producer also
// takes. Nothing is asserted about a genuinely concurrent registration: both
// answers are correct there, so a test of it would pin nothing.
func TestRegistrationBeforeSendIsAlwaysObserved(t *testing.T) {
	const rounds = 200
	for i := 0; i < rounds; i++ {
		h := New[int](WithDefaultMerge(latestWins))
		tx := h.Sender()
		release := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			<-release
			done <- tx.Send(1, 10)
		}()

		rx := h.Receiver() // registration completes before the release below
		close(release)
		require.NoError(t, <-done)

		assertRecv(t, rx, 1, 10)
		h.Close()
	}
}

// TestSendContextCancelledOnEmptyHubStillLosesToClose pins closed > cancelled
// for a cancelled send on a hub with no receivers.
//
// The fast path reads the receiver count and ctx's Done channel at two
// different moments. A Sender.Close landing between them makes a cancellation
// verdict correct at neither: at the count read the answer is nil, and by the
// select the sender is closed and ErrClosed outranks the cancellation. So a
// cancelled ctx must not be resolved without the lock, where txClosed and
// ctxDone are read under one acquisition.
//
// The seam stands in for that window. It runs after the fast path has declined
// to answer and before the lock, so a close fired from it lands exactly where a
// concurrent Sender.Close would. A fast path that answers the cancellation
// itself never reaches the seam, and returns context.Canceled here.
func TestSendContextCancelledOnEmptyHubStillLosesToClose(t *testing.T) {
	h := New[int](WithDefaultMerge(latestWins))
	tx := h.Sender()
	require.Zero(t, h.forTestingReceiverCount(), "the fast path is what is under test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()
	h.s.forTestingBeforeSendLock = func() { tx.Close() }

	assert.ErrorIs(t, tx.SendContext(ctx, 1, 10), gobus.ErrClosed)
}
