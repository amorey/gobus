package conflate

import (
	"context"
	"runtime"
	"sync"
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

func TestImplementsCommonInterfaces(t *testing.T) {
	h := New[int](latestWins)
	var _ gobus.Sender[int, int] = h.Sender()
	var _ gobus.Receiver[int, int] = h.Receiver()
}

// TestEventIsTheSingleCurrency pins the property that makes Event worth having:
// every receive path hands back the same type, so one handler serves them all.
func TestEventIsTheSingleCurrency(t *testing.T) {
	h := New[int](latestWins)
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
	h := New[int](latestWins)
	rx := h.Receiver()
	defer rx.Close()
	require.NoError(t, h.Sender().Send(1, 10))
	// The same handler signature the Recv methods feed also drains Chan.
	handle := func(ev gobus.Event[int, int]) gobus.Event[int, int] { return ev }
	assert.Equal(t, gobus.Event[int, int]{Key: 1, Value: 10}, handle(<-rx.Chan()))
}

func TestNewRequiresMerge(t *testing.T) {
	assert.Panics(t, func() { New[int, int](nil) })
}

func TestSenderSingleton(t *testing.T) {
	h := New[int](latestWins)
	assert.Same(t, h.Sender(), h.Sender())
}

func TestBasicDelivery(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 100))
	assertRecv(t, rx, 1, 100)
	assertEmpty(t, rx)
}

func TestRecvWakesParkedReceiver(t *testing.T) {
	h := New[int](latestWins)
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
	h := New[int](latestWins)
	rx := h.Receiver()
	errCh := make(chan error, 1)
	go func() {
		_, err := rx.Recv()
		errCh <- err
	}()
	waitParked(t, rx)
	rx.Close()
	assert.ErrorIs(t, <-errCh, gobus.ErrClosed)
}

func TestRecvContextCancel(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := rx.RecvContext(ctx)
		errCh <- err
	}()
	waitParked(t, rx)
	cancel()
	assert.ErrorIs(t, <-errCh, context.Canceled)
}

func TestRecvContextAlreadyCancelled(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A cancelled context does not prevent delivery of an already-pending
	// event: the queue is checked before the loop ever parks.
	require.NoError(t, h.Sender().Send(1, 1))
	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[int, int]{Key: 1, Value: 1}, ev)
	_, err = rx.RecvContext(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestCoalesceLatestWins(t *testing.T) {
	h := New[int](latestWins)
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
	h := New[int](latestWins)
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
	h := New[int](latestWins)
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
	h := New[int](latestWins)
	assert.Panics(t, func() { h.WithKeyFilter(nil) })
}

func TestWithMergeIsPerReceiver(t *testing.T) {
	h := New[int](latestWins)
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
	h := New[int](latestWins)
	assert.Panics(t, func() { h.WithMerge(nil) })
}

// TestOptionsCompose covers the combination a constructor-per-variant API
// could not express at all: filter *and* a private merge on one receiver.
func TestOptionsCompose(t *testing.T) {
	h := New[int](latestWins)
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
	h := New[int](latestWins)
	// A nil option is a caller bug; fail loudly rather than nil-dereferencing
	// inside the hub, matching the nil-policy panics elsewhere in the package.
	assert.Panics(t, func() { h.Receiver(nil) })
}

// TestOptionsLastWins pins the documented precedence for repeated options.
func TestOptionsLastWins(t *testing.T) {
	h := New[int](latestWins)
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
	h := New[int](latestWins)
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
	h := New[int](latestWins)
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
	h := New[int](latestWins)
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
	h := New[int](latestWins)
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
	h := New[int](latestWins)
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
	h := New[int](latestWins)
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

func TestTryRecvOnClosedReceiver(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	rx.Close()
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestTryRecvCloseRaceBeforeLock(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 1))
	// Close wins the race between the lock-free done pre-check and taking mu;
	// the under-lock re-check must still return ErrClosed, not a value.
	rx.forTestingBeforeTryRecvLock = func() { rx.Close() }
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestReceiverClose(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	rx.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	rx.Close() // idempotent
}

func TestHubClose(t *testing.T) {
	h := New[int](latestWins)
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
	h := New[int](latestWins)
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

func TestSenderCloseWakesParkedReceiver(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	errCh := make(chan error, 1)
	go func() {
		_, err := rx.Recv()
		errCh <- err
	}()
	waitParked(t, rx)
	h.Sender().Close()
	assert.ErrorIs(t, <-errCh, gobus.ErrClosed)
}

func TestCloseRaceBeforeLock(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	// Close wins the race between the lock-free done pre-check and taking mu;
	// the under-lock re-check must still return ErrClosed, not a value.
	require.NoError(t, h.Sender().Send(1, 1))
	rx.forTestingBeforeRecvLock = func() { rx.Close() }
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestChanDeliversInOrderAndClosesOnSenderClose(t *testing.T) {
	h := New[int](func(_, next string) (string, bool) { return next, true })
	rx := h.Receiver()
	defer rx.Close()
	ch := rx.Chan()
	assert.Equal(t, (<-chan gobus.Event[int, string])(rx.ch), rx.Chan(), "Chan should be memoized")

	tx := h.Sender()
	require.NoError(t, tx.Send(1, "a"))
	assert.Equal(t, gobus.Event[int, string]{Key: 1, Value: "a"}, <-ch)

	// Coalescing still applies behind the feeder: the feeder is parked on
	// notify with an empty queue, so both sends land in key 2's slot and only
	// the merged value is delivered.
	waitParked(t, rx)
	require.NoError(t, tx.Send(2, "b"))
	require.NoError(t, tx.Send(2, "c"))
	ev := <-ch
	assert.Equal(t, 2, ev.Key)
	assert.Contains(t, []string{"b", "c"}, ev.Value)

	tx.Close()
	_, ok := <-ch
	assert.False(t, ok, "feeder should close the channel after draining")
	assert.Equal(t, 0, h.forTestingReceiverCount(), "drained feeder should deregister")
}

func TestChanClosesOnReceiverClose(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	ch := rx.Chan()
	waitParked(t, rx) // feeder is parked on notify
	rx.Close()
	_, ok := <-ch
	assert.False(t, ok)
}

func TestChanOnPreClosedReceiver(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	rx.Close()
	// The feeder's first check sees the closed receiver and exits immediately.
	_, ok := <-rx.Chan()
	assert.False(t, ok)
}

func TestChanFeederCloseRaceBeforeLock(t *testing.T) {
	h := New[int](latestWins)
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 1))
	// Close wins the race between the feeder's lock-free pre-check and the
	// lock; the under-lock re-check must abandon the pending event.
	rx.forTestingFeederBeforeLock = func() { rx.Close() }
	_, ok := <-rx.Chan()
	assert.False(t, ok)
}

func TestChanFeederCloseWhileDelivering(t *testing.T) {
	h := New[int](latestWins)
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
	h := New[int](func(_, next int) (int, bool) { return next, true })
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
	h := New[int](func(_, next int) (int, bool) { return next, true })
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
