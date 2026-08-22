package watch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gobus"
)

// The handles implement the module-wide interfaces. A compile-time assertion,
// so a signature drift fails the build rather than the conformance suite.
var (
	_ gobus.Sender[string, int]   = (*Sender[string, int])(nil)
	_ gobus.Receiver[string, int] = (*Receiver[string, int])(nil)
)

// val is the test value type: a payload plus the order it moved in, matching
// the shape the requester uses.
type val struct {
	N   int
	Seq uint64
}

// bySeq is the requester's rule: a strict order over val, as R16a requires.
func bySeq(prev, next val) bool { return next.Seq > prev.Seq }

func TestNewReturnsALiveHub(t *testing.T) {
	h := New[string, val]()
	require.NotNil(t, h)
	assert.NotNil(t, h.Sender())
}

func TestNewAcceptsAnOptionAndInfersV(t *testing.T) {
	// The call site spells K only: V comes from the option (D6).
	h := New[string](WithAccept(bySeq))
	require.NotNil(t, h)
}

func TestWithAcceptPanicsOnNil(t *testing.T) {
	assert.PanicsWithValue(t, "gobus: watch.WithAccept requires a non-nil Accept", func() {
		WithAccept[val](nil)
	})
}

func TestNewPanicsOnNilOption(t *testing.T) {
	// New takes ...Option[V], so New[string, val](nil) is legal Go. Without
	// this check it panics on a nil func call with no message (R50).
	assert.PanicsWithValue(t, "gobus: watch.New received a nil Option", func() {
		New[string, val](nil)
	})
}

func TestSenderIsASingleton(t *testing.T) {
	h := New[string, val]()
	assert.Same(t, h.Sender(), h.Sender())
}

func TestWatchDoesNotDeliverTheSeed(t *testing.T) {
	// R19: the baseline is the caller's own argument. It is the prev of the
	// first Accept, not a value to hand back.
	h := New[string](WithAccept(bySeq))
	rx := h.Watch("a", h.WithBaseline(val{N: 1, Seq: 1}))
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestWatchPanicsOnNilOption(t *testing.T) {
	// Watch takes ...WatchOption[K, V], so Watch("a", nil) is legal Go.
	h := New[string, val]()
	assert.PanicsWithValue(t, "gobus: watch.Hub.Watch received a nil WatchOption", func() {
		h.Watch("a", nil)
	})
}

func TestWatchAcrossPanicsOnNilOption(t *testing.T) {
	h := New[string, val]()
	assert.PanicsWithValue(t, "gobus: watch.Hub.Watch received a nil WatchOption", func() {
		h.WatchAcross(nil)
	})
}

// TestNoBaselineTakesTheFirstValueUnjudged pins what omitting WithBaseline
// means. bySeq would reject this value against any baseline at or above Seq 5,
// including the zero val the receiver would hold if the slot were simply seeded
// empty — so a hub that invented a prev fails here.
func TestNoBaselineTakesTheFirstValueUnjudged(t *testing.T) {
	h := New[string](WithAccept(func(prev, next val) bool {
		require.Fail(t, "Accept must not run against an empty slot")
		return false
	}))
	rx := h.Watch("a")
	require.NoError(t, h.Sender().Send("a", val{N: 7, Seq: 0}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 7, Seq: 0}})
}

// TestNoBaselineJudgesEveryValueAfterTheFirst pins the other half: the slot
// holds a value from the first send on, so Accept governs from the second.
func TestNoBaselineJudgesEveryValueAfterTheFirst(t *testing.T) {
	h := New[string](WithAccept(bySeq))
	rx := h.Watch("a")
	tx := h.Sender()

	require.NoError(t, tx.Send("a", val{N: 1, Seq: 5})) // taken unjudged
	require.NoError(t, tx.Send("a", val{N: 2, Seq: 4})) // loses to Seq 5
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 1, Seq: 5}})

	require.NoError(t, tx.Send("a", val{N: 3, Seq: 6})) // beats Seq 5
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 3, Seq: 6}})
}

// TestWithBaselineTakesTheZeroValue pins the zero V as a usable baseline:
// hasBaseline carries the "set" bit, so a receiver based at the zero val must
// judge against it rather than take the first value unjudged.
func TestWithBaselineTakesTheZeroValue(t *testing.T) {
	h := New[string](WithAccept(bySeq))
	rx := h.Watch("a", h.WithBaseline(val{}))
	// Seq 0 does not beat the zero baseline's Seq 0, so nothing lands.
	require.NoError(t, h.Sender().Send("a", val{N: 7, Seq: 0}))
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

// TestWatchAcrossWithoutBaselineTakesTheFirstValue pins the wildcard receiver
// on the same rule, including the key that travels with that first value.
func TestWatchAcrossWithoutBaselineTakesTheFirstValue(t *testing.T) {
	h := New[string](WithAccept(bySeq))
	rx := h.WatchAcross()
	require.NoError(t, h.Sender().Send("b", val{N: 4, Seq: 0}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "b", Value: val{N: 4, Seq: 0}})
}

// TestLastBaselineWins pins the option-application order Watch promises.
func TestLastBaselineWins(t *testing.T) {
	h := New[string](WithAccept(bySeq))
	rx := h.Watch("a", h.WithBaseline(val{N: 1, Seq: 9}), h.WithBaseline(val{N: 2, Seq: 1}))
	// Seq 5 loses to the first baseline and beats the second.
	require.NoError(t, h.Sender().Send("a", val{N: 3, Seq: 5}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 3, Seq: 5}})
}

// TestBaselineIsPerReceiver pins why the baseline is an option on Watch rather
// than on the hub: two receivers on one key register at different instants and
// judge the same value against their own reads.
func TestBaselineIsPerReceiver(t *testing.T) {
	h := New[string](WithAccept(bySeq))
	behind := h.Watch("a", h.WithBaseline(val{N: 1, Seq: 1}))
	ahead := h.Watch("a", h.WithBaseline(val{N: 1, Seq: 9}))

	require.NoError(t, h.Sender().Send("a", val{N: 2, Seq: 5}))
	assertRecv(t, behind, gobus.Event[string, val]{Key: "a", Value: val{N: 2, Seq: 5}})
	_, err := ahead.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestTryRecvIsEmptyUntilSomethingChanges(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	for i := 0; i < 3; i++ {
		_, err := rx.TryRecv()
		require.ErrorIs(t, err, gobus.ErrEmpty)
	}
}

func TestReceiverCloseIsTheUnwatch(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.Equal(t, 1, h.forTestingReceiverCount())
	require.Equal(t, 1, h.forTestingKeyCount())

	rx.Close()
	assert.Equal(t, 0, h.forTestingReceiverCount())
	// R5: the last receiver for a key takes the key's state with it.
	assert.Equal(t, 0, h.forTestingKeyCount())
}

func TestReceiverCloseIsIdempotent(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	rx.Close()
	assert.NotPanics(t, rx.Close)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestKeyStateSurvivesWhileAnotherReceiverWatches(t *testing.T) {
	h := New[string, val]()
	a := h.Watch("k", h.WithBaseline(val{N: 1}))
	b := h.Watch("k", h.WithBaseline(val{N: 1}))
	require.Equal(t, 1, h.forTestingKeyCount())

	a.Close()
	assert.Equal(t, 1, h.forTestingKeyCount(), "b still watches k")
	b.Close()
	assert.Equal(t, 0, h.forTestingKeyCount())
}

func TestWatchAfterHubCloseIsPreClosed(t *testing.T) {
	h := New[string, val]()
	h.Close()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NotNil(t, rx, "R23: a pre-closed handle, never nil")
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

// assertRecv pins the whole Event, so the key/value pairing is checked too.
func assertRecv(t *testing.T, rx *Receiver[string, val], want gobus.Event[string, val]) {
	t.Helper()
	ev, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, want, ev)
}

// assertPeek is assertRecv's non-consuming twin, for the same reason.
func assertPeek(t *testing.T, rx *Receiver[string, val], want gobus.Event[string, val]) {
	t.Helper()
	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, want, ev)
}

func TestSendReachesTheWatchingReceiver(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
}

func TestSendForAnUnwatchedKeyIsDropped(t *testing.T) {
	// R43c: no receiver means no buffer. A later Watch never sees it.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("b", val{N: 9}))

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
	assert.Equal(t, 1, h.forTestingKeyCount(), "b was never retained")
}

func TestSendOnlyTouchesItsOwnKey(t *testing.T) {
	h := New[string, val]()
	a := h.Watch("a", h.WithBaseline(val{N: 1}))
	b := h.Watch("b", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	assertRecv(t, a, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
	_, err := b.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestAcceptRejectsAValueAndTheReceiverNeverLearns(t *testing.T) {
	// R9: a false result changes nothing, silently.
	h := New[string](WithAccept(bySeq))
	rx := h.Watch("a", h.WithBaseline(val{N: 1, Seq: 5}))
	require.NoError(t, h.Sender().Send("a", val{N: 2, Seq: 4}))

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestAcceptRunsPerReceiverAgainstItsOwnSlot(t *testing.T) {
	// R10: two receivers of one key seeded at different moments. One value is
	// new for the older seed and stale for the newer one.
	h := New[string](WithAccept(bySeq))
	behind := h.Watch("k", h.WithBaseline(val{N: 1, Seq: 3}))
	ahead := h.Watch("k", h.WithBaseline(val{N: 1, Seq: 7}))

	require.NoError(t, h.Sender().Send("k", val{N: 2, Seq: 5}))

	assertRecv(t, behind, gobus.Event[string, val]{Key: "k", Value: val{N: 2, Seq: 5}})
	_, err := ahead.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "5 is stale against a seed of 7")
}

func TestTheDefaultAcceptTakesEveryValue(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1, Seq: 9}))
	require.NoError(t, h.Sender().Send("a", val{N: 2, Seq: 1}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2, Seq: 1}})
}

func TestAnUnreadValueIsOverwrittenNotQueued(t *testing.T) {
	// R26: a slow reader skips to the current value.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 0}))
	for i := 1; i <= 3; i++ {
		require.NoError(t, h.Sender().Send("a", val{N: i}))
	}
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 3}})

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "one slot, not a queue")
}

func TestTrySendIsSend(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().TrySend("a", val{N: 2}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
}

func TestAcceptPanicReleasesTheLockAndLeavesAPartialFanOut(t *testing.T) {
	// R14 and R14a. The receivers reached before the panic keep the value; the
	// rest do not; the hub stays usable.
	var boom bool
	h := New[string](WithAccept(func(prev, next val) bool {
		if boom && next.N == 99 {
			panic("accept exploded")
		}
		return true
	}))
	first := h.Watch("k", h.WithBaseline(val{N: 0}))
	second := h.Watch("k", h.WithBaseline(val{N: 0}))

	boom = true
	assert.PanicsWithValue(t, "accept exploded", func() {
		_ = h.Sender().Send("k", val{N: 99})
	})

	// The lock is free, so the hub still works.
	boom = false
	require.NoError(t, h.Sender().Send("k", val{N: 7}))

	// Exactly one of the two saw the panicking value; both end on N=7.
	assertRecv(t, first, gobus.Event[string, val]{Key: "k", Value: val{N: 7}})
	assertRecv(t, second, gobus.Event[string, val]{Key: "k", Value: val{N: 7}})
}

// waitParked spins until n readers are parked on rx, so a test can fire a
// Send or Close at a reader that is provably in its blocking select. This
// replaces a sleep, which would only encode a guess about the scheduler.
func waitParked(t *testing.T, rx *Receiver[string, val], n int) {
	t.Helper()
	for {
		rx.s.mu.Lock()
		got := rx.waiters
		rx.s.mu.Unlock()
		if got >= n {
			return
		}
	}
}

func TestRecvReturnsAnAlreadyUnreadValue(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	ev, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, ev)
}

func TestRecvBlocksUntilASendLands(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))

	got := make(chan gobus.Event[string, val], 1)
	go func() {
		ev, err := rx.Recv()
		require.NoError(t, err)
		got <- ev
	}()

	waitParked(t, rx, 1)
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, <-got)
}

func TestRecvIgnoresARejectedValueAndStaysParked(t *testing.T) {
	h := New[string](WithAccept(bySeq))
	rx := h.Watch("a", h.WithBaseline(val{N: 1, Seq: 5}))

	got := make(chan gobus.Event[string, val], 1)
	go func() {
		ev, err := rx.Recv()
		require.NoError(t, err)
		got <- ev
	}()

	waitParked(t, rx, 1)
	require.NoError(t, h.Sender().Send("a", val{N: 2, Seq: 4})) // rejected
	require.NoError(t, h.Sender().Send("a", val{N: 3, Seq: 6})) // taken
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 3, Seq: 6}}, <-got)
}

func TestRecvOnAClosedReceiverIsTerminal(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	rx.Close()

	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestEveryCloseWakesAParkedReader(t *testing.T) {
	for _, tc := range []struct {
		name  string
		close func(*Hub[string, val], *Receiver[string, val])
	}{
		{"receiver", func(_ *Hub[string, val], rx *Receiver[string, val]) { rx.Close() }},
		{"sender", func(h *Hub[string, val], _ *Receiver[string, val]) { h.Sender().Close() }},
		{"hub", func(h *Hub[string, val], _ *Receiver[string, val]) { h.Close() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := New[string, val]()
			rx := h.Watch("a", h.WithBaseline(val{N: 1}))

			done := make(chan error, 1)
			go func() {
				_, err := rx.Recv()
				done <- err
			}()

			waitParked(t, rx, 1)
			tc.close(h, rx)
			assert.ErrorIs(t, <-done, gobus.ErrClosed)
		})
	}
}

func TestParkedReadersLeaveNoWaiterBehind(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := rx.Recv()
		require.NoError(t, err)
	}()

	waitParked(t, rx, 1)
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	<-done

	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	assert.Zero(t, rx.waiters)
}

func TestSenderCloseDrainsThenErrClosed(t *testing.T) {
	// R38: the unread value survives the soft close.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	h.Sender().Close()

	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestSenderCloseWithNothingUnreadIsTerminalAtOnce(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	h.Sender().Close()

	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestSenderCloseRejectsFurtherSends(t *testing.T) {
	h := New[string, val]()
	tx := h.Sender()
	tx.Close()
	assert.ErrorIs(t, tx.Send("a", val{N: 1}), gobus.ErrClosed)
	assert.ErrorIs(t, tx.TrySend("a", val{N: 1}), gobus.ErrClosed)
	assert.NotPanics(t, tx.Close)
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
// send publishes nothing (the receiver's slot still holds its baseline), and a
// send that wins the ordering is still drained by the soft close.
func TestSenderCloseIsSafeConcurrentWithSend(t *testing.T) {
	t.Run("close lands first", func(t *testing.T) {
		h := New[string, val]()
		tx := h.Sender()
		rx := h.Watch("a", h.WithBaseline(val{N: 1}))

		h.s.forTestingBeforeSendLock = func() { tx.Close() }
		assert.ErrorIs(t, tx.Send("a", val{N: 2}), gobus.ErrClosed)
		h.s.forTestingBeforeSendLock = nil

		// Terminal at once, which is only true if nothing was published: a
		// slot holding N:2 would drain it before reporting ErrClosed.
		_, err := rx.TryRecv()
		assert.ErrorIs(t, err, gobus.ErrClosed)
	})

	t.Run("send lands first", func(t *testing.T) {
		h := New[string, val]()
		tx := h.Sender()
		rx := h.Watch("a", h.WithBaseline(val{N: 1}))

		require.NoError(t, tx.Send("a", val{N: 2}))
		tx.Close()

		assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
	})
}

// TestSenderCloseIsSafeConcurrentWithSendContext is the SendContext twin. The
// promise covers both send paths, and SendContext reaches the lock by its own
// route — the fast path it declines is the one that also consults ctx.
func TestSenderCloseIsSafeConcurrentWithSendContext(t *testing.T) {
	h := New[string, val]()
	tx := h.Sender()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))

	h.s.forTestingBeforeSendLock = func() { tx.Close() }
	assert.ErrorIs(t, tx.SendContext(context.Background(), "a", val{N: 2}), gobus.ErrClosed)
	h.s.forTestingBeforeSendLock = nil

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

// TestSenderCloseOnIdleHubRacesToNilOrClosed pins the other half of "no third
// outcome": with no receiver a send answers from the lock-free load, so the
// close it races is either already poisoned or not yet. Both answers are the
// contract; neither publishes anything, since there is nobody watching.
func TestSenderCloseOnIdleHubRacesToNilOrClosed(t *testing.T) {
	h := New[string, val]()
	tx := h.Sender()
	require.Zero(t, h.forTestingReceiverCount(), "the fast path is what is under test")

	assert.NoError(t, tx.Send("a", val{N: 1}), "load precedes the poison")
	tx.Close()
	assert.ErrorIs(t, tx.Send("a", val{N: 2}), gobus.ErrClosed, "load follows the poison")
}

func TestTerminalErrClosedDropsTheKey(t *testing.T) {
	// R41 and R43: a receiver leaves the hub by draining, not only by Close,
	// and both reading paths owe the same tear-down.
	reads := map[string]func(*Receiver[string, val]) error{
		"TryRecv": func(rx *Receiver[string, val]) error { _, err := rx.TryRecv(); return err },
		"Recv":    func(rx *Receiver[string, val]) error { _, err := rx.Recv(); return err },
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			h := New[string, val]()
			rx := h.Watch("a", h.WithBaseline(val{N: 1}))
			h.Sender().Close()

			require.ErrorIs(t, read(rx), gobus.ErrClosed)
			assert.Zero(t, h.forTestingReceiverCount())
			assert.Zero(t, h.forTestingKeyCount(), "R5 holds on both exit paths")
		})
	}
}

func TestHubCloseIsHardTearDown(t *testing.T) {
	// R39: no drain, even with a value unread.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	h.Close()

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.ErrorIs(t, h.Sender().Send("a", val{N: 3}), gobus.ErrClosed)
	assert.NotPanics(t, h.Close)
}

func TestWatchAfterSenderCloseIsLiveButTerminal(t *testing.T) {
	// R43a: unlike Hub.Close, the handle is not pre-closed. It holds nothing
	// unread, so its first read is terminal.
	h := New[string, val]()
	h.Sender().Close()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.False(t, rx.done.IsClosed(), "live handle, not pre-closed")

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.Equal(t, 0, h.forTestingKeyCount())
}

func TestAnIdleReceiverPinsItsKey(t *testing.T) {
	// R43b: R5 is a guarantee about closed receivers, not idle ones.
	h := New[string, val]()
	_ = h.Watch("a", h.WithBaseline(val{N: 1}))
	assert.Equal(t, 1, h.forTestingKeyCount())
}

func TestHubSenderAfterHubCloseReportsErrClosed(t *testing.T) {
	// R43d: the same handle, never nil.
	h := New[string, val]()
	h.Close()
	tx := h.Sender()
	require.NotNil(t, tx)
	assert.ErrorIs(t, tx.Send("a", val{N: 1}), gobus.ErrClosed)
}

func TestCloseRaceIsResolvedUnderTheLock(t *testing.T) {
	// The lock-free rx.done pre-check can go stale; the re-check under s.mu is
	// what makes "close wins" correct rather than best-effort.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	rx.forTestingBeforeTryRecvLock = func() { rx.Close() }
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestCloseRaceIsResolvedUnderTheLockForRecv(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	rx.forTestingBeforeRecvLock = func() { rx.Close() }
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestPeekDoesNotConsume(t *testing.T) {
	// Two Peeks report the same value while nothing supersedes it, and it is
	// still there for the read that actually takes it.
	h := New[string](WithAccept(bySeq))
	rx := h.Watch("a", h.WithBaseline(val{N: 1, Seq: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2, Seq: 2}))

	want := gobus.Event[string, val]{Key: "a", Value: val{N: 2, Seq: 2}}
	assertPeek(t, rx, want)
	assertPeek(t, rx, want)

	// The key is fixed for a receiver's whole life, but the value is not: an
	// accepted Send replaces the slot, so the next Peek reports the newer
	// value and the superseded one is never handed back by any path.
	require.NoError(t, h.Sender().Send("a", val{N: 3, Seq: 3}))
	newest := gobus.Event[string, val]{Key: "a", Value: val{N: 3, Seq: 3}}
	assertPeek(t, rx, newest)
	assertRecv(t, rx, newest)
}

func TestPeekReportsWhatIsUnreadNotTheCurrentState(t *testing.T) {
	// Peek is TryRecv minus the take, not a read of the key's state: the
	// baseline is not peekable, and neither is a value already taken.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))

	_, err := rx.Peek()
	require.ErrorIs(t, err, gobus.ErrEmpty, "the baseline is not a delivery")

	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})

	_, err = rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "the slot holds a value, but nothing is unread")
}

// TestPeekPrecedenceMatchesTryRecv pins that Peek is not a raw-state read: a
// closed handle reports ErrClosed even with an unread value waiting, exactly
// as TryRecv does.
func TestPeekPrecedenceMatchesTryRecv(t *testing.T) {
	for _, tc := range []struct {
		name  string
		close func(*Hub[string, val], *Receiver[string, val])
	}{
		{"receiver", func(_ *Hub[string, val], rx *Receiver[string, val]) { rx.Close() }},
		{"hub", func(h *Hub[string, val], _ *Receiver[string, val]) { h.Close() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := New[string, val]()
			rx := h.Watch("a", h.WithBaseline(val{N: 1}))
			require.NoError(t, h.Sender().Send("a", val{N: 2}))
			tc.close(h, rx)

			_, err := rx.Peek()
			assert.ErrorIs(t, err, gobus.ErrClosed,
				"ErrClosed is not a statement that nothing was unread")
		})
	}
}

func TestPeekDrainsThenReportsClosed(t *testing.T) {
	// Sender.Close is the soft path, so the final value is still peekable —
	// and peeking it does not consume it.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	h.Sender().Close()

	want := gobus.Event[string, val]{Key: "a", Value: val{N: 2}}
	assertPeek(t, rx, want)
	assertRecv(t, rx, want)

	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	// The terminal verdict owes the same tear-down whichever path derives it.
	assert.Zero(t, h.forTestingReceiverCount(), "the terminal Peek skipped deregistration")
	assert.Zero(t, h.forTestingKeyCount(), "R5 holds on the Peek exit path too")
}

func TestPeekCloseRaceIsResolvedUnderTheLock(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	rx.forTestingBeforePeekLock = func() { rx.Close() }
	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

// TestPeekSeesTheValueInFlightToTheFeeder is where this bus differs from
// conflate: the feeder marks a value read only once the consumer has taken it,
// so the value it is holding is still unread and Peek reports it.
func TestPeekSeesTheValueInFlightToTheFeeder(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	defer rx.Close()

	// The hook runs on the feeder goroutine, so the result is carried back to
	// the test goroutine to assert on: a require here would Goexit the feeder
	// and park the test on the channel below until the package timeout.
	type peek struct {
		ev  gobus.Event[string, val]
		err error
	}
	peeked := make(chan peek, 1)
	rx.forTestingFeederParked = func() {
		rx.forTestingFeederParked = nil
		ev, err := rx.Peek()
		peeked <- peek{ev, err}
	}
	ch := rx.Chan()
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	want := gobus.Event[string, val]{Key: "a", Value: val{N: 2}}
	got := <-peeked
	require.NoError(t, got.err)
	assert.Equal(t, want, got.ev)
	assert.Equal(t, want, <-ch)
}

// peekSink is a package-level sink for the allocation test. Assigning the
// returned Event to a local would let the escape analysis of the test body,
// rather than Peek itself, decide the result.
var peekSink gobus.Event[string, val]

func TestPeekAllocatesNothing(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	// Three field reads and a struct return: nothing to allocate.
	avg := testing.AllocsPerRun(100, func() { peekSink, _ = rx.Peek() })
	assert.Zero(t, avg, "Peek should allocate nothing")
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, peekSink)
}

func TestRecvContextReturnsAValue(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	ev, err := rx.RecvContext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, ev)
}

func TestRecvContextCancelBeatsAnUnreadValue(t *testing.T) {
	// R36: cancellation outranks a ready value, and does not consume it.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rx.RecvContext(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// The value was declined, not eaten.
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
}

func TestRecvContextCancelWakesAParkedReader(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := rx.RecvContext(ctx)
		done <- err
	}()

	waitParked(t, rx, 1)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestRecvContextCancelDoesNotCloseTheReceiver(t *testing.T) {
	// R37: ctx.Err() is not an end of stream.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rx.RecvContext(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, h.forTestingReceiverCount(), "still registered")

	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
}

func TestClosedBeatsCancelledOnTheReceiveSide(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	rx.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestDrainedSenderCloseBeatsCancelled(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	h.Sender().Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestParkedCloseAndCancelResolveToClosed(t *testing.T) {
	// Arm both terminations while holding s.mu: the woken reader cannot pass
	// the lock until we release, so both are visible whenever it derives its
	// answer, whatever the scheduler does. Arm the context first — signalling
	// the receiver first lets the reader leave the select before ctxDone
	// exists, and the arm under test is never taken.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := rx.RecvContext(ctx)
		done <- err
	}()

	waitParked(t, rx, 1)
	rx.s.mu.Lock()
	cancel()
	rx.done.Close()
	rx.s.mu.Unlock()

	assert.ErrorIs(t, <-done, gobus.ErrClosed)
}

func TestSendContextPublishes(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	require.NoError(t, h.Sender().SendContext(context.Background(), "a", val{N: 2}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
}

func TestSendContextCancelledDoesNotPublish(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, h.Sender().SendContext(ctx, "a", val{N: 2}), context.Canceled)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestClosedBeatsCancelledOnTheSendSide(t *testing.T) {
	h := New[string, val]()
	_ = h.Watch("a", h.WithBaseline(val{N: 1}))
	h.Sender().Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, h.Sender().SendContext(ctx, "a", val{N: 2}), gobus.ErrClosed)
}

func TestSendContextChecksCancellationAtTheLockNotAtEntry(t *testing.T) {
	// R35: nothing is published for a context that expired while the send was
	// waiting for the lock.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	ctx, cancel := context.WithCancel(context.Background())

	h.s.forTestingBeforeSendLock = cancel
	require.ErrorIs(t, h.Sender().SendContext(ctx, "a", val{N: 2}), context.Canceled)
	h.s.forTestingBeforeSendLock = nil

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestChanDeliversValues(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	defer rx.Close()

	ch := rx.Chan()
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, <-ch)
}

func TestChanIsTheSameChannelEveryTime(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	defer rx.Close()
	assert.Equal(t, rx.Chan(), rx.Chan())
}

func TestChanClosesOnReceiverClose(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	ch := rx.Chan()

	rx.Close()
	_, open := <-ch
	assert.False(t, open)
}

func TestChanClosesAfterSenderCloseDrains(t *testing.T) {
	// R46: the final value first, then the close.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	defer rx.Close()
	ch := rx.Chan()

	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, <-ch)

	h.Sender().Close()
	_, open := <-ch
	assert.False(t, open)
}

func TestChanClosesOnHubClose(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 1}))
	defer rx.Close()
	ch := rx.Chan()

	h.Close()
	_, open := <-ch
	assert.False(t, open)
}

func TestFeederSkipsToTheCurrentValue(t *testing.T) {
	// R45: a value that lands while the feeder is parked on delivery replaces
	// the one waiting, so a slow consumer reads the current value.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 0}))
	defer rx.Close()

	// Sequence through the feeder's own hook, which fires on every park: the
	// first is the snapshot of N:1, where the replacing Send lands, and the
	// second is the re-snapshot it forces. The consumer is released only on
	// that second one. Arm the hook before Chan starts the feeder, or the
	// write races the feeder's read.
	//
	// Releasing the consumer on the *first* park is what made this flaky, and
	// it fails the way this bus documents rather than by any defect: the
	// replacing Send closes notify, so a consumer arriving at <-ch before the
	// feeder reaches its select leaves both arms ready, and Go chooses between
	// ready arms at random. Half those runs deliver the superseded N:1, which
	// [Receiver.Chan] expressly permits — so R45 is only assertable in the
	// window where nobody is waiting on the channel. Waiting for the
	// re-snapshot puts us in it: notify is fresh by then, and the send to ch
	// is the only arm that can fire.
	replaced := make(chan struct{})
	parks := 0 // feeder-goroutine-only, like the hook itself
	rx.forTestingFeederParked = func() {
		parks++
		switch parks {
		case 1:
			require.NoError(t, h.Sender().Send("a", val{N: 2}))
		case 2:
			close(replaced)
		}
	}
	ch := rx.Chan()
	require.NoError(t, h.Sender().Send("a", val{N: 1}))

	<-replaced
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, <-ch)
	assert.Equal(t, 2, parks, "the feeder never re-snapshotted")
}

func TestFeederConvergesOnTheNewestValue(t *testing.T) {
	// R45b: whatever the delivery select picks, a consumer that keeps reading
	// ends on the current value. R45c permits the superseded one to arrive
	// first, so this must not assert that it does not.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 0}))
	defer rx.Close()
	ch := rx.Chan()

	for i := 1; i <= 5; i++ {
		require.NoError(t, h.Sender().Send("a", val{N: i}))
	}
	for ev := range ch {
		if ev.Value.N == 5 {
			return
		}
	}
	t.Fatal("channel closed before the newest value arrived")
}

func TestFeederCloseWhileDeliveringDoesNotLeak(t *testing.T) {
	// Both arms of the delivery select can be ready at once. Sequence with the
	// exit hook rather than letting a waiting reader make the choice random.
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 0}))

	exited := make(chan struct{})
	rx.forTestingFeederExit = func() { close(exited) }
	rx.forTestingFeederParked = func() {
		rx.forTestingFeederParked = nil
		rx.Close()
	}
	ch := rx.Chan()
	require.NoError(t, h.Sender().Send("a", val{N: 1}))

	// Wait on the exit hook, not on the channel. A reader here would make the
	// delivery select's two arms both ready and the outcome random; with
	// nobody reading, the close arm is the only one that can fire.
	<-exited
	for range ch {
	}
}

func TestFeederCloseRaceIsResolvedUnderTheLock(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 0}))

	exited := make(chan struct{})
	rx.forTestingFeederExit = func() { close(exited) }
	rx.forTestingFeederBeforeLock = func() {
		rx.forTestingFeederBeforeLock = nil
		rx.Close()
	}
	ch := rx.Chan()
	require.NoError(t, h.Sender().Send("a", val{N: 1}))

	<-exited
	for range ch {
	}
}

// countLocks arms the send-side hook to count the sends that reach the lock.
// Arm it only while no send is in flight: the field is hub-wide and read
// outside the mutex.
func countLocks(h *Hub[string, val], n *int) {
	h.s.forTestingBeforeSendLock = func() { *n++ }
}

func TestSendSkipsTheLockWithNoReceiver(t *testing.T) {
	// R30: no receiver means no work and no other answer to give.
	h := New[string, val]()
	var locks int
	countLocks(h, &locks)

	require.NoError(t, h.Sender().Send("a", val{N: 1}))
	assert.Zero(t, locks, "the hub lock was taken for nobody")
}

func TestSendTakesTheLockOnceAReceiverExists(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 0}))
	defer rx.Close()

	var locks int
	countLocks(h, &locks)
	require.NoError(t, h.Sender().Send("a", val{N: 1}))
	assert.Equal(t, 1, locks)
}

func TestTheFastPathReturnsAfterTheLastReceiverLeaves(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", h.WithBaseline(val{N: 0}))
	require.EqualValues(t, 1, h.forTestingLiveReceivers())

	rx.Close()
	assert.EqualValues(t, 0, h.forTestingLiveReceivers())

	var locks int
	countLocks(h, &locks)
	require.NoError(t, h.Sender().Send("a", val{N: 1}))
	assert.Zero(t, locks)
}

func TestClosedHubsStayOnTheLockedPath(t *testing.T) {
	// The poison is what makes ErrClosed durable: a Receiver.Close after a
	// close must not write a zero over it and let the fast path answer nil.
	for _, tc := range []struct {
		name  string
		close func(*Hub[string, val])
	}{
		{"sender", func(h *Hub[string, val]) { h.Sender().Close() }},
		{"hub", func(h *Hub[string, val]) { h.Close() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := New[string, val]()
			rx := h.Watch("a", h.WithBaseline(val{N: 0}))
			tc.close(h)
			rx.Close()

			assert.ErrorIs(t, h.Sender().Send("a", val{N: 1}), gobus.ErrClosed)
		})
	}
}

func TestSendContextFastPathAnswersOnlyNil(t *testing.T) {
	h := New[string, val]()
	var locks int
	countLocks(h, &locks)

	require.NoError(t, h.Sender().SendContext(context.Background(), "a", val{N: 1}))
	assert.Zero(t, locks)
}

func TestSendContextCancelledOnEmptyHubStillLosesToClose(t *testing.T) {
	// The count and ctxDone are two reads at two moments, so a cancelled ctx
	// cannot be answered without the lock: a Sender.Close landing between them
	// would make ctx.Err() right at neither. Falling through costs one
	// acquisition and derives closed > cancelled from one view.
	h := New[string, val]()
	h.Sender().Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, h.Sender().SendContext(ctx, "a", val{N: 1}), gobus.ErrClosed)
}

func TestSendContextCancelledOnEmptyHubTakesTheLock(t *testing.T) {
	h := New[string, val]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var locks int
	countLocks(h, &locks)
	require.ErrorIs(t, h.Sender().SendContext(ctx, "a", val{N: 1}), context.Canceled)
	assert.Equal(t, 1, locks, "a cancelled ctx falls through to the lock")
}

// --- Hub.WatchAcross: the receiver bound to every key ---------------------
//
// One slot is the contract here, not an artifact, so these read twice wherever
// a single read would also pass against a per-key structure.

func TestWatchAcrossReceivesEveryKey(t *testing.T) {
	h := New[string, val]()
	rx := h.WatchAcross(h.WithBaseline(val{N: 0}))
	tx := h.Sender()

	require.NoError(t, tx.Send("a", val{N: 1}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 1}})
	require.NoError(t, tx.Send("b", val{N: 2}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "b", Value: val{N: 2}})
}

// TestWatchAcrossHoldsOneSlot is the contract, not an implementation artifact: a
// burst across many keys leaves exactly one pending value, so a consumer whose
// whole reaction is "go re-read the store" wakes once rather than once per key.
//
// The second read is what makes it a statement about cardinality rather than
// about ordering. A per-key structure would satisfy the first assertion too,
// by handing back "c" first and the rest afterwards.
func TestWatchAcrossHoldsOneSlot(t *testing.T) {
	h := New[string, val]()
	rx := h.WatchAcross(h.WithBaseline(val{N: 0}))
	tx := h.Sender()

	require.NoError(t, tx.Send("a", val{N: 1}))
	require.NoError(t, tx.Send("b", val{N: 2}))
	require.NoError(t, tx.Send("c", val{N: 3}))

	assertRecv(t, rx, gobus.Event[string, val]{Key: "c", Value: val{N: 3}})
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "the burst left more than one value")
}

// TestWatchAcrossEventKeyNamesTheValuesKey pins that the key travels with the
// value rather than being the last key sent to. Only an accepted value moves
// it, which is the same rule the value itself follows — a rejected send leaves
// the slot entirely alone.
func TestWatchAcrossEventKeyNamesTheValuesKey(t *testing.T) {
	h := New[string](WithAccept(bySeq))
	rx := h.WatchAcross(h.WithBaseline(val{N: 0, Seq: 5}))
	tx := h.Sender()

	require.NoError(t, tx.Send("a", val{N: 1, Seq: 6}))
	// Rejected: Seq 1 does not beat the slot's 6. If the key were written
	// before Accept ran, or independently of it, this read would name "b".
	require.NoError(t, tx.Send("b", val{N: 2, Seq: 1}))

	assertPeek(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 1, Seq: 6}})
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 1, Seq: 6}})
}

func TestWatchAcrossDoesNotDeliverTheSeed(t *testing.T) {
	// Registration is the snapshot, exactly as for Watch: initial is the
	// caller's own baseline and the prev of the first Accept, never a delivery.
	h := New[string](WithAccept(bySeq))
	rx := h.WatchAcross(h.WithBaseline(val{N: 1, Seq: 5}))
	_, err := rx.TryRecv()
	require.ErrorIs(t, err, gobus.ErrEmpty)

	// ...and it really is the baseline: a value that loses to it is rejected.
	require.NoError(t, h.Sender().Send("a", val{N: 2, Seq: 4}))
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

// TestWatchAcrossCoexistsWithSingleKeyReceivers pins both directions of the
// routing: a send reaches the key's own watchers *and* every wildcard, and a
// single-key receiver is unaffected by the wildcard's presence.
func TestWatchAcrossCoexistsWithSingleKeyReceivers(t *testing.T) {
	h := New[string, val]()
	all := h.WatchAcross(h.WithBaseline(val{N: 0}))
	a := h.Watch("a", h.WithBaseline(val{N: 0}))
	tx := h.Sender()

	require.NoError(t, tx.Send("a", val{N: 1}))
	assertRecv(t, a, gobus.Event[string, val]{Key: "a", Value: val{N: 1}})
	assertRecv(t, all, gobus.Event[string, val]{Key: "a", Value: val{N: 1}})

	require.NoError(t, tx.Send("b", val{N: 2}))
	assertRecv(t, all, gobus.Event[string, val]{Key: "b", Value: val{N: 2}})
	_, err := a.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "a wildcard receiver widened a single-key one")
}

// TestWatchAcrossPinsNoKey holds the R5 guarantee up against the new receiver
// kind: a key costs nothing once its last *watcher* has gone, and a wildcard
// receiver is not a watcher of any particular key. Were it indexed under one —
// its zero K, say — that key would be pinned for the receiver's whole life and
// every send to it would fan out twice.
func TestWatchAcrossPinsNoKey(t *testing.T) {
	h := New[string, val]()
	all := h.WatchAcross(h.WithBaseline(val{N: 0}))
	require.Equal(t, 1, h.forTestingReceiverCount())
	assert.Zero(t, h.forTestingKeyCount(), "a wildcard receiver pinned a key")

	a := h.Watch("a", h.WithBaseline(val{N: 0}))
	require.Equal(t, 1, h.forTestingKeyCount())
	a.Close()
	assert.Zero(t, h.forTestingKeyCount(), "the wildcard held a's state open")

	all.Close()
	assert.Zero(t, h.forTestingReceiverCount())
}

// TestWatchAcrossCloseDeregistersFromTheWildcardSet is the leak a receiver-count
// assertion cannot see. A wildcard receiver left in s.wildcard is still offered
// every value and still holds its slot, while every read on the handle reports
// ErrClosed — so nothing observable through the handle would fail.
func TestWatchAcrossCloseDeregistersFromTheWildcardSet(t *testing.T) {
	h := New[string, val]()
	rx := h.WatchAcross(h.WithBaseline(val{N: 0}))
	require.Equal(t, 1, h.forTestingWildcardCount())

	rx.Close()
	assert.Zero(t, h.forTestingWildcardCount())
	assert.Zero(t, h.forTestingReceiverCount())
	assert.Zero(t, h.forTestingLiveReceivers(), "the send fast path still sees a receiver")

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.NotPanics(t, rx.Close)
}

// TestWatchAcrossTerminalReadDeregisters is the other exit path: a receiver that
// reaches ErrClosed by draining owes the same tear-down as one that is closed.
func TestWatchAcrossTerminalReadDeregisters(t *testing.T) {
	h := New[string, val]()
	rx := h.WatchAcross(h.WithBaseline(val{N: 0}))
	tx := h.Sender()
	require.NoError(t, tx.Send("a", val{N: 1}))
	tx.Close()

	// Sender.Close is the soft path, so the unread value comes back once...
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 1}})
	_, err := rx.TryRecv()
	require.ErrorIs(t, err, gobus.ErrClosed)

	assert.Zero(t, h.forTestingWildcardCount())
	assert.Zero(t, h.forTestingReceiverCount())
}

func TestWatchAcrossAfterHubCloseIsPreClosed(t *testing.T) {
	h := New[string, val]()
	h.Close()
	rx := h.WatchAcross(h.WithBaseline(val{N: 1}))
	require.NotNil(t, rx, "a pre-closed handle, never nil")
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestHubCloseTearsDownWildcardReceivers(t *testing.T) {
	h := New[string, val]()
	rx := h.WatchAcross(h.WithBaseline(val{N: 0}))
	require.NoError(t, h.Sender().Send("a", val{N: 1}))

	h.Close() // hard tear-down: no drain, even with a value unread
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.ErrorIs(t, h.Sender().Send("a", val{N: 2}), gobus.ErrClosed)
}

// TestWatchAcrossCountsTowardTheSendFastPath keeps the wildcard set from becoming
// a second population the lock-free count does not know about. A hub whose only
// receiver is a wildcard must not read as idle, or every send to it returns nil
// and publishes nothing.
func TestWatchAcrossCountsTowardTheSendFastPath(t *testing.T) {
	h := New[string, val]()
	require.Zero(t, h.forTestingLiveReceivers())

	rx := h.WatchAcross(h.WithBaseline(val{N: 0}))
	require.Equal(t, int64(1), h.forTestingLiveReceivers())
	require.NoError(t, h.Sender().Send("a", val{N: 1}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 1}})
}

// TestWatchAcrossSenderCloseIsSafeConcurrentWithSend re-runs the promise
// Sender.Close makes against the new receiver kind, through the same seam. The
// promise is beehive's, cited by name, and the wildcard fan-out is new code on
// the path it covers.
func TestWatchAcrossSenderCloseIsSafeConcurrentWithSend(t *testing.T) {
	h := New[string, val]()
	tx := h.Sender()
	rx := h.WatchAcross(h.WithBaseline(val{N: 1}))

	h.s.forTestingBeforeSendLock = func() { tx.Close() }
	assert.ErrorIs(t, tx.Send("a", val{N: 2}), gobus.ErrClosed)
	h.s.forTestingBeforeSendLock = nil

	// Terminal at once, which is only true if nothing was published.
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

// TestWatchAcrossChanDeliversAcrossKeys covers the feeder path, which builds its
// event from the same slot the reading paths do — so the key it carries has to
// move with the value there too.
func TestWatchAcrossChanDeliversAcrossKeys(t *testing.T) {
	h := New[string, val]()
	rx := h.WatchAcross(h.WithBaseline(val{N: 0}))
	defer rx.Close()
	ch := rx.Chan()
	tx := h.Sender()

	require.NoError(t, tx.Send("a", val{N: 1}))
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 1}}, <-ch)
	require.NoError(t, tx.Send("b", val{N: 2}))
	assert.Equal(t, gobus.Event[string, val]{Key: "b", Value: val{N: 2}}, <-ch)
}

// TestWatchAcrossRecvBlocksUntilAnyKeyLands pins the wake-up: a wildcard receiver
// parked in Recv is woken by a send to a key it never named.
func TestWatchAcrossRecvBlocksUntilAnyKeyLands(t *testing.T) {
	h := New[string, val]()
	rx := h.WatchAcross(h.WithBaseline(val{N: 0}))
	tx := h.Sender()

	evCh := make(chan gobus.Event[string, val], 1)
	go func() {
		ev, err := rx.Recv()
		require.NoError(t, err)
		evCh <- ev
	}()
	waitParked(t, rx, 1)

	require.NoError(t, tx.Send("z", val{N: 9}))
	assert.Equal(t, gobus.Event[string, val]{Key: "z", Value: val{N: 9}}, <-evCh)
}
