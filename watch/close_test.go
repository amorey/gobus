package watch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gobus"
)

func TestSenderCloseDrainsThenErrClosed(t *testing.T) {
	// R38: the unread value survives the soft close.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	h.Sender().Close()

	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestSenderCloseWithNothingUnreadIsTerminalAtOnce(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
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

func TestSenderCloseWakesAParkedRecv(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})

	done := make(chan error, 1)
	go func() {
		_, err := rx.Recv()
		done <- err
	}()

	waitParked(t, rx, 1)
	h.Sender().Close()
	assert.ErrorIs(t, <-done, gobus.ErrClosed)
}

func TestTerminalErrClosedDropsTheKey(t *testing.T) {
	// R41 and R43: a receiver leaves the hub by draining, not only by Close.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	h.Sender().Close()

	_, err := rx.TryRecv()
	require.ErrorIs(t, err, gobus.ErrClosed)
	assert.Equal(t, 0, h.forTestingReceiverCount())
	assert.Equal(t, 0, h.forTestingKeyCount(), "R5 holds on both exit paths")
}

func TestTerminalErrClosedFromRecvDropsTheKey(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	h.Sender().Close()

	_, err := rx.Recv()
	require.ErrorIs(t, err, gobus.ErrClosed)
	assert.Equal(t, 0, h.forTestingKeyCount())
}

func TestHubCloseIsHardTearDown(t *testing.T) {
	// R39: no drain, even with a value unread.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	h.Close()

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.ErrorIs(t, h.Sender().Send("a", val{N: 3}), gobus.ErrClosed)
	assert.NotPanics(t, h.Close)
}

func TestHubCloseWakesAParkedRecv(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})

	done := make(chan error, 1)
	go func() {
		_, err := rx.Recv()
		done <- err
	}()

	waitParked(t, rx, 1)
	h.Close()
	assert.ErrorIs(t, <-done, gobus.ErrClosed)
}

func TestWatchAfterSenderCloseIsLiveButTerminal(t *testing.T) {
	// R43a: unlike Hub.Close, the handle is not pre-closed. It holds nothing
	// unread, so its first read is terminal.
	h := New[string, val]()
	h.Sender().Close()
	rx := h.Watch("a", val{N: 1})
	require.False(t, rx.done.IsClosed(), "live handle, not pre-closed")

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.Equal(t, 0, h.forTestingKeyCount())
}

func TestAnIdleReceiverPinsItsKey(t *testing.T) {
	// R43b: R5 is a guarantee about closed receivers, not idle ones.
	h := New[string, val]()
	_ = h.Watch("a", val{N: 1})
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
	rx := h.Watch("a", val{N: 1})
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	rx.forTestingBeforeTryRecvLock = func() { rx.Close() }
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestCloseRaceIsResolvedUnderTheLockForRecv(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	rx.forTestingBeforeRecvLock = func() { rx.Close() }
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}
