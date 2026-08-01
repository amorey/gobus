package watch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gobus"
)

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
	rx := h.Watch("a", val{N: 0})
	defer rx.Close()

	var locks int
	countLocks(h, &locks)
	require.NoError(t, h.Sender().Send("a", val{N: 1}))
	assert.Equal(t, 1, locks)
}

func TestTheFastPathReturnsAfterTheLastReceiverLeaves(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 0})
	require.EqualValues(t, 1, h.forTestingLiveReceivers())

	rx.Close()
	assert.EqualValues(t, 0, h.forTestingLiveReceivers())

	var locks int
	countLocks(h, &locks)
	require.NoError(t, h.Sender().Send("a", val{N: 1}))
	assert.Zero(t, locks)
}

func TestSenderCloseKeepsClosedHubsOnTheLockedPath(t *testing.T) {
	// The poison is what makes ErrClosed durable: a Receiver.Close after a
	// Sender.Close must not write a zero over it and let the fast path answer
	// nil where ErrClosed is the answer.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 0})
	h.Sender().Close()
	rx.Close()

	assert.ErrorIs(t, h.Sender().Send("a", val{N: 1}), gobus.ErrClosed)
}

func TestHubCloseKeepsClosedHubsOnTheLockedPath(t *testing.T) {
	h := New[string, val]()
	_ = h.Watch("a", val{N: 0})
	h.Close()

	assert.ErrorIs(t, h.Sender().Send("a", val{N: 1}), gobus.ErrClosed)
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
