package watch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gobus"
)

func TestRecvContextReturnsAValue(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	ev, err := rx.RecvContext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, ev)
}

func TestRecvContextCancelBeatsAnUnreadValue(t *testing.T) {
	// R36: cancellation outranks a ready value, and does not consume it.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
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
	rx := h.Watch("a", val{N: 1})
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
	rx := h.Watch("a", val{N: 1})
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
	rx := h.Watch("a", val{N: 1})
	rx.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestDrainedSenderCloseBeatsCancelled(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
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
	rx := h.Watch("a", val{N: 1})
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
	rx := h.Watch("a", val{N: 1})
	require.NoError(t, h.Sender().SendContext(context.Background(), "a", val{N: 2}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
}

func TestSendContextCancelledDoesNotPublish(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, h.Sender().SendContext(ctx, "a", val{N: 2}), context.Canceled)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestClosedBeatsCancelledOnTheSendSide(t *testing.T) {
	h := New[string, val]()
	_ = h.Watch("a", val{N: 1})
	h.Sender().Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, h.Sender().SendContext(ctx, "a", val{N: 2}), gobus.ErrClosed)
}

func TestSendContextChecksCancellationAtTheLockNotAtEntry(t *testing.T) {
	// R35: nothing is published for a context that expired while the send was
	// waiting for the lock.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	ctx, cancel := context.WithCancel(context.Background())

	h.s.forTestingBeforeSendLock = cancel
	require.ErrorIs(t, h.Sender().SendContext(ctx, "a", val{N: 2}), context.Canceled)
	h.s.forTestingBeforeSendLock = nil

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}
