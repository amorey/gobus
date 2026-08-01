package watch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gobus"
)

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
	rx := h.Watch("a", val{N: 1})
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	ev, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, ev)
}

func TestRecvBlocksUntilASendLands(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})

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
	rx := h.Watch("a", val{N: 1, Seq: 5})

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
	rx := h.Watch("a", val{N: 1})
	rx.Close()

	_, err := rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestReceiverCloseWakesAParkedRecv(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})

	done := make(chan error, 1)
	go func() {
		_, err := rx.Recv()
		done <- err
	}()

	waitParked(t, rx, 1)
	rx.Close()
	assert.ErrorIs(t, <-done, gobus.ErrClosed)
}

func TestParkedReadersLeaveNoWaiterBehind(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})

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
