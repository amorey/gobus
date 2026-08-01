package watch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gobus"
)

func TestChanDeliversValues(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	defer rx.Close()

	ch := rx.Chan()
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, <-ch)
}

func TestChanIsTheSameChannelEveryTime(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	defer rx.Close()
	assert.Equal(t, rx.Chan(), rx.Chan())
}

func TestChanClosesOnReceiverClose(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	ch := rx.Chan()

	rx.Close()
	_, open := <-ch
	assert.False(t, open)
}

func TestChanClosesAfterSenderCloseDrains(t *testing.T) {
	// R46: the final value first, then the close.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
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
	rx := h.Watch("a", val{N: 1})
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
	rx := h.Watch("a", val{N: 0})
	defer rx.Close()

	// Sequence through the feeder's own hook: it fires after the feeder has
	// snapshotted a value and before it enters the delivery select, so the
	// replacement is deterministic rather than a race with the consumer. Arm
	// it before Chan starts the feeder, or the write races the feeder's read.
	replaced := make(chan struct{})
	rx.forTestingFeederParked = func() {
		rx.forTestingFeederParked = nil
		require.NoError(t, h.Sender().Send("a", val{N: 2}))
		close(replaced)
	}
	ch := rx.Chan()
	require.NoError(t, h.Sender().Send("a", val{N: 1}))

	<-replaced
	assert.Equal(t, gobus.Event[string, val]{Key: "a", Value: val{N: 2}}, <-ch)
}

func TestFeederConvergesOnTheNewestValue(t *testing.T) {
	// R45b: whatever the delivery select picks, a consumer that keeps reading
	// ends on the current value. R45c permits the superseded one to arrive
	// first, so this must not assert that it does not.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 0})
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
	rx := h.Watch("a", val{N: 0})

	exited := make(chan struct{})
	rx.forTestingFeederExit = func() { close(exited) }
	rx.forTestingFeederParked = func() {
		rx.forTestingFeederParked = nil
		rx.Close()
	}
	ch := rx.Chan()
	require.NoError(t, h.Sender().Send("a", val{N: 1}))

	<-exited
	for range ch { // drain whatever the select chose
	}
}

func TestFeederCloseRaceIsResolvedUnderTheLock(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 0})

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
