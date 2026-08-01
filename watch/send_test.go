package watch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gobus"
)

// assertRecv pins the whole Event, so the key/value pairing is checked too.
func assertRecv(t *testing.T, rx *Receiver[string, val], want gobus.Event[string, val]) {
	t.Helper()
	ev, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, want, ev)
}

func TestSendReachesTheWatchingReceiver(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	require.NoError(t, h.Sender().Send("a", val{N: 2}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
}

func TestSendForAnUnwatchedKeyIsDropped(t *testing.T) {
	// R43c: no receiver means no buffer. A later Watch never sees it.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	require.NoError(t, h.Sender().Send("b", val{N: 9}))

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
	assert.Equal(t, 1, h.forTestingKeyCount(), "b was never retained")
}

func TestSendOnlyTouchesItsOwnKey(t *testing.T) {
	h := New[string, val]()
	a := h.Watch("a", val{N: 1})
	b := h.Watch("b", val{N: 1})
	require.NoError(t, h.Sender().Send("a", val{N: 2}))

	assertRecv(t, a, gobus.Event[string, val]{Key: "a", Value: val{N: 2}})
	_, err := b.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestAcceptRejectsAValueAndTheReceiverNeverLearns(t *testing.T) {
	// R9: a false result changes nothing, silently.
	h := New[string](WithAccept(bySeq))
	rx := h.Watch("a", val{N: 1, Seq: 5})
	require.NoError(t, h.Sender().Send("a", val{N: 2, Seq: 4}))

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestAcceptRunsPerReceiverAgainstItsOwnSlot(t *testing.T) {
	// R10: two receivers of one key seeded at different moments. One value is
	// new for the older seed and stale for the newer one.
	h := New[string](WithAccept(bySeq))
	behind := h.Watch("k", val{N: 1, Seq: 3})
	ahead := h.Watch("k", val{N: 1, Seq: 7})

	require.NoError(t, h.Sender().Send("k", val{N: 2, Seq: 5}))

	assertRecv(t, behind, gobus.Event[string, val]{Key: "k", Value: val{N: 2, Seq: 5}})
	_, err := ahead.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "5 is stale against a seed of 7")
}

func TestTheDefaultAcceptTakesEveryValue(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1, Seq: 9})
	require.NoError(t, h.Sender().Send("a", val{N: 2, Seq: 1}))
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 2, Seq: 1}})
}

func TestAnUnreadValueIsOverwrittenNotQueued(t *testing.T) {
	// R26: a slow reader skips to the current value.
	h := New[string, val]()
	rx := h.Watch("a", val{N: 0})
	for i := 1; i <= 3; i++ {
		require.NoError(t, h.Sender().Send("a", val{N: i}))
	}
	assertRecv(t, rx, gobus.Event[string, val]{Key: "a", Value: val{N: 3}})

	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "one slot, not a queue")
}

func TestTrySendIsSend(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
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
	first := h.Watch("k", val{N: 0})
	second := h.Watch("k", val{N: 0})

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
