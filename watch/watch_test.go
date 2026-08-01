package watch

import (
	"testing"

	"github.com/amorey/gobus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	// R19: initial is the caller's own argument. It is the baseline for
	// Accept, not a value to hand back.
	h := New[string](WithAccept(bySeq))
	rx := h.Watch("a", val{N: 1, Seq: 1})
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

func TestTryRecvIsEmptyUntilSomethingChanges(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	for i := 0; i < 3; i++ {
		_, err := rx.TryRecv()
		require.ErrorIs(t, err, gobus.ErrEmpty)
	}
}

func TestReceiverCloseIsTheUnwatch(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	require.Equal(t, 1, h.forTestingReceiverCount())
	require.Equal(t, 1, h.forTestingKeyCount())

	rx.Close()
	assert.Equal(t, 0, h.forTestingReceiverCount())
	// R5: the last receiver for a key takes the key's state with it.
	assert.Equal(t, 0, h.forTestingKeyCount())
}

func TestReceiverCloseIsIdempotent(t *testing.T) {
	h := New[string, val]()
	rx := h.Watch("a", val{N: 1})
	rx.Close()
	assert.NotPanics(t, rx.Close)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

func TestKeyStateSurvivesWhileAnotherReceiverWatches(t *testing.T) {
	h := New[string, val]()
	a := h.Watch("k", val{N: 1})
	b := h.Watch("k", val{N: 1})
	require.Equal(t, 1, h.forTestingKeyCount())

	a.Close()
	assert.Equal(t, 1, h.forTestingKeyCount(), "b still watches k")
	b.Close()
	assert.Equal(t, 0, h.forTestingKeyCount())
}

func TestWatchAfterHubCloseIsPreClosed(t *testing.T) {
	h := New[string, val]()
	h.Close()
	rx := h.Watch("a", val{N: 1})
	require.NotNil(t, rx, "R23: a pre-closed handle, never nil")
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrClosed)
}
