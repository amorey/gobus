package watch

import (
	"testing"

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
