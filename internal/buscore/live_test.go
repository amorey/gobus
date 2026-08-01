package buscore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLiveCountStartsIdle(t *testing.T) {
	var c LiveCount
	assert.True(t, c.Idle())
	assert.EqualValues(t, 0, c.Load())
}

func TestLiveCountSyncTracksTheTruth(t *testing.T) {
	var c LiveCount
	c.Sync(2)
	assert.False(t, c.Idle())
	assert.EqualValues(t, 2, c.Load())

	c.Sync(0)
	assert.True(t, c.Idle())
}

func TestPoisonIsNotIdleAndNeverClears(t *testing.T) {
	// The whole point: a receiver leaving after the sender closed must not
	// write a zero over the poison and re-open the fast path.
	var c LiveCount
	c.Sync(1)
	c.Poison()
	assert.False(t, c.Idle())

	c.Sync(0)
	assert.False(t, c.Idle(), "sync must not clear the poison")
	assert.EqualValues(t, poisoned, c.Load())
}
