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

func TestPoisonSurvivesASyncRacingIt(t *testing.T) {
	// Sync is a read-modify-write. A Poison landing between its two halves
	// must not be overwritten: if it were, the count would read as idle on a
	// closed bus and every later send would return nil instead of ErrClosed —
	// an unrecoverable drop, since these buses have no replay.
	//
	// The hook lands the poison inside that window deterministically, rather
	// than hammering and hoping to hit it.
	var c LiveCount
	c.Sync(1)
	c.forTestingBeforeStore = func() {
		c.forTestingBeforeStore = nil
		c.Poison()
	}

	c.Sync(0)
	assert.False(t, c.Idle(), "a Sync racing Poison cleared it")
	assert.EqualValues(t, poisoned, c.Load())
}
