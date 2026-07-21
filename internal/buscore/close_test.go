package buscore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloseOnce(t *testing.T) {
	c := NewCloseOnce()
	assert.False(t, c.IsClosed())

	select {
	case <-c.Done():
		t.Fatal("done should not be closed before Close")
	default:
	}

	assert.True(t, c.Close(), "first Close should report that it performed the close")
	assert.True(t, c.IsClosed())
	<-c.Done()

	assert.False(t, c.Close(), "second Close should be a no-op")
}

func TestCloseOnceInitEmbedded(t *testing.T) {
	var s struct{ done CloseOnce }
	s.done.Init()
	assert.False(t, s.done.IsClosed())
	assert.True(t, s.done.Close())
	<-s.done.Done()
}
