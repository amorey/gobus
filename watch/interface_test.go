package watch_test

import (
	"github.com/amorey/gobus"
	"github.com/amorey/gobus/watch"
)

// The handles implement the module-wide interfaces. A compile-time assertion,
// so a signature drift fails the build rather than the conformance suite.
var (
	_ gobus.Sender[string, int]   = (*watch.Sender[string, int])(nil)
	_ gobus.Receiver[string, int] = (*watch.Receiver[string, int])(nil)
)
