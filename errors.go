// Package gobus provides specialized event bus architectures for Go.
//
// Where [github.com/amorey/gochan] supplies channel architectures that move
// anonymous values between goroutines, gobus supplies *keyed* architectures:
// every value travels under a key, and each bus type defines its own policy
// for what happens when several values for the same key are in flight at once.
//
// Sentinel errors defined here are shared across all subpackages.
package gobus

import "errors"

var (
	ErrClosed = errors.New("gobus: bus closed")
	ErrEmpty  = errors.New("gobus: no pending events")
	ErrFull   = errors.New("gobus: bus full")
)
