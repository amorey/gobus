# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`gobus` (module `github.com/amorey/gobus`) is a small Go library of higher-level
*event bus* architectures. It is the sister library to
[`gochan`](https://github.com/amorey/gochan), which supplies lower-level channel
architectures for anonymous values. The distinction drives every design decision
here: on a gobus every value travels under a **key**, and each bus type defines
its own policy for what happens when several values for the same key are in
flight at once. Requires Go 1.21+.

Currently one bus type: `conflate`.

See `README.md` for the full public API reference (constructors, methods,
semantics tables, error meanings). When changing public behavior, update
`README.md` in the same change.

## Commands

```console
go test ./...                        # all tests
go test -race ./...                  # what CI runs
go test -run TestAnnihilation ./conflate   # a single test
go run ./conflate/examples/recv      # runnable examples
```

Lint gates, all of which CI enforces separately:

```console
test -z "$(gofmt -l .)"
go vet ./...
staticcheck -checks=all ./...        # go install honnef.co/go/tools/cmd/staticcheck@latest
```

CI additionally enforces **100% coverage on library packages** (examples
excluded) by merging per-package profiles built with `-coverpkg=./...`. A new
exported symbol without a test will fail the build.

The CI test matrix runs Go 1.21–1.26. To verify the 1.21 floor locally, install
a real toolchain (`go install golang.org/dl/go1.21.13@latest && go1.21.13
download`) — a modern toolchain alone does not prove it, since only the language
version is gated by the `go` directive. On current macOS, Go 1.21 binaries abort
with `dyld: missing LC_UUID load command` because 1.21's internal linker omits
that command; add `-ldflags=-linkmode=external` to run them. That is an
environment issue, not a code issue, and does not affect CI (Linux).

## Layout

- `gobus.go` — common `Sender[K, V]` and `Receiver[K, V]` interfaces every
  package's handles implement, plus `Event[K, V]`. There is intentionally no
  shared `Hub` interface — each bus package exposes its own concrete
  `*Hub[K, V]` so callers can't accidentally substitute one architecture for
  another.
- `errors.go` — shared sentinel errors: `ErrClosed`, `ErrEmpty`, `ErrFull`.
  `conflate` never returns `ErrFull`; it is reserved for future bounded buses.
- `conflate/` — keyed latest-value fan-out. `New` returns `*Hub[K, V]`; handles
  come from `hub.Sender()` and `hub.Receiver(opts...)`.
- `internal/buscore/` — shared building blocks, not part of the public API.
  Currently `CloseOnce` (atomic flag + done channel), used for the lock-free
  closed pre-check on every receive path. Prefer extending `buscore` over
  duplicating select/close logic across bus packages.

## Architecture

**`Event` is the single currency of the receive side.** `Recv`, `TryRecv`,
`RecvContext` and `Chan` all deal in `Event[K, V]`, so one
`func(gobus.Event[K, V])` handler serves every path. The send side stays
unpacked (`Send(k, v)`) because a publisher already holds the key and value
separately. `conflate/conflate_test.go` pins this with
`TestEventIsTheSingleCurrency`; don't re-split the receive signatures.

**One mutex guards every receiver.** `conflate.shared.mu` is the single lock for
the whole hub. `Send` fans a write across all receivers under it, and each
receiver pops from its own queue under the same lock. This is deliberate — it
keeps enqueue/coalesce/pop consistent without per-receiver locking races. Any
new operation belongs under `s.mu` too.

**Coalescing happens at Send, not at Recv.** Each receiver owns three
structures: `order` (a `list.List` of keys in first-touch order), `elems`
(key → its list element, for O(1) coalesce/remove) and `pending`
(key → latest undelivered value). `enqueueLocked` either appends a new key or
merges into the existing slot via `Merge`, leaving queue position unchanged. A
`Merge` returning `keep == false` annihilates the key from all three. Reads are
therefore a plain pop (`popLocked`). This is what bounds a receiver's memory by
the live key set rather than write volume.

**Two receive paths share that pop.** `recvLoop` (backing `Recv`/`RecvContext`)
parks on a per-receiver `notify` channel that `signalLocked` closes-and-replaces;
`feed` is a per-receiver goroutine backing `Chan`. Both must stay consistent —
a change to one usually needs the mirror change in the other.

**Nil means default on a receiver's policy fields.** `keep == nil` accepts all
keys; `merge == nil` falls back to the hub's shared merge (always non-nil, since
`New` panics otherwise).

**Close has three distinct meanings**, and tests depend on all three:
`Sender.Close()` is a soft drain (receivers see pending values, then
`ErrClosed`), `Hub.Close()` is hard tear-down with no drain, `Receiver.Close()`
affects one handle. A receiver that reaches terminal `ErrClosed` deregisters
itself so a long-lived hub doesn't pin drained receivers.

## Conventions

Mirror `gochan`'s conventions unless there's a reason not to.

- Policy is explicit, never implicit. `New`, `WithKeyFilter` and `WithMerge`
  panic on a nil function rather than substituting a default — the coalescing
  policy is the point of the bus. A nil option passed to `Receiver` panics too.
- Per-receiver options are **methods on the Hub** (`hub.WithKeyFilter(...)`),
  not package-level functions. This is forced by generics: package-level
  `WithKeyFilter[K, V](func(K) bool)` cannot infer `V`, and
  `WithMerge[K, V](Merge[V])` cannot infer `K`, so callers would have to spell
  out both type arguments at every call site. Taking the option off the hub
  fixes `K` and `V` — call sites need no type arguments, and an option built
  from a differently-typed hub is a compile error. Prefer this shape for any
  future per-handle options rather than growing a constructor per combination.
- `ReceiverOption` is a plain func type over an unexported config, not a sealed
  interface. The unexported parameter type already closes the option set (outside
  code cannot name it), and the interface form trips a staticcheck U1000 false
  positive on generic methods reached only through an interface — which would
  need a `//lint:ignore` to get past `-checks=all`.
- Handle `Close()` is always idempotent. `Hub.Close()` is equivalent to closing
  every handle it has handed out and is hard tear-down; `Sender.Close()` is the
  soft path.
- `Receiver.Chan()` is a per-receiver private channel driven by a per-receiver
  feeder goroutine. `Receiver.Close()` *does* close it, and sender-close closes
  it after the feeder drains. Abandoning the channel without closing the receiver
  leaks the feeder.
- Lock-free closed checks are always re-checked under `s.mu` before handing back
  a value. `Close` serializes through the same mutex, so that re-check is what
  makes "close wins the race" correct rather than best-effort. Every such
  re-check has a `forTesting*` hook so the race can be exercised
  deterministically.
- Don't call `Hub.Close` concurrently with an active `Send` from another
  goroutine — it inherits the sender's close discipline.

## Testing

- Use [testify](https://github.com/stretchr/testify) (`assert` and `require`).
- Do not use magic sleeps (`time.Sleep`, or `time.After` timeouts whose duration
  encodes an assumption about scheduling) to coordinate goroutines or "wait for"
  state changes. Synchronize through channels or observable state instead.
  Prefer `TryRecv() == ErrEmpty` over a timeout to assert "nothing is pending",
  and the `waitParked` helper (spins on the receiver's `waiters` count) to
  guarantee a reader is parked in its blocking select before firing a
  `Send`/`Close` at it.
- Beware `select` arms that are both ready — e.g. the feeder's
  "deliver vs. closed" select. A test that has a reader waiting on the channel
  makes the choice random; use the feeder's exit hook to sequence the test
  instead. `TestChanFeederCloseWhileDelivering` is the worked example.
- Assert whole `Event` values rather than key and value separately, so the
  pairing itself is pinned (`assertRecv`).
- Test-only helpers live in `conflate/helpers_test.go` (`lenForTest`,
  `forTestingReceiverCount`).
