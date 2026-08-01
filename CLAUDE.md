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

Currently two bus types: `conflate` (keyed event bus) and `watch` (keyed
state bus).

See `README.md` for the full public API reference (constructors, methods,
semantics tables, error meanings). When changing public behavior, update
`README.md` in the same change.

## Commands

```console
go test ./...                        # all tests
go test -race ./...                  # what CI runs
go test -run TestAnnihilation ./conflate   # a single test
go run ./conflate/examples/recv      # runnable examples
go run ./watch/examples/chan
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

- `conformance_test.go` (root, `package gobus_test`) — the cross-architecture
  suite. It drives handles through the shared interfaces, not concrete types,
  and pins the close/cancel/value precedence the interface docs promise on
  every bus type's behalf. A new bus package means a new row in
  `architectures`; if it can't pass, the interface doc is wrong and has to
  change with it. This is the only place a bus that resolves the ordering its
  own way fails — per-package tests can't see it, since such a bus still looks
  internally consistent.
- `gobus.go` — common `Sender[K, V]` and `Receiver[K, V]` interfaces every
  package's handles implement, plus `Event[K, V]`. These doc comments are the
  module-wide contract, not a summary of `conflate`: a change to public
  close/cancel behavior updates `gobus.go`, `README.md` and the conformance
  suite, not just the package. There is intentionally no
  shared `Hub` interface — each bus package exposes its own concrete
  `*Hub[K, V]` so callers can't accidentally substitute one architecture for
  another.
- `errors.go` — shared sentinel errors: `ErrClosed`, `ErrEmpty`, `ErrFull`.
  `conflate` never returns `ErrFull`; it is reserved for future bounded buses.
- `conflate/` — keyed latest-value fan-out. `New` returns `*Hub[K, V]`; handles
  come from `hub.Sender()` and `hub.Receiver(opts...)`.
- `watch/` — keyed latest-value **state** bus. One receiver watches one key,
  seeded by the caller at `hub.Watch(k, initial)`; `Receiver.Close` is the
  unwatch. A caller `Accept(prev, next) bool` decides which of two values wins,
  evaluated per receiver against that receiver's own slot. See
  `docs/adr/2026-08-01-watch-keyed-state-bus.md` for why it exists and what was
  rejected on the way.
- `internal/buscore/` — shared building blocks, not part of the public API.
  `CloseOnce` (atomic flag + done channel), used for the lock-free closed
  pre-check on every receive path, and `LiveCount` (the poisoned send-fast-path
  counter both buses gate `Send` on). Prefer extending `buscore` over
  duplicating select/close logic across bus packages — `LiveCount` exists
  because the second bus package would otherwise have copied the poison
  invariant and its rationale.

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

**The send side skips the lock when nobody is listening.**
`shared.liveReceivers` is a lock-free copy of `len(s.receivers)`, written under
`s.mu` at every site that mutates the map (`receiver`, `deregisterLocked`) and
read without it by `Send`/`SendContext`. Zero means "no receiver, send side
open" and is the only state in which a publisher may return early.
`syncLiveLocked` *derives* the count rather than incrementing it, because only
one direction of error is safe: over-reporting costs one uncontended lock,
while under-reporting drops a value permanently — a conflated bus has no retry,
so the next `Send` for that key coalesces into a slot the subscriber was never
told had been skipped. `Sender.Close` and `Hub.Close` store `sendPoisoned`, and
`syncLiveLocked`'s early return is what holds it: without that guard a
`Receiver.Close` *after* a `Sender.Close` writes a zero over the poison and the
fast path answers `nil` where `ErrClosed` is the durable answer.
`TestSenderCloseDrainsThenErrClosed` already catches that, which is why the
poison and the fast path cannot land in separate commits. The subscriber-side
corollary — register before snapshotting — is stated in `Send`'s doc comment
because a publisher that skips the lock cannot notice a late subscriber.
`SendContext`'s fast path answers **only `nil`**, and a cancelled `ctx` falls
through to the lock: the count and `ctxDone` are two reads at two moments, so a
`Sender.Close` landing between them would make `ctx.Err()` right at neither
(`nil` at the load, `ErrClosed` by the select) and would reverse closed >
cancelled for a caller that ordered the close first. Only `sendLocked` reads
both under one acquisition, which is where that precedence has to be derived.
`TestSendContextCancelledOnEmptyHubStillLosesToClose` pins it.

**Two receive paths share that pop.** `recvLoop` (backing `Recv`/`RecvContext`)
parks on a per-receiver `notify` channel that `signalLocked` closes-and-replaces;
`feed` is a per-receiver goroutine backing `Chan`. Both must stay consistent —
a change to one usually needs the mirror change in the other.

**Close/cancel precedence is one ordered run under `s.mu`.** `recvLoop`
resolves **closed > cancelled > value**: receiver/hub closed, then
sender-closed-and-drained (`txClosed && order.Len() == 0`, which is exactly
when `popLocked` would fail), then the `ctx` check, then the pop. The
cancellation check must stay *above* the pop — otherwise the only cancellation
arm is the parked `<-ctxDone`, and a receiver looping on `RecvContext` against
a fast publisher never observes its own shutdown. The terminal exits carry a
tear-down obligation (`delete(s.receivers, rx)`) that has to happen under the
lock that decided they were terminal, so don't hoist any of them into the
lock-free probe. The parking select's arms are deliberately **empty** — every
wake falls through to the loop top so that ordered run is the only place a
verdict is produced; an arm that returned `ErrClosed`/`ctx.Err()` itself would
let a close racing a cancellation resolve by select roulette and skip the
terminal deregistration. `SendContext` is the send-side twin: closed >
cancelled, both tests under one acquisition of `s.mu`, delegating the fan-out
to `sendLocked` — so the only bus state read outside the lock is
`liveReceivers` (see the send fast path below), and `txClosed` stays a plain
`bool` because the poison keeps every closed hub on the locked path where it is
read. What is *not* done under `s.mu` on either side is calling into
the caller's `context.Context`: `sendLocked` and `recvLoop` both take an
already-obtained `Done` channel into the locked region and resolve `ctx.Err()`
only after releasing, because a user-supplied context's methods are arbitrary
code that may take application locks — inverting the lock order against any
goroutine that enters the bus while holding them. `sendLocked` therefore
*reports* cancellation (`cancelled bool`) rather than resolving it, and its
locked region sits in a closure with a deferred unlock — the caller's `Merge`
and key filter run under `s.mu`, and a panic out of either must still release
it or a recovering caller finds the hub wedged. The `ctx` check being *under*
`s.mu` rather than an entry-time snapshot is deliberate and pinned by
`TestSendContextChecksCancellationAtTheLockNotAtEntry` (via
`forTestingBeforeSendLock`): nothing is published on behalf of a context that
expired while the send waited for the lock, which keeps every verdict in the
package derived from state read at the decision point. Read it before
"restoring" the pre-check — the entry-snapshot form still passes every other
test. gochan makes `txClosed` an `atomic.Bool` because its
`watch`/`broadcast` read it from a lock-free `TryRecv` fast path; conflate's two
lock-free pre-checks are `rx.done` and `liveReceivers`, and neither needs it —
`rx.done` is its own atomic, and `liveReceivers` carries closedness in its
poison, so a closed hub always reaches the locked read. Copying the atomic here
would buy nothing and give one field two access disciplines.

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

- Policy is explicit, never implicit. A nil *function* passed to an option
  constructor panics rather than being replaced by a default, and a nil
  *option* passed to a constructor panics too (`conflate.Hub.Receiver`,
  `watch.New`). `conflate.New` goes further and requires its `Merge`, because
  coalescing is the point of that bus; `watch` lets `Accept` be omitted,
  because a state bus has a meaningful identity rule (last-writer-wins) and
  omitting the option is then a statement rather than an oversight. Supplying
  a nil one is still a panic.
- Per-receiver options are **methods on the Hub** (`hub.WithKeyFilter(...)`),
  not package-level functions. Hub-*construction* options cannot be, since
  there is no hub yet — `watch.WithAccept` is package-level for that reason,
  and it works without type-argument noise only because `watch.Option[V]`
  carries `V` alone. Adding a `K`-dependent hub option would force both type
  arguments at every call site; don't, without meaning to. This is forced by
  generics: package-level
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
  deterministically. The send fast path is not an exception: `liveReceivers` is
  a no-op check, not a closed check — it asks whether there is any work, never
  whether the bus is alive — and the poison is what keeps closedness out of it,
  so a closed hub is never resolved without the lock. It hands back no value, so
  there is nothing to re-check. The hook rule still applies:
  `forTestingBeforeSendLock` runs on every send path that is about to take
  `s.mu` and on neither fast path, which is how a test proves the lock was
  skipped without timing.
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
  `recvLoop`'s parking select needs no such seam, and deliberately has none:
  its arms are empty, so the wake cannot decide anything and there is no
  outcome to sequence. To fuse two terminations there, hold `s.mu` while
  arming both — the woken reader cannot pass `s.mu.Lock()` until you release
  it, so both are visible whenever it derives its answer, regardless of
  scheduling. `TestParkedCloseAndCancelResolveToClosed` is the worked example.
  Arm the *context* first: signalling `notify` first lets the reader leave the
  select before `ctxDone` exists, so the arm under test is never taken and the
  test passes while covering nothing. Verify by mutation, not by green.
- Assert whole `Event` values rather than key and value separately, so the
  pairing itself is pinned (`assertRecv`).
- Test-only helpers live in `conflate/helpers_test.go` (`lenForTest`,
  `forTestingReceiverCount`).
