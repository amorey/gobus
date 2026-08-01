# Give `watch` a `Peek`, and keep it off the shared interface

- **Status:** accepted, implemented
- **Date:** 2026-08-01
- **Scope:** `github.com/amorey/gobus`, package `watch`
- **Relation to prior ADRs:** the 2026-07-28 `conflate`-`Peek` decision kept
  `Peek` off `gobus.Receiver` on the grounds that it "may generalize later",
  with a second implementation as the thing that would test that. This is that
  second implementation, and the answer is no. It also amends the 2026-08-01
  keyed-state-bus ADR, which recorded that `watch` ships no `Peek`.

## Context

`watch` receivers hold one slot per handle, unread exactly while
`version > lastSeen`. A consumer that wants to know whether anything is waiting
for it — a poller deciding whether a unit of work is worth starting, a test
asserting a value landed — had only `TryRecv`, which consumes what it reports.
`conflate` already answers that question with `Peek`, and the two buses are
meant to be learnable as a pair.

## Decision

**`watch.Receiver.Peek` is `TryRecv` minus the take.** It runs the same ordered
run under `s.mu` — `terminalLocked`, then `unreadLocked` — and returns
`gobus.ErrEmpty` or `gobus.ErrClosed` exactly where `TryRecv` does. It is not a
read of the key's current state: a caught-up receiver gets `ErrEmpty` though
its slot holds a value, and a closed handle gets `ErrClosed` though one is
waiting. A caller that wants current state on demand keeps its own copy of the
last value read, which the reading goroutine already has and which costs no
lock.

The alternative — "Peek returns the slot, always" — was rejected. It would make
`Peek` the one receive path on either bus whose answer is not derived from the
closed > value precedence the interface docs promise, and it would hand back a
value to a closed handle, which nothing else in the module does.

**A terminal `Peek` deregisters.** The drained verdict owes the same tear-down
whichever path derives it, under the lock that decided it. Skipping it would
let a hub pin a drained receiver, and its key with it, for a consumer that
happens to poll rather than read.

**`Peek` stays off `gobus.Receiver`, and `conformance_test.go` gains no row.**
Not "not yet": the two implementations are non-substitutable in an observable
way. For a value already handed to the `Chan` feeder, conflate's `Peek` reports
`ErrEmpty` — the event has left the queue — while `watch`'s reports the value,
because the feeder marks it read only once the consumer has taken it. That is
queue-versus-slot, not a defect either side can fix. Promoting the method would
force a doc comment that one implementation violates, or one weakened to
"closed > value, in-flight unspecified", which says too little for an
architecture-agnostic call site to use. The close/value precedence *does*
conform on both, and that half is already pinned by each package's own tests.

## Consequences

- Two bus types now have a `Peek` that agrees on precedence and disagrees on
  in-flight values. Both doc comments and the README say so explicitly; a third
  bus type does not reopen the promotion question unless it also removes that
  divergence.
- `eventLocked` is now the single place an `Event` is built from a `watch`
  slot, shared by `Peek`, `takeLocked` and the `Chan` feeder.
- `Peek` takes the hub-wide lock, so polling it in a loop slows every publisher
  and every other receiver. Documented on both bus types; not enforced.
