# Take a conflating receiver's whole backlog as one cut with `TryRecvAll`

- **Status:** accepted, implemented
- **Date:** 2026-08-03
- **Scope:** `github.com/amorey/gobus`, package `conflate`
- **Supersedes:** the `Receiver.Drain` feature request — same method, renamed,
  with the motivation re-grounded on atomicity rather than on lock contention.
  See *Decision* for the name and *Consequences* for the contention trade.

## Context

A consumer that reads a burst as one unit calls `RecvContext` for the first
event, then loops on `TryRecv` until the receiver reports empty, then processes
the batch. That loop is a *sequence of instants*, not one instant: a `Send`
landing between two iterations joins the batch, and a `Send` landing just after
the loop observes empty does not. The caller has no way to ask for "everything
pending as of one instant", and no way to synthesize it — it cannot ask how
many keys are pending, and any answer would be stale before it acted on it.
Only the bus can take that cut, because only the bus holds the lock across the
whole queue.

That gap matters for a conflate consumer specifically. A receiver's queue is
ordered by **first touch**, and a merge leaves a key at its original position
(`enqueueLocked`), so queue order carries no relation to any ordering quantity
inside `V`. A consumer whose `V` carries such a quantity — a log position, a
version, a watermark — must sort the batch it assembles, and a sort is only
sound over a complete set.

Concretely: a pending queue of `A@12, B@11`, where `A@12` coalesced in place
from `A@10`, is safe to checkpoint only if both are taken together and sorted.
A consumer that takes a proper subset, checkpoints `12`, and leaves `B@11` for
the next batch loses `B@11` for good. This is the same silent-skip failure
[the `Peek` ADR](./2026-07-28-conflate-peek.md) exists to prevent, approached
from the other end: `Peek` tells you what is still pending below your batch,
`TryRecvAll` guarantees there is nothing still pending below it.

Two lesser observations informed the shape but were not sufficient to justify
new public surface on their own. A `TryRecv` loop ends when the receiver reports
empty, which a fast enough producer postpones indefinitely — real callers
terminate for reasons that are properties of their producer, not of the bus, so
a reader of the loop cannot see the bound; a cut terminates by construction. And
draining N keys acquires the single hub-wide mutex N times, each acquisition
contending with every publisher's fan-out, which is exactly the spin `Peek`'s
doc comment warns against.

## Decision

Add one accessor to `conflate.Receiver`:

```go
// TryRecvAll pops every pending event without blocking.
func (rx *Receiver[K, V]) TryRecvAll() ([]gobus.Event[K, V], error)
```

`TryRecvAll` is `TryRecv` that takes the whole queue instead of the head, under
one acquisition of `s.mu`. It adds no new state, option, or send-path work — the
same shape as `Peek`.

Named for what it is: `TryRecv`, all of it. `Drain` was rejected — in Go idiom
"drain" more often means *discard* a backlog than return it, and this package
already uses "drained" as a *state* (`drainedLocked`, and the "sender closed and
drained" phrasing throughout the docs), so a `Drain` method and a
`drainedLocked` predicate meaning unrelated things would be a standing
readability hazard.

### Semantics

- **Returns every pending event**, in queue order (first-touch order), and
  empties the queue. The returned slice is freshly allocated and owned by the
  caller.
- **One entry per key.** `order` holds each key once, so a cut cannot contain a
  key twice — the returned slice is safe to build a map from, or to dispatch
  per-key over, without deduping. This is a guarantee of a *single* call and is
  forfeited by composing calls; see *Alternatives considered*.
- **One acquisition of `s.mu`**, spanning the whole queue. That atomicity is the
  contract, not an optimization.
- **Precedence is `TryRecv`'s, unchanged**: closed beats empty beats value.

  | Receiver state | Result |
  | --- | --- |
  | receiver or hub closed | `nil, gobus.ErrClosed` |
  | sender closed **and** queue empty | `nil, gobus.ErrClosed` + deregister |
  | sender closed, queue non-empty | all events, `nil` |
  | queue empty, sender open | `nil, gobus.ErrEmpty` |
  | otherwise | all events, `nil` |

- **Partial results are not a case.** Either it returns a non-empty slice and a
  nil error, or no events and an error. There is never "some values and
  `ErrClosed`". A caller may test `err != nil` and ignore the slice.
- **`ErrClosed` is not a statement that the backlog was empty**, for the same
  reason it isn't on `Peek`: `Hub.Close` and `Receiver.Close` abandon whatever is
  queued, and only `Sender.Close` drains first. A cursor-tracking caller must
  distinguish *why* its loop stopped.
- **`TryRecvAll` orders nothing.** It returns queue order, which is first-touch
  order. The caller still sorts. What it supplies is the *set*, which is what
  makes the caller's sort sound.
- **`Chan` interaction is unchanged.** An event already handed to the feeder has
  left the queue, so `TryRecvAll` does not see it — the same rule `Peek` states,
  and the same in-flight edge.
- **Single-consumer-meaningful, any-goroutine-safe**, as for every other receive
  path.

### Doc comment obligations

Three things are stated in the doc comment because nothing else records them:

1. It returns first-touch order, which carries **no** relation to any ordering
   quantity inside `V`. A caller that needs value order sorts.
2. The returned slice holds **one entry per key**, and that holds for one call
   only — a batch assembled from more than one receive can repeat a key.
3. The critical section is O(live keys) and runs **no caller code** — no `Merge`,
   no key filter. That is what makes holding `s.mu` across the whole queue
   acceptable, and it is an invariant future changes must preserve.

## Consequences

**Fewer lock acquisitions, held longer.** `popAllLocked` walks the queue, does
one map lookup per key, clears both maps, and allocates the result slice — all
under `s.mu`, and `Event` values are copied by value, so a large `V` or a large
live key set makes the critical section correspondingly long. Amortized total
cost goes down; worst-case *publisher* latency goes **up**. That is a different
contention profile, not a strict improvement, and it is described that way
wherever it is mentioned. None of it has been measured and no correctness claim
rests on it, which is why it is not the headline motive in the doc comment or
the README.

The allocation cannot reasonably be hoisted out of the lock — `n` is only known
under it — so paying it there is accepted deliberately. What keeps the hold
bounded is that no caller code runs inside it.

**No `max` parameter.** A cap gives back exactly the split this method exists to
prevent. It is also unnecessary: conflate already bounds a receiver's memory by
the live key set rather than by write volume, so "everything pending" is bounded
by the same property the README already claims for a slow receiver.

**Clearing `elems` and `pending` is load-bearing, not tidiness.** A stale `elems`
entry sends the next `Send` for that key down `enqueueLocked`'s coalesce branch,
which writes a slot without re-queuing the key — so the key would silently
vanish instead of reappearing at the tail. Pinned by a test that sends for a
previously taken key and asserts it arrives.

**A third reader of the queue.** `recvLoop` and `feed` both go through
`popLocked`; `TryRecvAll` is the one reader that does not. `drainedLocked`
remains the single definition of "this stream is over" and `TryRecvAll` joins the
list of paths its doc comment names, so the new path cannot grow its own idea of
when to stop.

**Not on the shared interface.** `gobus.Receiver` does not grow a member and
`conformance_test.go` gained no row: this is a method on an existing bus, not a
new architecture, and close/cancel/value precedence is unchanged. `TryRecvAll` is
concrete-type surface, like `Peek`.

**The one-call flush.** Prose that told a caller stopping on `ctx.Err()` to loop
on `TryRecv` until any error, before `Close`, is now a single call whose error
reports which state it stopped in. The surrounding point is unchanged: a flush is
not a substitute for `Close`, because against a still-open sender it ends on
`ErrEmpty`, which does not deregister.

## Alternatives considered

**`Drain` as the name.** See *Decision*.

**A `max` cap.** See *Consequences*. It reintroduces the split the method
prevents.

**`DrainAppend` / `TryRecvAllAppend`, taking a caller-supplied buffer.**
Deferred until an allocation profile asks for it. In a repo gated on 100%
coverage a second method is a second full test matrix, for a copy that is
dwarfed by whatever the caller does with the batch.

**A blocking `RecvAllContext(ctx)` — park until at least one event is pending,
then take everything.** This is the shape that actually matches the caller's
loop: today's pattern is `RecvContext` for the first event, then a flush, which
even with `TryRecvAll` is two calls, two lock acquisitions, and a first event
that arrives *outside* the returned slice for every caller to prepend by hand.
`recvLoop` already has the parking machinery; the delta is producing a slice at
the value arm instead of an event.

The prepend is worse than an ergonomic irritation, and this is the strongest
argument for the blocking form: it forfeits the one-entry-per-key guarantee.
`RecvContext` pops key `A` at T1; a `Send` for `A` at T1.5 enqueues `A` afresh
at the tail (coalescing cannot reach an event that has already left the slots);
the cut at T2 contains `A` again. The assembled batch now holds two entries for
`A` with different values — something a single `TryRecvAll` can never produce,
and something a caller that maps or dispatches per key will not expect.

This is not a correctness hole in `TryRecvAll`. The sort-and-checkpoint argument
in *Context* survives intact, because it rests on nothing being left *pending
below* the batch, which the cut still guarantees. But a caller composing the two
calls must either tolerate duplicate keys or fold them, and `RecvAllContext`
would remove the question by making the whole batch one cut.

Deferred rather than rejected, and deliberately not bundled here, for two
reasons. It has a real design question `TryRecvAll` does not: `recvLoop` resolves
closed > cancelled > value in one ordered run under `s.mu`, and a batch form must
decide what a cancellation landing on a *non-empty* queue returns — `ctx.Err()`
with the batch dropped is consistent with `RecvContext` (which discards nothing,
since the event stays in the slots) but is not obviously what a batch caller
wants. And `TryRecvAll` is a strict prerequisite: `RecvAllContext`'s value arm is
`popAllLocked`, so shipping the non-blocking form first costs nothing if the
blocking one follows. Revisit once a caller has used `TryRecvAll` and can say
whether the prepend is a real irritation.

**An exported pending count, so a caller could size its own loop.** Rejected for
the reason the count would be stale the moment it was returned — the same
argument that rules out synthesizing the cut caller-side. `TryRecvAll` returning
`ErrEmpty` is the emptiness test, and it distinguishes "empty" from "over" for
free.

## Implementation

One method, one unexported helper, one test seam. No new receiver state, no new
option, no new import, no change to `enqueueLocked`, `popLocked`, `peekLocked`,
`Merge`, delivery order, or the memory bound. The `Send` path is untouched.

`popAllLocked` mirrors `popLocked`, named to sit beside it rather than beside the
`drainedLocked` *predicate*. It walks `order`, reads each key's slot, then resets
the list with `order.Init()` and clears both maps with `clear` — Go 1.21, the
module floor, and capacity-retaining, which is what a receiver that repeatedly
refills the same live key set wants. Like `popLocked`, it deliberately does
**not** delegate to `peekLocked`: it walks elements it already holds, and
delegating would cost a second `elems` lookup per key under `s.mu`. The
duplication is documented at both sites.

`TryRecvAll`'s preamble is `TryRecv`'s verbatim — lock-free closed check, seam,
lock, re-check, drained-and-deregister — then the pop.
`forTestingBeforeTryRecvAllLock` joins the receiver's other pre-lock seams; the
convention is unconditional, so every lock-free closed pre-check gets one and the
close race is exercised deterministically rather than by timing.

Tests, each branch a CI coverage gate and each pin verified by mutation rather
than by green: lock-free `ErrClosed`; under-lock `ErrClosed` via the seam, so the
re-check rather than the pre-check produces the verdict; drained `ErrClosed`
after `Sender.Close` with the deregistration asserted via
`forTestingReceiverCount`; `Hub.Close` with events **still queued** returning
`ErrClosed` and abandoning them, which is the row the "not a statement that the
backlog was empty" bullet rests on; soft-drain ordering, where `Sender.Close`
with events queued returns all of them with a nil error and only the *next* call
is terminal; `ErrEmpty` on an open hub; multiple keys in first-touch order with a
coalesced key holding its merged value at its *original* position, asserted as
whole `Event` values over the whole slice so a duplicate key would fail;
annihilation; key filter; emptiness afterwards, including a `Send` for a
previously taken key arriving at the tail; the `Chan` in-flight edge; a
`TryRecvAll` racing `Sender.Close` under `-race`; and zero allocations when
nothing is pending.

Gates: `gofmt`, `go vet`, `staticcheck -checks=all`, `go test -race ./...`, and
100% coverage on the library packages.

## Still open

- Does any caller need to distinguish *why* `TryRecvAll` returned `ErrClosed` —
  receiver-closed and abandoned vs. sender-closed and fully drained? The
  distinction is exactly `Peek` ADR obligation 3, and the answer today is "keep
  the previous checkpoint on `ErrClosed`". If that proves too conservative, the
  fix is a distinct sentinel, not a partial result.
- Is queue order plus a caller-side sort right, or does a caller eventually want
  the bus to return the batch already ordered by a caller-supplied comparison?
  The `Peek` ADR's answer — the ordering quantity lives in `V`, and the bus makes
  no ordering claim — should hold here unless something new argues otherwise.
- Does the prepend irritation in the deferred `RecvAllContext` show up in a real
  caller? That is the trigger for revisiting the blocking form.
