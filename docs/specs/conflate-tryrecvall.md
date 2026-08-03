# Spec: `conflate.Receiver.TryRecvAll`

- **Status:** accepted, implemented
- **Date:** 2026-08-03
- **Scope:** `github.com/amorey/gobus`, package `conflate`
- **Supersedes:** the `Receiver.Drain` feature request in
  `docs/gobus-conflate-drain.md` — same method, renamed, with the motivation
  re-grounded on atomicity rather than lock contention (see *Motivation*).

## Summary

Add one accessor to `conflate.Receiver`:

```go
// TryRecvAll pops every pending event without blocking.
func (rx *Receiver[K, V]) TryRecvAll() ([]gobus.Event[K, V], error)
```

`TryRecvAll` is `TryRecv` that takes the whole queue instead of the head, under
one acquisition of `s.mu`. It shares `TryRecv`'s precedence exactly and adds no
new state, option, or send-path work — the same shape as
[`Peek`](../adr/2026-07-28-conflate-peek.md).

## Motivation

### A `TryRecv` loop cannot take a consistent cut

A consumer that reads a burst as one unit calls `RecvContext` for the first
event, then loops on `TryRecv` until the receiver reports empty, then processes
the batch. That loop is a *sequence of instants*, not one instant: a `Send`
landing between two iterations joins the batch, and a `Send` landing just after
the loop observes empty does not. The caller has no way to ask for "everything
pending as of one instant", and no way to synthesize it — it cannot ask how
many keys are pending, and any answer would be stale before it acted on it.
Only the bus can take that cut, because only the bus holds the lock across the
whole queue.

That is the guarantee this method adds, and it is the only part of the case
that a caller genuinely cannot build itself.

### Why the cut matters for a conflate consumer specifically

A conflate receiver's queue is ordered by **first touch**, and a merge leaves a
key at its original position (`enqueueLocked`, `conflate/conflate.go:491`). So
queue order carries no relation to any ordering quantity inside `V`. A consumer
whose `V` carries such a quantity — a log position, a version, a watermark —
must sort the batch it assembles, and a sort is only sound over a complete set.

Concretely: a pending queue of `A@12, B@11`, where `A@12` coalesced in place
from `A@10`, is safe to checkpoint only if both are taken together and sorted.
A consumer that takes a proper subset, checkpoints `12`, and leaves `B@11` for
the next batch loses `B@11` for good. This is the same silent-skip failure
[the `Peek` ADR](../adr/2026-07-28-conflate-peek.md) exists to prevent,
approached from the other end: `Peek` tells you what is still pending below
your batch, `TryRecvAll` guarantees there is nothing still pending below it.

`TryRecvAll` **does not order anything.** It returns queue order, which is
first-touch order. The caller still sorts. What it supplies is the *set*, which
is what makes the caller's sort sound.

### The termination bound is a corollary, not the case

A `TryRecv` loop ends when the receiver reports empty, which a fast enough
producer postpones indefinitely. Real callers terminate for reasons that are
properties of their producer (a store-bound tailer that must re-read before it
can publish again), not of the bus — so a reader of the loop cannot see the
bound. A snapshot terminates by construction. Worth stating; not sufficient on
its own to justify new public surface.

### Lock contention is a secondary motive, and a trade rather than a win

Draining N keys acquires the single hub-wide mutex N times, each acquisition
contending with every publisher's fan-out and every other receiver's reads —
the precise pattern `Peek`'s doc calls out ("call it once per unit of work, not
as a spin", `conflate/conflate.go:744`). `TryRecvAll` is how a caller honours
that advice when the unit of work is a whole burst.

But it buys fewer acquisitions with a longer hold. `popAllLocked` walks the
queue, does one map lookup per key, clears both maps, and allocates the result
slice — all under `s.mu`, and `Event` values are copied by value, so a large `V`
or a large live key set makes the critical section correspondingly long.
Amortized total cost goes down; worst-case publisher latency goes **up**. That
is a different contention profile, not a strict improvement, and it should be
described that way wherever it is mentioned.

The allocation cannot reasonably be hoisted out of the lock — `n` is only known
under it — so paying it there is accepted deliberately. What keeps the hold
bounded is that no caller code runs inside it; see *Doc comment obligations*.

None of this has been measured and no correctness claim rests on it. It should
not appear as the headline motive in the doc comment or the README.

## Surface

```go
func (rx *Receiver[K, V]) TryRecvAll() ([]gobus.Event[K, V], error)
```

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
  forfeited by composing calls; see *Rejected and deferred*.
- **One acquisition of `s.mu`**, spanning the whole queue. That atomicity is
  the contract, not an optimization.
- **Precedence is `TryRecv`'s, unchanged**: closed beats empty beats value.

  | Receiver state | Result |
  | --- | --- |
  | receiver or hub closed | `nil, gobus.ErrClosed` |
  | sender closed **and** queue empty | `nil, gobus.ErrClosed` + deregister |
  | sender closed, queue non-empty | all events, `nil` |
  | queue empty, sender open | `nil, gobus.ErrEmpty` |
  | otherwise | all events, `nil` |

- **Partial results are not a case.** Either it returns a non-empty slice and a
  nil error, or an empty slice and an error. There is never "some values and
  `ErrClosed`". A caller may test `err != nil` and ignore the slice.
- **`ErrClosed` is not a statement that the backlog was empty**, for the same
  reason it isn't on `Peek`: `Hub.Close` and `Receiver.Close` abandon whatever
  is queued, and only `Sender.Close` drains first. A cursor-tracking caller must
  distinguish *why* its loop stopped.
- **No `max` parameter.** A cap gives back exactly the split this method exists
  to prevent. It is also unnecessary: conflate already bounds a receiver's
  memory by the live key set rather than by write volume, so "everything
  pending" is bounded by the same property the README already claims for a slow
  receiver.
- **No `TryRecvAllAppend` / caller-supplied buffer.** Deferred until an
  allocation profile asks for it. In a repo gated on 100% coverage a second
  method is a second full test matrix, for a copy that is dwarfed by whatever
  the caller does with the batch.
- **`Chan` interaction is unchanged.** An event already handed to the feeder has
  left the queue, so `TryRecvAll` does not see it — the same rule `Peek` states,
  and the same in-flight edge.
- **Single-consumer-meaningful, any-goroutine-safe**, as for every other receive
  path.

### Doc comment obligations

Three things must be stated in the doc comment because nothing else records them:

1. It returns first-touch order, which carries **no** relation to any ordering
   quantity inside `V`. A caller that needs value order sorts.
2. The returned slice holds **one entry per key**, and that holds for one call
   only — a batch assembled from more than one receive can repeat a key.
3. The critical section is O(live keys) and runs **no caller code** — no
   `Merge`, no key filter. That is what makes holding `s.mu` across the whole
   queue acceptable, and it is an invariant future changes must preserve.

## Implementation

One method, one unexported helper, one test seam. No new receiver state, no new
option, no new import, no change to `enqueueLocked`, `popLocked`, `peekLocked`,
`Merge`, delivery order, or the memory bound. The `Send` path is untouched.

### `popAllLocked`

Mirrors `popLocked` (`conflate/conflate.go:516`), named to sit beside it rather
than beside the `drainedLocked` *predicate*.

```go
// popAllLocked removes and returns every pending event in queue order. Caller
// holds s.mu.
func (rx *Receiver[K, V]) popAllLocked() []gobus.Event[K, V] {
	n := rx.order.Len()
	if n == 0 {
		return nil
	}
	out := make([]gobus.Event[K, V], 0, n)
	for e := rx.order.Front(); e != nil; e = e.Next() {
		k := e.Value.(K)
		out = append(out, gobus.Event[K, V]{Key: k, Value: rx.pending[k]})
	}
	rx.order.Init()
	clear(rx.elems)
	clear(rx.pending)
	return out
}
```

`clear` on a map is Go 1.21, which is the module floor — it retains capacity,
which is what a receiver that repeatedly refills the same live key set wants.
`order.Init()` is `list.List`'s documented reset.

Like `popLocked`, this deliberately does **not** delegate to `peekLocked`: it
walks elements it already holds, and delegating would cost a second `elems`
lookup per key under `s.mu`. The duplication is documented at the site, as
`popLocked`'s is.

### `TryRecvAll`

The preamble is `TryRecv`'s (`conflate/conflate.go:703`) verbatim — lock-free
closed check, seam, lock, re-check, drained-and-deregister — then the pop.

```go
func (rx *Receiver[K, V]) TryRecvAll() ([]gobus.Event[K, V], error) {
	if rx.done.IsClosed() {
		return nil, gobus.ErrClosed
	}
	if rx.forTestingBeforeTryRecvAllLock != nil {
		rx.forTestingBeforeTryRecvAllLock()
	}
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	// rx.done can flip between the lock-free check above and acquiring mu;
	// Close holds mu before closing done, so a re-check here is race-free.
	if rx.done.IsClosed() {
		return nil, gobus.ErrClosed
	}
	if rx.drainedLocked() {
		rx.deregisterLocked()
		return nil, gobus.ErrClosed
	}
	if evs := rx.popAllLocked(); len(evs) > 0 {
		return evs, nil
	}
	return nil, gobus.ErrEmpty
}
```

### Test seam

`forTestingBeforeTryRecvAllLock func()` joins `forTestingBeforeRecvLock`,
`forTestingBeforeTryRecvLock` and `forTestingBeforePeekLock` on the receiver
(`conflate/conflate.go:191`), and their shared doc comment grows a name. The
convention is unconditional: every lock-free closed pre-check gets a seam so the
close race is exercised deterministically rather than by timing.

`drainedLocked`'s doc comment (`conflate/conflate.go:545`) lists the paths that
share it — `recvLoop`, `TryRecv`, `Peek`, `feed`. `TryRecvAll` joins that list.
That comment exists to stop each path from growing its own idea of when to stop.

## Tests

100% coverage on the new path is a CI gate, so every branch needs a case.
Verify the pinning tests by mutation, not by green.

- Lock-free `ErrClosed` (receiver closed before the call).
- Under-lock `ErrClosed` via the seam — close the receiver from inside
  `forTestingBeforeTryRecvAllLock`, proving the re-check, not the pre-check,
  produced the verdict.
- Drained `ErrClosed` after `Sender.Close`, **with** the deregistration
  asserted via `forTestingReceiverCount` (`conflate/helpers_test.go`).
- `Hub.Close` with events **still queued** returns `ErrClosed` and abandons the
  backlog. This is the row that separates hard tear-down from the soft drain
  above, and it is what the "`ErrClosed` is not a statement that the backlog was
  empty" bullet rests on. (It resolves at the closed pre-check or re-check, since
  `Hub.Close` closes every `rx.done` under `s.mu` — `conflate/conflate.go:304`.)
- Soft-drain ordering: `Sender.Close` with events queued returns all of them and
  `nil`, and only the *next* call returns `ErrClosed`. This is the "no partial
  results" pin.
- `ErrEmpty` on an open hub with nothing pending.
- Success: multiple keys returned in first-touch order, with a coalesced key
  holding its merged value at its *original* position — the property the sort
  argument rests on. Assert whole `Event` values, per the repo's `assertRecv`
  convention. The same case pins one-entry-per-key: the twice-sent key appears
  exactly once. Assert the whole slice, so a duplicate would fail.
- Annihilation: a key removed by `Merge` returning `keep == false` is absent.
- Key filter: a filtered key never appears.
- Emptiness after: an immediately following `TryRecv` returns `ErrEmpty`, and a
  following `Send` for a previously drained key enqueues afresh at the tail
  (proving `elems`/`pending` were cleared, not just `order`).
- `Chan` in-flight: an event handed to the feeder is not in the returned slice.
- Race: a `TryRecvAll` racing `Sender.Close` resolves closed-beats-value like
  the other receive paths, under `-race`.
- Zero pending allocates nothing (mirrors `Peek`'s allocation pin).

No new `conformance_test.go` row. This is a method on an existing bus, not a new
architecture; `gobus.Receiver[K, V]` does not grow a member, and close/cancel/
value precedence is unchanged. `TryRecvAll` is concrete-type surface, like
`Peek`.

## Docs to update in the same change

Each prose site has a mirror in the source; update both, or the doc comments and
the README disagree about what to call.

- **`gobus.go`** — unchanged. The shared interface does not grow a member; note
  this explicitly in review so it isn't added by reflex.
- **`README.md`**, `conflate` section, beside `Peek` (~line 69). `Peek`'s
  "call it once per unit of work" line should point at `TryRecvAll` as the way
  to honour it when the unit of work is a whole burst.
- **`conflate.go:744`** — the same "call it once per unit of work, not as a
  spin" sentence in `Peek`'s own doc comment. It is the mirror of the README
  line above and needs the same pointer.
- **`README.md`** ~line 217, which currently tells a caller stopping on
  `ctx.Err()` to "loop on `TryRecv` until it returns *any* error" as the flush
  before `Close`. That is now a one-call flush. The surrounding point stands
  unchanged: a flush is not a substitute for `Close`, because against a
  still-open sender it ends on `ErrEmpty`, which does not deregister.
- **`conflate.go:595`** — `RecvContext`'s doc comment carries that same
  "loop on `[Receiver.TryRecv]` until it reports any error, then Close" advice.
  Same rewrite, same surviving caveat about `ErrEmpty` not deregistering.
- **`README.md`** should state the one-entry-per-key guarantee where it
  describes the returned batch; it is the property a batch consumer will build
  on.
- **`CLAUDE.md`**, the "Two receive paths share that pop" paragraph, if a third
  reader of the queue changes what that paragraph should say.
- **`docs/gobus-conflate-drain.md`** — nothing to do. The superseded request was
  never committed to this repo and has since been removed from the working tree;
  the beehive-side copy is authoritative. This spec's *Supersedes* header is the
  only record needed here.
- An ADR is **not** required. This spec plus the `Peek` ADR carry the rationale;
  `TryRecvAll` decides nothing architectural that the `Peek` ADR did not already
  decide.

## Rejected and deferred

**`Drain` as the name.** See *Surface*.

**A `max` cap.** See *Semantics*. It reintroduces the split the method prevents.

**`DrainAppend` / `TryRecvAllAppend`.** Deferred; no measured caller.

**A blocking `RecvAllContext(ctx)` — park until at least one event is pending,
then take everything.** This is the shape that actually matches the caller's
loop: today's pattern is `RecvContext` for the first event, then a flush, which
even with `TryRecvAll` is two calls, two lock acquisitions, and a first event
that arrives *outside* the returned slice for every caller to prepend by hand.
`recvLoop` (`conflate/conflate.go:623`) already has the parking machinery; the
delta is producing a slice at the value arm instead of an event.

**The prepend is worse than an ergonomic irritation, and this is the strongest
argument for the blocking form.** It forfeits the one-entry-per-key guarantee.
`RecvContext` pops key `A` at T1; a `Send` for `A` at T1.5 enqueues `A` afresh
at the tail (coalescing cannot reach an event that has already left the slots);
the cut at T2 contains `A` again. The assembled batch now holds two entries for
`A` with different values — something a single `TryRecvAll` can never produce,
and something a caller that maps or dispatches per key will not expect.

This is not a correctness hole in `TryRecvAll`. The sort-and-checkpoint
argument in *Motivation* survives intact, because it rests on nothing being
left *pending below* the batch, which the cut still guarantees. But a caller
composing the two calls must either tolerate duplicate keys or fold them, and
`RecvAllContext` would remove the question by making the whole batch one cut.

Deferred rather than rejected, and deliberately not bundled here, for two
reasons. It has a real design question `TryRecvAll` does not: `recvLoop`
resolves closed > cancelled > value in one ordered run under `s.mu`, and a
batch form must decide what a cancellation landing on a *non-empty* queue
returns — `ctx.Err()` with the batch dropped is consistent with `RecvContext`
(which discards nothing, since the event stays in the slots) but is not
obviously what a batch caller wants. And `TryRecvAll` is a strict prerequisite:
`RecvAllContext`'s value arm is `popAllLocked`, so shipping the non-blocking
form first costs nothing if the blocking one follows. Revisit once a caller has
used `TryRecvAll` and can say whether the prepend is a real irritation.

## Open questions

- Does any caller need to distinguish *why* `TryRecvAll` returned `ErrClosed`
  — receiver-closed and abandoned vs. sender-closed and fully drained? The
  distinction is exactly `Peek` ADR obligation 3, and the answer today is "keep
  the previous checkpoint on `ErrClosed`". If that proves too conservative, the
  fix is a distinct sentinel, not a partial result.
- Is queue order plus a caller-side sort right, or does a caller eventually want
  the bus to return the batch already ordered by a caller-supplied comparison?
  The `Peek` ADR's answer — the ordering quantity lives in `V`, and the bus
  makes no ordering claim — should hold here unless something new argues
  otherwise.
