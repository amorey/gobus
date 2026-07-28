# Expose a conflating receiver's backlog head with `Peek`

- **Status:** accepted, implemented
- **Date:** 2026-07-28
- **Scope:** `github.com/amorey/gobus`, package `conflate`
- **Supersedes:** the "pending backlog visibility" proposal (a bus-owned arrival
  counter plus a `WithSequence` option), withdrawn — see *Alternatives*.

## Context

A consumer that resumes a stream from a persisted cursor needs to know how far
its backlog reaches, not just what it has already received. The concrete case: a
downstream service commits a watermark after each batch, meaning "every write at
or below this version has been delivered." It reads that watermark from the
highest version in the batch it just assembled.

That is wrong whenever a key is still pending. Conflate delivers in *first-touch*
order, so a key first touched at a low version can sit in the queue while later
keys are delivered ahead of it. Committing the batch high-water mark skips the
undelivered write, and nothing about it looks wrong: the batch is complete, the
versions are monotonic, the cursor advances. The write is simply never replayed.

Avoiding that requires the consumer to know the *lowest* version still pending —
which is a property of the front of the receiver's queue, and conflate exposed
nothing about the queue at all. Every receive path consumed what it looked at.

## Decision

Add one accessor to `conflate.Receiver`, and keep the ordering quantity out of
the bus:

```go
// Peek returns the oldest pending event without removing it.
func (rx *Receiver[K, V]) Peek() (gobus.Event[K, V], error)
```

`Peek` is `TryRecv` without the pop, and that is the whole contract. It shares
`TryRecv`'s precedence exactly — `ErrClosed` when the receiver or hub is closed
or the sender has closed and the queue has drained, `ErrEmpty` when nothing is
pending, otherwise the head event. It is not a raw read of the queue: a closed
handle reports `ErrClosed` even with a value at the head, and the
drained-and-closed verdict carries the same `deregisterLocked` tear-down the
popping paths carry, under the lock that decided it.

The ordering quantity lives in `V`, folded by the `Merge` the bus already has.
`Merge` is the designated answer to "what does it mean to have two undelivered
values for this key"; carrying a first-touch stamp through a coalesce is exactly
that question. So the bus needs no notion of sequence, rank, or monotonicity —
only a way to look at the front of the queue without consuming it.

### What that costs the library

One method, one unexported helper (`peekLocked`), one test seam
(`forTestingBeforePeekLock`). No new option, no new receiver state, no new
imports, no change to `enqueueLocked`, `popLocked`, `Merge`, delivery order, or
the memory bound. The `Send` path is untouched. `Peek` is O(1) — a list-head read
plus one map lookup — and allocates nothing.

`popLocked` deliberately does **not** delegate to `peekLocked`. It already holds
the list element it removes, so sharing would cost it a second `elems` lookup on
the pop path under `s.mu`. Four duplicated lines are the cheaper trade; the
duplication is documented at both sites.

There is no `Empty()` and no exported pending count. `Peek` returning `ErrEmpty`
*is* the empty test, and it distinguishes "empty" from "over" for free.

## Consumer-side obligations

The design deliberately moves work out of the library, so the obligations it
creates are part of the decision rather than a footnote.

1. **The producer stamps the ordering quantity on every send.** `Merge` is never
   called on a first touch — `enqueueLocked`'s new-slot branch stores the sent
   value verbatim — so a design expecting `Merge` to establish the field leaves
   every first-touch value carrying a zero, and the watermark is pinned forever
   while every "it advances" test still passes.

2. **The fold must return a new value, never mutate in place.** `enqueueLocked`
   fans the *same* value to every receiver and stores each merge result in that
   receiver's own slot. With a pointer `V`, an in-place write to the merged
   struct is visible through other receivers' slots and through
   already-delivered events, and the stamp silently becomes whatever the last
   receiver's merge wrote. Use a value type for `V`, and construct the merged
   value rather than assigning into `prev` or `next`.

3. **`ErrClosed` does not mean "the backlog was empty."** It means that only for
   `Sender.Close`, the soft drain. `Hub.Close` and `Receiver.Close` abandon
   whatever is in the queue, and `Peek` cannot distinguish the three. A cursor
   that reads `ErrClosed` as empty and commits the batch high-water mark
   reintroduces exactly the skip this decision exists to prevent. The
   conservative rule: on `ErrClosed`, keep the previously committed watermark —
   unless the drain loop that assembled the batch itself stopped because it
   observed empty-or-closed, in which case committing is safe, since under
   version-ordered publication anything arriving after an observed-empty ranks
   above everything already seen. That exception requires the drain loop to
   report *why* it stopped, not just that it did.

4. **Version-ordered publication is the consumer's premise.** Soundness rests on
   "publication order matches the ordering quantity's order ⇒ the earliest
   first-touched key holds the lowest pending value." The bus does not check it.
   A publisher that commits under a lock held across publication satisfies it by
   construction; assert it at the send site if it is worth pinning.

Annihilation does not threaten any of this. A `Merge` returning `keep == false`
removes the head key, so the head can change with nothing delivered — but the
replacement head was first touched later, so its stamp is higher and the
watermark only ever moves conservatively. The annihilated key has no undelivered
write left to skip.

## Consequences

**Head-key stability is conditional, and the condition matters.** Coalescing
leaves the head key's queue position — and so its identity as the head —
unchanged while changing its value. Annihilation changes the head key. Both are
pinned by tests; a consumer reading a stamp off the head must tolerate the
second.

**A polling `Peek` loop is expensive for everyone.** `Peek` takes `s.mu`, the
single hub-wide lock that serializes the entire `Send` fan-out across all
receivers. Spinning on it degrades every publisher and every other receiver, not
just the caller's. It is documented as once-per-unit-of-work, not a spin. This is
the only real performance consequence; nothing is added to the send path.

**It is concurrency-safe but single-consumer-meaningful.** `Peek` is exactly the
method someone will want to call from a monitor or metrics goroutine. It is
mutex-safe, so that is not a data race — but a `Peek` racing a `Recv` is a
time-of-check/time-of-use gap: the parked reader takes the event just peeked. The
documented contract is *safe to call anywhere, meaningful only on the consuming
goroutine*.

**The `Chan` in-flight caveat now has a visible edge.** The feeder pops under
`s.mu` and parks on delivery outside it, so a `Chan` consumer can observe
`Peek() == ErrEmpty` while exactly one event is in flight, undelivered. Sound for
the cursor case — the in-flight event outranks everything already seen — but
surprising enough to be documented and tested.

**No general numeric backlog layer.** A consumer whose `V` carries no ordering
field gets nothing numeric from `Peek`: no "distinct keys admitted" ordinal, no
lag span, no head-of-line staleness number. It can see *what* is at the head, not
*how far* the queue has advanced. If that layer acquires a customer, the
rejected alternative below becomes relevant again.

**A monotonicity violation is silent.** Nothing checks the premise in
obligation 4, so a publisher bug yields a watermark that is too *high* — the
silent-skip failure the whole design exists to prevent. The mitigation is an
assertion at the send site, where the knowledge lives.

**`Peek` is not on the shared `gobus.Receiver` interface**, and
`conformance_test.go` gained no row. It is a conflate-specific accessor on the
concrete `*Receiver`; adding it to the interface would oblige every future bus
architecture to have a head-of-queue notion. It may generalize later. This
decision does not affect close/cancel/value precedence, which is why the
conformance suite is untouched.

## Alternatives considered

**A bus-owned ordering quantity (the withdrawn proposal).** Have every receiver
stamp each key at first touch, either with an always-on arrival counter or with a
caller function of the value supplied through a new `WithSequence` hub option,
and expose the head's stamp as an `OldestSequence() (int64, error)`.

It was complete and implementation-ready, and was displaced for four reasons:

1. *The library change was much larger.* It added a slot type, widened
   `elems` from `map[K]*list.Element` to a 16-byte value (+8 bytes per live key,
   on exactly the high-cardinality receiver it serves), three `Receiver` fields, a
   `receiverConfig` field, a hub option, an enforcement panic inside
   `enqueueLocked`, and two imports — all on the fan-out path under `s.mu`. This
   decision adds one method, one helper and one seam, and touches no existing path.

2. *It baked a rank type into the public API.* `int64` in `OldestSequence`'s
   signature permanently, with a generic `Hub[K, V, S]` recorded as a rejected
   generalization. Here the ordering quantity is whatever type the consumer likes,
   folded however the consumer likes.

3. *It put an ordering obligation in the library.* It had to document
   serialized publication, enforce it with a panic that aborts a fan-out
   mid-flight, and then declare `WithSequence` incompatible with the
   shared-`Sender`-across-goroutines pattern the package elsewhere documents as
   safe. With a `Merge` fold the bus makes no ordering claim and `Send` grows no
   new panic path. Note also that the panic only ever covered the serialized
   publisher — the case that was already correct — and explicitly could not cover
   the concurrent one.

4. *`Peek` is the more general primitive.* An always-on arrival counter is a
   bus-invented ordinal with no meaning outside the process. `Peek` is domain-free
   by construction, imposes no obligation, and is the O(1) head-only form of the
   full-backlog scan that remains the recorded extension point — without that
   one's whole-key-set walk under `s.mu`.

And it introduced a second per-key policy callback parallel to `Merge`, which is
already the designated per-key combining policy.

**Exposing the pending count, a `PeekN`, or the full queue contents.** Out of
scope. A count invites polling loops and answers nothing the cursor case needs. A
full `Pending(yield func(K, V) bool)` scan remains the recorded extension point
for a consumer whose ordering quantity is *not* monotonic in publication order —
under either design, that consumer needs more than the head.

## Implementation

Landed in five red/green cycles: `Peek` + `peekLocked`; the close-precedence
ordering and its test seam; the head-key stability pins; the `Chan` in-flight and
zero-allocation pins; then the docs. The pinning cycles were verified by mutation
rather than by passing — reading the queue tail instead of the head fails the head
and coalescing tests, and an added heap allocation fails the allocation test.

`drainedLocked`'s doc comment lists the paths that share it, so `Peek` joined that
list; that comment exists to stop each path from growing its own idea of when to
stop.

Gates: `gofmt`, `go vet`, `staticcheck -checks=all`, `go test -race ./...`, and
100% coverage on the library packages — every `Peek` branch including the
lock-free `ErrClosed`, the under-lock `ErrClosed` via the seam, the drained
`ErrClosed` with its deregistration, `ErrEmpty`, and the success return.

## Still open

- Does anything want a domain-free numeric backlog marker (stall detection,
  head-of-line staleness metric) that `Peek` cannot serve? If so, the withdrawn
  alternative's always-on layer has a customer.
- Is stamping the ordering quantity on every send acceptable at the publication
  site, given a forgotten stamp is a silently pinned watermark that no test
  catches?
- Is asserting version-ordered publication at the send site enough, given the bus
  no longer panics on a violation?
- The consumer-side watermark work is not closed by this decision. It has to be
  rewritten against `Peek`, including making the drain loop report why it
  stopped — obligation 3 depends on it.
