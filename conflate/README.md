# conflate

*Keyed latest-value fan-out: one slot per key, coalesced by a caller-supplied merge.*

[![Go Reference](https://pkg.go.dev/badge/github.com/amorey/gobus/conflate.svg)](https://pkg.go.dev/github.com/amorey/gobus/conflate)

Part of [gobus](../README.md).

```go
import "github.com/amorey/gobus/conflate"
```

## Contents

- [Overview](#overview)
- [Quick start](#quick-start)
- [Semantics](#semantics)
- [Receiver options](#receiver-options)
- [Inspecting the backlog head](#inspecting-the-backlog-head)
- [Taking the whole backlog at once](#taking-the-whole-backlog-at-once)
- [Publishing with no receivers](#publishing-with-no-receivers)
- [Contexts and cancellation](#contexts-and-cancellation)
- [Close semantics](#close-semantics)
- [Chan support](#chan-support)
- [Thread safety](#thread-safety)
- [API reference](#api-reference)
- [Errors](#errors)
- [Examples](#examples)

## Overview

Conflate is a single-producer, multi-consumer keyed latest-value bus. Every
value published through the singleton `Sender` is fanned out to *every* live
`Receiver`, but each receiver holds **one slot per key** plus an
insertion-ordered key queue. A `Send` for a key that already has an undelivered
value coalesces into that slot via a caller-supplied `Merge` instead of
appending, and the key keeps its original queue position.

Because conflate keeps the latest value **per key**, a slow receiver never
lags-as-loss: it catches up to the current state of *every* key in first-touch
order, and its memory stays bounded by the live key set rather than by write
volume. There is no capacity argument and no lag error, because there is no
unbounded backlog to grow.

Typical uses: streaming resource state to watchers (Kubernetes-style
informers), UI or dashboard update feeds where only the current state of each
entity matters, cache invalidation fan-out, incremental index maintenance, and
any "coalesce writes per entity, deliver at consumer pace" pipeline.

Pick `conflate` over [`watch`](../watch/README.md) when a consumer follows many
keys at once, or when it must observe a key's *history* collapsed rather than
just its latest state — in particular when a create-then-delete pair must leave
no residue, which is what `Merge`'s annihilation gives you.

## Quick start

```go
// merge is the coalescing policy: how an undelivered pending value combines
// with a newly sent one for the same key.
merge := func(prev, next Update) (Update, bool) {
    if prev.Phase == "Added" && next.Phase == "Deleted" {
        return Update{}, false // annihilate: the consumer never saw it exist
    }
    return next, true // newer revision supersedes the older
}

hub := conflate.New[string, Update](merge)
defer hub.Close()

tx := hub.Sender()
defer tx.Close()

rx := hub.Receiver()
defer rx.Close()

go func() {
    for {
        ev, err := rx.Recv()
        if err != nil {
            return // ErrClosed
        }
        // ev.Value is the *latest* Update for ev.Key, not every intermediate one.
        apply(ev.Key, ev.Value)
    }
}()

for _, u := range updates {
    tx.Send(u.Key, u)
}
```

## Semantics

**Latest-value-per-key delivery.** Each receiver owns an insertion-ordered
queue of keys plus one value slot per key. A `Send` for a key with no pending
slot appends the key at the back of the queue; a `Send` for a key that is
already pending coalesces into the existing slot via `Merge` and leaves the
key's queue position unchanged. Delivery order is therefore **first-touch
order**, and a hot key does not starve a cold one by repeatedly jumping the
queue.

**Coalescing happens at `Send`, not at `Recv`.** A read is a plain pop. What
the consumer gets is whatever the slot had accumulated by the moment it read.

**Bounded by the key set, not the write volume.** A thousand sends across four
keys leave at most four pending entries.

**`Merge` may annihilate.** Returning `keep == false` drops the key entirely —
queue entry and value slot both — so a create/delete pair the consumer never
observed leaves no residue at all.

**`Send` never blocks.** Slow receivers cannot apply backpressure to the
publisher; they simply coalesce more aggressively.

**A receiver starts empty.** It observes values sent after it was created, not
the producer's history. See [Publishing with no
receivers](#publishing-with-no-receivers) for the ordering rule that follows.

`Merge` and any key filter are **caller code running under the bus lock**. They
must not call back into the hub, and they must not take a lock another
goroutine may hold while calling into the bus. Reading their arguments and
nothing else is always safe.

## Receiver options

Receivers take composable options, minted by the hub itself rather than by
package-level functions — that fixes `K` and `V` from the hub, so call sites
need no type arguments and an option built from a differently-typed hub is a
compile error.

```go
rx := hub.Receiver()                                  // every key, hub's merge
rx := hub.Receiver(hub.WithKeyFilter(wanted))         // one key subset
rx := hub.Receiver(hub.WithMerge(stricter))           // own coalescing policy
rx := hub.Receiver(hub.WithKeyFilter(wanted), hub.WithMerge(stricter))
```

`WithKeyFilter` filters at *enqueue*, so an unwanted key never occupies a
slot — that is how a consumer watching one key of a high-cardinality producer
stays bounded by that one key.

`WithMerge` gives a single consumer its own coalescing policy, which matters
when consumers of the same producer disagree about what may be dropped; one
hub-wide `Merge` cannot express that.

Later options win over earlier ones for the same setting. A nil option, or a
nil function passed to either constructor, panics — policy here is explicit,
never implicitly defaulted.

## Inspecting the backlog head

`Peek()` returns the oldest pending event without removing it — `TryRecv` minus
the pop, sharing its precedence exactly: `ErrEmpty` when nothing is pending,
`ErrClosed` when the receiver or hub is closed or the sender has closed and the
queue has drained.

```go
ev, err := rx.Peek()   // what would Recv hand me next, without taking it?
```

It is not a raw read of the queue, so a closed handle reports `ErrClosed` even
with a value at the head. The corollary matters if you track a cursor:
`ErrClosed` is *not* a statement that the backlog was empty, because
`Receiver.Close()` and `Hub.Close()` abandon whatever is still queued. Only
`Sender.Close()` drains first.

The value you get back is the current merged contents of the head key's slot,
so it can change between two `Peek`s while the head *key* does not — coalescing
leaves queue position alone. Annihilation is the exception: a `Merge` returning
`keep == false` for the head key removes it, and the next `Peek` reports a
different key.

Two cautions. `Peek` takes the same hub lock that serializes the entire `Send`
fan-out, so polling it in a loop degrades every publisher and every other
receiver on the bus — call it once per unit of work. When that unit of work is
a whole burst, [`TryRecvAll`](#taking-the-whole-backlog-at-once) is how you say
so in one call. And while it is safe to call from any goroutine, it is only
*meaningful* on the receiver's single consuming goroutine: anything else
consuming concurrently can take the event you just looked at. An event already
handed to the `Chan` feeder has left the queue, so `Peek` reports `ErrEmpty`
while that one event is in flight.

The intended use is a consumer that needs to know how far its backlog reaches —
a resumable cursor or watermark. Fold the ordering quantity into `V` and let
the bus's own `Merge` carry it: stamp each value at publication, keep the
*earliest* stamp when two values for a key coalesce, and read it off the head.
The bus makes no ordering claim of its own here, so the premise that
publication order matches that quantity's order is yours to hold; if it
doesn't, the head key's stamp is not the lowest one pending.

## Taking the whole backlog at once

`TryRecvAll()` pops everything pending, in queue order, and empties the queue —
`TryRecv` applied to the whole queue instead of the head, under one acquisition
of the hub lock. It shares that precedence too: `ErrEmpty` when nothing is
pending, `ErrClosed` on the same terminal conditions.

```go
evs, err := rx.TryRecvAll()   // everything pending, as of one instant
```

The atomicity is the contract, not an optimization. A loop of `TryRecv` is a
*sequence* of instants, so the batch it assembles has no defined membership — a
`Send` landing between two iterations joins it, one landing just after the loop
observes empty does not — and no caller can close that gap from outside the
lock. This matters most for the cursor case above. Conflate delivers in
first-touch order, which carries no relation to any ordering quantity inside
`V`, so a consumer that sorts its batch by that quantity needs the batch to be
a complete set: take a proper subset and the next batch interleaves with the
one you already committed. `TryRecvAll` supplies the *set*; you still sort it.

Two properties of the returned slice are worth building on. It is in queue
order, which is first-touch order — sort if you need value order. And it holds
one entry per key, since a receiver's queue holds each key once, so it folds
into a map without deduping. That second one belongs to a *single* call: an
event that has already left the receiver's slots cannot be coalesced into, so a
batch you assemble from a `Recv` plus a `TryRecvAll` can contain the same key
twice.

Partial results are not a case — either every pending event with a nil error,
or no events and an error — so you can test the error and ignore the slice.
Against `Sender.Close()` the whole queue still comes back with a nil error and
only the *next* call reports `ErrClosed`.

There is no `max` parameter. A cap would hand back exactly the split the method
exists to prevent, and it is unnecessary: a receiver's memory is already bounded
by the live key set rather than by write volume, so "everything pending" is
bounded by construction. Note the trade against a `TryRecv` loop, though: total
lock acquisitions go down, but this one is held for the length of the queue, so
worst-case *publisher* latency goes up. No caller code runs inside it — no
`Merge`, no key filter — which is what keeps that hold bounded.

## Publishing with no receivers

`Send` on a hub with no live receiver returns `nil` without taking the bus
lock. That lock is hub-wide — every `Send` fan-out, every pop, `Recv`, `Peek`,
`TryRecv`, `TryRecvAll` and `Close` serializes through it — so the cost of an
unwatched hub otherwise lands on the *producer's* hot path, per publish,
whether or not anything reads the result. This matters when values are
published from inside a producer's own critical sections: without it, every
write in a subsystem pays a mutex for a bus nobody is subscribed to.

The result is unchanged, only the cost: a send with no receivers already fanned
out to nobody. `TrySend` and `SendContext` take the same path, and a cancelled
`ctx` is still reported rather than swallowed. A closed sender still returns
`ErrClosed` on an empty receiver set.

The corollary is an ordering requirement on **your** side, and it is the one
thing to get right:

```go
rx := hub.Receiver()   // register FIRST
state := snapshot()    // then read your snapshot
```

Take the snapshot before registering and any value published in the gap reaches
no receiver — conflate has no replay, and the next `Send` for that key
coalesces into a slot your consumer was never told had been skipped. This has
always been conflate's delivery model; a publisher that skips the lock simply
makes it unforgiving.

(`watch` removes this rule by taking your snapshot as its registration
argument. See [its README](../watch/README.md#registration-is-the-snapshot).)

## Contexts and cancellation

Precedence is **closed > cancelled > value** on the receive side and **closed >
cancelled** on the send side; both are pinned by the module's conformance
suite. The full rationale lives in the [root README](../README.md#close--cancel-precedence).
What is conflate-specific:

- `SendContext` never blocks, so `ctx` is consulted exactly once, where the
  send is *resolved* — under the bus lock, not on entry. A `ctx` cancelled
  while the call waited for the lock publishes nothing and returns `ctx.Err()`.
  Waiting for that lock is real work: your `Merge` and key filters run under it.
- `RecvContext` returns `ctx.Err()` *even when an event is pending*, and leaves
  that event queued. Without it, a consumer looping against a fast publisher
  would take a value every iteration and never notice its own shutdown.
- `ctx.Err()` is not an end-of-stream and does not deregister the receiver. A
  caller that stops on it must `Close` the handle, or it keeps coalescing —
  one slot per live key — for the hub's lifetime. `defer rx.Close()` covers it.
- To consume what is left first, call `TryRecvAll` once: the error tells you
  which state you stopped in — `ErrEmpty` while the sender is open, `ErrClosed`
  once it has closed and the queue is drained. The flush is not a substitute
  for the `Close`: against a still-open sender it ends on `ErrEmpty`, which is
  not terminal and does not deregister.

## Close semantics

| Call               | Effect                                                                                                                                  |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| `Sender.Close()`   | Graceful end-of-stream. Each receiver drains its pending per-key values once, then sees `ErrClosed` / a closed `Chan`.                     |
| `Receiver.Close()` | This handle only. Other receivers and the sender keep running; this handle's pending values are abandoned and its `Chan` feeder shuts down. |
| `Hub.Close()`      | Hard tear-down: sender plus every live receiver, with no drain. Future `Hub.Receiver()` calls return pre-closed handles.                   |

All idempotent. Don't call `Hub.Close` concurrently with an active `Send` from
another goroutine — it tears down the receivers that send is fanning out to.

`Sender.Close` **is** safe to call concurrently with a `Send` or `SendContext`
from another goroutine. The two serialize, so a racing send resolves to exactly
one of two outcomes — it publishes and returns `nil`, or it returns `ErrClosed`
and publishes nothing. There is no third outcome and no partial one. Which
ordering wins is unspecified: a caller that needs a value visible before
shutdown must order the two itself, and a caller shutting down that doesn't care
whether the last value lands needs no fence on its write path. This holds
because `conflate`'s `Send` never parks; it is a promise about this package,
not a module-wide rule.

A receiver that reaches the terminal `ErrClosed` after a `Sender.Close` drain
deregisters itself from the hub, so a long-lived hub doesn't pin abandoned
receivers.

## Chan support

`Chan()` returns a per-receiver **private** channel fed by a per-receiver
goroutine. It yields pending events in first-touch key order, carrying the same
`gobus.Event` values the `Recv` methods return, and repeated calls return the
same channel.

```go
for ev := range rx.Chan() {
    apply(ev.Key, ev.Value)
}
```

The channel is unbuffered on purpose: coalescing continues in the receiver's
per-key slots while the consumer is busy, so a fast publisher produces no
backlog beyond the live key set. One caveat — an event already handed to the
feeder has **left** the receiver's slots, so a `Send` for that key while the
feeder is parked on delivery enqueues the key afresh rather than coalescing
into the in-flight event. (This is also why `Peek` reports `ErrEmpty` for an
event in flight.)

The channel closes when the feeder observes receiver-close, or sender/hub-close
with nothing left to drain. Abandoning the channel without calling
`Receiver.Close()` pins the feeder goroutine — it parks forever waiting for the
next event. **Always `Close` the receiver when you stop reading.**

## Thread safety

`Sender` is safe to share across goroutines: `Send` and `Close` both serialize
through the hub lock, and `Send` first reads a lock-free receiver count so it
takes that lock only when a receiver is registered.

A `Receiver` is intended for a **single consumer goroutine**, and conflate
relies on it: the receiver owns an insertion-ordered queue meant to be popped
by one reader. `Peek`, `TryRecv` and `TryRecvAll` are safe to call from any
goroutine but are only *meaningful* on that one, since a concurrent consumer
can take the event between your two calls.

One mutex guards every receiver on the hub. `Send` fans a write across all of
them under it, and each receiver pops from its own queue under the same lock.

## API reference

### Hub

```go
func New[K comparable, V any](merge Merge[V]) *Hub[K, V]
```

Creates a hub whose receivers coalesce per key using `merge`. **Panics if
`merge` is nil** — the coalescing policy is the whole point of this bus, so
there is no implicit default.

```go
func (h *Hub[K, V]) Sender() *Sender[K, V]
func (h *Hub[K, V]) Receiver(opts ...ReceiverOption[K, V]) *Receiver[K, V]
func (h *Hub[K, V]) WithKeyFilter(keep func(K) bool) ReceiverOption[K, V]
func (h *Hub[K, V]) WithMerge(merge Merge[V]) ReceiverOption[K, V]
func (h *Hub[K, V]) Close()
```

`Sender` returns the singleton send-side handle; repeated calls return the same
one. `Receiver` returns a fresh handle per subscriber. Both return pre-closed
handles once the hub is closed. `WithKeyFilter` and `WithMerge` panic on a nil
function; `Receiver` panics on a nil option.

### Merge

```go
type Merge[V any] func(prev, next V) (merged V, keep bool)
```

Combines an undelivered pending value with a newly sent one for the same key.
Invoked only when the key already has a pending slot. `keep == false`
annihilates: the key is dropped from the queue and the slot alike. Called under
the bus lock, so it must not call back into the hub.

### Sender

```go
func (tx *Sender[K, V]) Send(k K, v V) error
func (tx *Sender[K, V]) TrySend(k K, v V) error
func (tx *Sender[K, V]) SendContext(ctx context.Context, k K, v V) error
func (tx *Sender[K, V]) Close()
```

`Send` never blocks. `TrySend` is equivalent to it — there is no separate
non-blocking path — and exists to satisfy `gobus.Sender`. All three return
`ErrClosed` once the sender or hub is closed. `conflate` never returns
`ErrFull`.

### Receiver

```go
func (rx *Receiver[K, V]) Recv() (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) RecvContext(ctx context.Context) (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) TryRecv() (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) TryRecvAll() ([]gobus.Event[K, V], error)
func (rx *Receiver[K, V]) Peek() (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) Chan() <-chan gobus.Event[K, V]
func (rx *Receiver[K, V]) Close()
```

`Peek` and `TryRecvAll` are concrete-`*Receiver` accessors, not part of the
`gobus.Receiver` interface; everything else on this list implements it.

## Errors

| Error               | Returned by                | Means                                                                                      |
| ------------------- | -------------------------- | ------------------------------------------------------------------------------------------ |
| `gobus.ErrClosed`   | every send and receive path | The receiver or hub is closed, or the sender is closed and this receiver's queue has drained. |
| `gobus.ErrEmpty`    | `TryRecv`, `TryRecvAll`, `Peek` | Nothing is pending. Not terminal.                                                        |
| `ctx.Err()`         | `SendContext`, `RecvContext` | The context was cancelled. Not terminal, and consumes nothing.                             |

`ErrFull` is never returned: there is no capacity argument, because coalescing
bounds a receiver's buffer by the live key set rather than the write volume.
There is no `ErrLagged` equivalent either — a receiver that falls behind does
not lose values it could be told about, it collapses them, which is the
contract rather than an error condition.

## Examples

- [`examples/recv`](./examples/recv/main.go) — the classic resource-watch
  pattern: a fast producer, slow subscribers, annihilation of a create/delete
  pair, and a graceful `Sender.Close` drain. `go run ./conflate/examples/recv`
- [`examples/chan`](./examples/chan/main.go) — the same bus consumed through
  `Chan()` and `select`. `go run ./conflate/examples/chan`

[Package docs on pkg.go.dev](https://pkg.go.dev/github.com/amorey/gobus/conflate)
