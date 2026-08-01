# gobus

*Gobus is a small library of common event bus architectures for Go*

<img width="435" src="https://github.com/user-attachments/assets/b564ee83-8171-4063-8796-665695e60906" />

[![Go Reference](https://pkg.go.dev/badge/github.com/amorey/gobus.svg)](https://pkg.go.dev/github.com/amorey/gobus)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

## Introduction

This library is a collection of common event bus architectures for Go. It's designed as a sister library to [`gochan`](https://github.com/amorey/gochan) which is a collection of lower-level channel architectures. Unlike keyless channels, event buses pass messages between senders and receivers based on keys and the interesting design decisions are about what happens when several values for the same key are in flight at once. Currently these are the event bus archictures included in this library:

| Package    | Senders | Receivers | Semantics                                                                   |
| ---------- | ------- | --------- | --------------------------------------------------------------------------- |
| `conflate` | 1       | many      | Keyed latest-value fan-out: per-key coalescing via a caller-supplied merge. |

## Installation

```console
go get github.com/amorey/gobus
```

Each architecture lives in its own subpackage:

```go
import "github.com/amorey/gobus/conflate"
```

Requires Go 1.21+.

## Event Bus Types

### Conflate

Conflate is a single-producer, multi-consumer keyed latest-value bus. Every value published through the singleton `Sender` is fanned out to *every* live `Receiver`, but each receiver holds **one slot per key** plus an insertion-ordered key queue. A `Send()` for a key that already has an undelivered value coalesces into that slot via a caller-supplied `Merge` function instead of appending, and the key keeps its original queue position. Because `conflate` keeps the latest value **per key**, a slow receiver catches up to the current state of *every* key in first-touch order and its memory stays bounded by the live key set rather than by write volume.

```go
hub := conflate.New[string, Update](merge)
defer hub.Close()

tx := hub.Sender()
defer tx.Close()

rx := hub.Receiver()
defer rx.Close()
go func() {
    for {
        ev, err := rx.Recv()
        if err != nil { return }
        // ev.Value is the *latest* Update for ev.Key, not every intermediate
        apply(ev.Key, ev.Value)
    }
}()

for _, u := range updates { tx.Send(u.Key, u) }
```

Receivers take composable options, minted by the hub. `WithKeyFilter` filters at *enqueue*, so an unwanted key never occupies a slot — that's how a consumer watching one key of a high-cardinality producer stays bounded by that one key. `WithMerge` gives a single consumer its own coalescing policy, which matters when consumers of the same producer disagree about what may be dropped; one hub-wide `Merge` cannot express that.

```go
rx := hub.Receiver(hub.WithKeyFilter(func(k string) bool { return k == "db-0" }))
rx := hub.Receiver(hub.WithMerge(stricter))
rx := hub.Receiver(hub.WithKeyFilter(wanted), hub.WithMerge(stricter))  // compose
```

#### Inspecting the backlog head

`Peek()` returns the oldest pending event without removing it — `TryRecv` minus the pop, sharing its precedence exactly: `ErrEmpty` when nothing is pending, `ErrClosed` when the receiver or hub is closed or the sender has closed and the queue has drained. It is not a raw read of the queue, so a closed handle reports `ErrClosed` even with a value at the head. The corollary matters if you track a cursor: `ErrClosed` is *not* a statement that the backlog was empty, because `Receiver.Close()` and `Hub.Close()` abandon whatever is still queued. Only `Sender.Close()` drains first.

The value you get back is the current merged contents of the head key's slot, so it can change between two `Peek`s while the head *key* does not — coalescing leaves queue position alone. Annihilation is the exception: a `Merge` returning `keep == false` for the head key removes it, and the next `Peek` reports a different key.

```go
ev, err := rx.Peek()   // what would Recv hand me next, without taking it?
```

Two cautions. `Peek` takes the same hub lock that serializes the entire `Send` fan-out, so polling it in a loop degrades every publisher and every other receiver on the bus — call it once per unit of work. And while it is safe to call from any goroutine, it is only *meaningful* on the receiver's single consuming goroutine: anything else consuming concurrently can take the event you just looked at. An event already handed to the `Chan` feeder has left the queue, so `Peek` reports `ErrEmpty` while that one event is in flight.

The intended use is a consumer that needs to know how far its backlog reaches — a resumable cursor or watermark. Fold the ordering quantity into `V` and let the bus's own `Merge` carry it: stamp each value at publication, keep the *earliest* stamp when two values for a key coalesce, and read it off the head. The bus makes no ordering claim of its own here, so the premise that publication order matches that quantity's order is yours to hold; if it doesn't, the head key's stamp is not the lowest one pending.

#### Publishing with no receivers

`Send` on a hub with no live receiver returns `nil` without taking the bus lock. That lock is hub-wide — every `Send` fan-out, every pop, `Recv`, `Peek`, `TryRecv` and `Close` serializes through it — so the cost of an unwatched hub otherwise lands on the *producer's* hot path, per publish, whether or not anything reads the result. This matters when values are published from inside a producer's own critical sections: without it, every write in a subsystem pays a mutex for a bus nobody is subscribed to.

The result is unchanged, only the cost: a send with no receivers already fanned out to nobody. `TrySend` and `SendContext` take the same path, and a cancelled `ctx` is still reported rather than swallowed. A closed sender still returns `ErrClosed` on an empty receiver set.

The corollary is an ordering requirement on **your** side, and it is the one thing to get right:

```go
rx := hub.Receiver()   // register FIRST
state := snapshot()    // then read your snapshot
```

Take the snapshot before registering and any value published in the gap reaches no receiver — conflate has no replay, and the next `Send` for that key coalesces into a slot your consumer was never told had been skipped. This has always been conflate's delivery model; a publisher that skips the lock simply makes it unforgiving.

[Recv Example](./conflate/examples/recv/main.go) · [Chan Example](./conflate/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gobus/conflate)

### Watch

Watch is a single-producer, multi-consumer keyed **state** bus. Where `conflate` streams events, `watch` distributes the *current value* of a key: each `Receiver` watches exactly one key and holds **one slot** for it, so a slow consumer skips to the current value rather than replaying what it missed.

A receiver is created by `Hub.Watch`, which is also where its baseline comes from — the caller passes the value it has just read, and `Receiver.Close()` is the matching unwatch.

```go
hub := watch.New[ObjectID](watch.WithAccept(func(prev, next Stamped) bool {
	return next.Seq > prev.Seq
}))
defer hub.Close()

// Read your state and register in one critical section: Watch calls no
// caller code, so it is safe under your own lock.
q.mu.Lock()
cur := q.current(id)
rx := hub.Watch(id, cur)
q.mu.Unlock()
defer rx.Close()

for ev := range rx.Chan() {
	use(ev.Value)
}
```

Three things distinguish it from `conflate`:

**Registration is the snapshot.** `Watch` takes the value you have just read and never hands it back — it is the baseline, not a delivery. Because `Watch` calls no caller code, you can read your state and register in one critical section, which removes the register-before-read ordering rule `conflate` imposes. This is the **opposite** of `gochan/watch`, whose registration deliberately does not snapshot; don't carry that rule across.

**`Accept` decides which value wins.** Instead of a `Merge`, `watch` takes an optional `Accept func(prev, next V) bool`. It runs under the bus lock once for each receiver watching the key, against *that receiver's own current value* — so one value can be new for a receiver that subscribed early and stale for one that subscribed late. This is what makes the settled value independent of which of two concurrent `Send` calls takes the lock first, provided your rule is a strict order over `V`. Without the option every value is accepted, which is last-writer-wins.

`Accept` is caller code running under the bus lock. It must not call back into the hub, and it must not take any lock a caller may hold while calling `Watch`, `Send` or `Close` — `Watch` is expressly safe under a producer's lock, so an `Accept` that takes that same lock inverts the two orders and deadlocks. Reading its two arguments and nothing else is always safe.

**One key per receiver.** There is no `Unwatch` and no key set: the constraint is structural, which is what removes the questions a mutable key set raises. A consumer watching N keys therefore holds N receivers and, if it uses `Chan()`, N goroutines — so `watch` is deliberately unsuited to wide subscriptions. A wide change-stream consumer wants `conflate`, which has the annihilation that a create-then-delete pair needs.

`Send` for a key nobody watches is dropped, and nothing is retained: there is no receiver and therefore no buffer. `Send` on a hub with no receiver at all returns `nil` without taking the bus lock, exactly as `conflate` does.

[Docs](https://pkg.go.dev/github.com/amorey/gobus/watch)


## Design notes

### Common interfaces

Every `Sender` and `Receiver` implements the common interfaces in [`gobus`](./gobus.go), so call sites can be swapped between architectures more easily. They mirror `gochan`'s, with a key threaded through:

```go
// The unit of delivery: every receive path returns one of these.
type Event[K comparable, V any] struct {
    Key   K
    Value V
}

type Sender[K comparable, V any] interface {
    Send(k K, v V) error                              // publishes v under k; never applies backpressure
    TrySend(k K, v V) error                           // returns ErrFull / ErrClosed immediately
    SendContext(ctx context.Context, k K, v V) error  // as Send, with cancellation
    Close()                                           // idempotent
}

type Receiver[K comparable, V any] interface {
    Recv() (Event[K, V], error)                            // blocks until an event is available or closed
    TryRecv() (Event[K, V], error)                         // returns ErrEmpty / ErrClosed immediately
    RecvContext(ctx context.Context) (Event[K, V], error)  // blocks with cancellation
    Chan() <-chan Event[K, V]                              // native channel for use with select
    Close()                                                // idempotent
}
```

`Event` is deliberately the single currency of the receive side: `Recv`, `TryRecv`, `RecvContext` and `Chan` all hand back the same type, so a handler written as `func(gobus.Event[K, V])` works against any of them. Returning the key alongside the value also means `V` doesn't have to redundantly embed it.

The send side stays unpacked — `Send(k, v)` rather than `Send(Event{...})` — because a publisher already has the key and value as separate values, and making it build a struct at every call site buys nothing.

As in `gochan`, there is intentionally no shared `Hub` interface — each multi-side package exposes its own concrete `*Hub[K, V]` so callers can't accidentally substitute one architecture for another. Every hub has the same shape:

```go
Sender()   *Sender[K, V]    // the singleton on single-Sender packages
Receiver() *Receiver[K, V]  // fresh handle per subscriber; may take per-package options
Close()                     // closes every live handle; idempotent
```

After `Hub.Close()`, returned handles report `ErrClosed` on use.

### Errors

```go
var ErrClosed = errors.New("gobus: bus closed")
var ErrEmpty  = errors.New("gobus: no pending events")
var ErrFull   = errors.New("gobus: bus full")
```

`conflate` never returns `ErrFull`: it has no capacity argument because coalescing bounds a receiver's buffer by the live key set rather than the write volume. `ErrFull` is reserved for future bounded bus types.

There is no `ErrLagged` equivalent. A conflate receiver that falls behind doesn't lose values it can be told about — it collapses them, which is the contract rather than an error condition.

#### Close / cancel precedence

`ErrClosed` outranks context cancellation in `SendContext`: a closed sender returns `ErrClosed` even for an already-cancelled `ctx`, since `ErrClosed` is the durable answer and a retry with a fresh context would only return it anyway. A cancelled `ctx` on a live sender still returns `ctx.Err()`.

`Send` never blocks, so `SendContext` consults `ctx` exactly once — there is no parked state for a cancellation to arrive in. That check happens where the send is *resolved*, not on entry. A live `ctx` on a hub with no live receiver resolves at the lock-free receiver count — nothing to publish, nothing to report. Every other send, including every cancelled one, resolves under the bus lock, so that closed and cancelled are read from one consistent view rather than from two reads taken a moment apart: a `ctx` that was live at the call but is cancelled by the time the send reaches the front of the lock returns `ctx.Err()` and publishes nothing. Waiting for that lock is real work — your `Merge` and key filters run under it — and a context is a bound on the publish actually happening, not on the function being entered. A cancellation racing the call can land either side of the lock; both outcomes are correct resolutions of that race.

`RecvContext` uses the same precedence, one rank longer: **closed > cancelled > value**. `ErrClosed` wins whenever the receive is terminal — the receiver or the hub is closed, or the sender has closed and this receiver's queue is drained — so a shutdown loop that cancels its own context can still drain to `ErrClosed` rather than spinning on `ctx.Err()`. Otherwise a cancelled `ctx` returns `ctx.Err()` *even when an event is pending*, and that event stays queued rather than being consumed. This is what keeps cancellation observable under load: without it, a consumer looping on `RecvContext` against a publisher fast enough to keep something always pending would take the value every iteration and never notice its own shutdown signal.

Because nothing is discarded, `ctx.Err()` is not an end-of-stream: with the sender closed and events still queued, every `RecvContext` on a cancelled `ctx` returns `ctx.Err()` and none of them reaches `ErrClosed`. Only draining to `ErrClosed` deregisters a receiver on its own, so a caller that stops on `ctx.Err()` must `Receiver.Close()` — otherwise the handle stays in the hub, still accumulating coalesced events, for the hub's lifetime. `defer rx.Close()` covers this. To consume what is left first, loop on `TryRecv` until it returns *any* error — `ErrEmpty` while the sender is open, `ErrClosed` once it has closed and the queue is drained.

The precedence is not just an entry-time check — it governs the parked path identically. A conflate wake carries no value and no verdict: the event stays in the receiver's slots until it is popped, and the wakeup only means "state changed, look again", so a parked call re-derives the whole closed > cancelled > value answer from state before returning. Whatever a parked receiver is woken by, the answer is the one that ranking gives for the state visible when it resumes — a close visible then reports `ErrClosed` (and deregisters) even if the cancellation is what did the waking.

What that cannot do is order two terminations the caller never ordered. If a close and a cancellation are issued from separate goroutines with no happens-before between them, whichever becomes visible first is the one the receive resolves against: a cancellation that lands while the close has not yet reached the bus lock returns `ctx.Err()`, exactly as it would if the caller had entered `RecvContext` a microsecond earlier or later. Both are terminal for that receive, so don't depend on which one you get.

The non-determinism there is only about which event arrives first, never about how the answer is derived from what has arrived. Whenever both are visible at the point the receive resolves, `ErrClosed` wins — every time, not usually. A bus that resolved such a wake by picking a ready `select` arm would be wrong even though its outcomes look similar from the outside. Cancellation still never consumes an event, whichever way the race falls.

Sender-close is the one termination that does not pre-empt a pending event: it is a graceful end-of-stream, so queued events drain first and `ErrClosed` follows only once nothing is left.

### Close semantics

| Call               | Effect                                                                                                                                 |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `Sender.Close()`   | Graceful end-of-stream. Each receiver drains its pending per-key values once, then sees `ErrClosed` / a closed `Chan`.                    |
| `Receiver.Close()` | This handle only. Other receivers and the sender keep running; this handle's pending values are abandoned and its `Chan` feeder shuts down. |
| `Hub.Close()`      | Hard tear-down: sender plus every live receiver, with no drain. Future `Hub.Receiver()` / `Hub.Watch()` calls return pre-closed handles.  |

All idempotent. Don't call `Hub.Close` concurrently with an active `Send` from another goroutine — it inherits the sender's close discipline.

A receiver that reaches the terminal `ErrClosed` after a `Sender.Close` drain deregisters itself from the hub, so a long-lived hub doesn't pin abandoned receivers. On `watch` that tear-down also releases the key's state, so a key costs nothing once its last watcher has gone by either exit path.

On `watch`, `Sender.Close()` drains at most one value per receiver — its slot holds one — and `Hub.Watch` after a `Sender.Close` returns a *live* handle that holds nothing unread, so its first read is terminal. Only `Hub.Close` returns pre-closed handles.

### Thread safety

Both packages' `Sender` is safe to share across goroutines: `Send` and `Close` both serialize through the hub lock, and `Send` first reads a lock-free receiver count so it takes that lock only when a receiver is registered.

A `Receiver` is intended for a single consumer goroutine in both. `conflate` relies on it — the receiver owns an insertion-ordered queue meant to be popped by one reader. `watch` treats it as intent rather than invariant: a receiver using `Chan()` genuinely has two readers (the feeder and any direct `TryRecv`), so its read position lives under the hub lock rather than in the reading goroutine.

### Chan support

`Chan()` returns a per-receiver **private** channel fed by a per-receiver goroutine, as in `gochan`'s `broadcast` and `watch`. `Receiver.Close()` closes it; `Sender.Close()` also closes it once the feeder has drained. Always `Close` the receiver when you stop reading or the feeder will leak.

The channel is unbuffered on purpose: coalescing continues in the receiver's per-key slots while the consumer is busy, so a fast publisher produces no backlog beyond the live key set. One caveat — an event already handed to the feeder has left the receiver's slots, so a `Send` for that key while the feeder is parked on delivery enqueues the key afresh rather than coalescing into the in-flight event.

`watch`'s feeder differs: it marks a value read only once the consumer takes it, so a newer value arriving mid-delivery makes the feeder re-snapshot instead of handing over the superseded one. That is a latency property, not a guarantee — once the feeder has committed to a delivery both arms of its select are ready and Go chooses at random, so a superseded value is sometimes delivered with the newer one immediately behind it. What holds is that values arrive in order and that a consumer which keeps reading converges on the current value.
