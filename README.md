# gobus

*Gobus is a small library of common event bus architectures for Go*

<img width="435" src="https://github.com/user-attachments/assets/b564ee83-8171-4063-8796-665695e60906" />

[![Go Reference](https://pkg.go.dev/badge/github.com/amorey/gobus.svg)](https://pkg.go.dev/github.com/amorey/gobus)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

## Introduction

This library is a collection of common event bus architectures for Go. It's designed as a sister library to [`gochan`](https://github.com/amorey/gochan) which is a collection of lower-level channel architectures. Unlike keyless channels, event buses pass messages between senders and receivers based on keys and the interesting design decisions are about what happens when several values for the same key are in flight at once. Currently these are the event bus architectures included in this library:

| Package                             | Senders | Receivers | Semantics                                                                        |
| ----------------------------------- | ------- | --------- | -------------------------------------------------------------------------------- |
| [`conflate`](./conflate/README.md)  | 1       | many      | Keyed latest-value fan-out: per-key coalescing, latest-wins or a caller's merge.  |
| [`watch`](./watch/README.md)        | 1       | many      | Keyed state bus: one receiver watches one key (or all keys) and holds one slot.   |

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

Keyed latest-value **fan-out**. Every value reaches every receiver, but each receiver holds one slot per key: a `Send` for a key that is still undelivered coalesces into that slot rather than queueing behind it. By default the newer value wins. `WithDefaultMerge` sets a `Merge` that combines values instead, or annihilates the slot. A slow receiver catches up to the current state of *every* key, in first-touch order, on memory bounded by the live key set instead of by write volume.

```go
hub := conflate.New[string, Update]()
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

Highlights: a hub-wide `WithDefaultMerge`; per-receiver `WithKey`, `WithKeyFilter` and `WithMerge` options; `Peek()` to read the backlog head without consuming it; `TryRecvAll()` to take the whole backlog as one atomic cut; a lock-free `Send` fast path when nobody is subscribed.

**[Full documentation →](./conflate/README.md)**

[Recv Example](./conflate/examples/recv/main.go) · [Chan Example](./conflate/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gobus/conflate)

### Watch

Keyed **state**. Where `conflate` streams events, `watch` distributes the *current value* of a key: each receiver holds one slot, so a slow consumer skips to the current value rather than replaying what it missed. `Hub.Watch` both registers a receiver for one key and takes its baseline — the value the caller has just read — and `Receiver.Close()` is the matching unwatch. `Hub.WatchAcross` registers one for every key, still with a single slot.

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

Three things distinguish it from `conflate`. **Registration is the snapshot**: `Watch` takes the value you have just read, never hands it back, and calls no caller code — so you can read your state and register in one critical section. **`Accept` decides which value wins**, evaluated per receiver against that receiver's own slot, so two concurrent sends settle on the same value whichever takes the lock first; omit it for last-writer-wins. **One key per receiver**, structurally — or every key, via `Hub.WatchAcross`, which still holds one slot and so collapses a burst across many keys into a single wake-up. A consumer that needs each key's *own* latest value wants `conflate` instead.

**[Full documentation →](./watch/README.md)**

[Recv Example](./watch/examples/recv/main.go) · [Chan Example](./watch/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gobus/watch)


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

`Peek` is deliberately *not* on the interface, even though both bus types now have one. It is a concrete-`*Receiver` accessor on each, because what "unread" means is architectural: conflate peeks the head of a queue, `watch` peeks a versioned slot, and they answer differently for a value already handed to the `Chan` feeder — conflate's has left the queue, `watch`'s has not. An interface method would have to either state a truth one side violates or say so little that no architecture-agnostic call site could be written against it.

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

Because nothing is discarded, `ctx.Err()` is not an end-of-stream: with the sender closed and events still queued, every `RecvContext` on a cancelled `ctx` returns `ctx.Err()` and none of them reaches `ErrClosed`. Only draining to `ErrClosed` deregisters a receiver on its own, so a caller that stops on `ctx.Err()` must `Receiver.Close()` — otherwise the handle stays in the hub, still accumulating coalesced events, for the hub's lifetime. `defer rx.Close()` covers this. To consume what is left first, `conflate` receivers offer `TryRecvAll` — one call takes the whole remaining queue, and the error tells you which state you stopped in: `ErrEmpty` while the sender is open, `ErrClosed` once it has closed and the queue is drained. On `watch`, or wherever you want them one at a time, loop on `TryRecv` until it returns *any* error. Neither flush is a substitute for the `Close`: against a still-open sender both end on `ErrEmpty`, which is not terminal and does not deregister.

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

All idempotent. Don't call `Hub.Close` concurrently with an active `Send` from another goroutine — it tears down the receivers that send is fanning out to. `Sender.Close` is the exception: both packages make it safe to call concurrently with a `Send`, see [Thread safety](#thread-safety).

A receiver that reaches the terminal `ErrClosed` after a `Sender.Close` drain deregisters itself from the hub, so a long-lived hub doesn't pin abandoned receivers. On `watch` that tear-down also releases the key's state, so a key costs nothing once its last watcher has gone by either exit path.

On `watch`, `Sender.Close()` drains at most one value per receiver — its slot holds one — and `Hub.Watch` after a `Sender.Close` returns a *live* handle that holds nothing unread, so its first read is terminal. Only `Hub.Close` returns pre-closed handles.

### Thread safety

Both packages' `Sender` is safe to share across goroutines: `Send` and `Close` both serialize through the hub lock, and `Send` first reads a lock-free receiver count so it takes that lock only when a receiver is registered.

That extends to closing while a send is in flight. Both packages promise `Sender.Close` is safe to call concurrently with a `Send` or `SendContext` from another goroutine: a racing send resolves to exactly one of two outcomes — it publishes and returns `nil`, or it returns `ErrClosed` and publishes nothing. There is no third outcome and no partial one, and which ordering wins is unspecified, so a caller that needs a value visible before shutdown must order the two itself. The promise holds because neither package's `Send` ever parks — the whole of `Close` runs under the hub lock, and the only step of a send outside it is the atomic load `Close` poisons. It is a promise about the two bus types that make it, not a rule a future one inherits, and it does **not** extend to `Hub.Close`, which keeps the discipline stated with the close table above.

A `Receiver` is intended for a single consumer goroutine in both. `conflate` relies on it — the receiver owns an insertion-ordered queue meant to be popped by one reader. `watch` treats it as intent rather than invariant: a receiver using `Chan()` genuinely has two readers (the feeder and any direct `TryRecv`), so its read position lives under the hub lock rather than in the reading goroutine.

### Chan support

`Chan()` returns a per-receiver **private** channel fed by a per-receiver goroutine, as in `gochan`'s `broadcast` and `watch`. `Receiver.Close()` closes it; `Sender.Close()` also closes it once the feeder has drained. Always `Close` the receiver when you stop reading or the feeder will leak.

The channel is unbuffered on purpose: coalescing continues in the receiver's per-key slots while the consumer is busy, so a fast publisher produces no backlog beyond the live key set. One caveat — an event already handed to the feeder has left the receiver's slots, so a `Send` for that key while the feeder is parked on delivery enqueues the key afresh rather than coalescing into the in-flight event.

`watch`'s feeder differs: it marks a value read only once the consumer takes it, so a newer value arriving mid-delivery makes the feeder re-snapshot instead of handing over the superseded one. That is a latency property, not a guarantee — once the feeder has committed to a delivery, anything making its select's other arms ready races that delivery and Go chooses at random. So a superseded value is sometimes delivered with the newer one immediately behind it, and a `Receiver.Close` or `Hub.Close` can likewise lose the race, delivering one value after it returns even though both abandon what is unread. What holds is that values arrive in order and that a consumer which keeps reading converges on the current value.
