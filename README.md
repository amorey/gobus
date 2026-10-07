# gobus

*Gobus is a small library of common event bus architectures for Go*

<img width="435" src="https://github.com/user-attachments/assets/b564ee83-8171-4063-8796-665695e60906" />

[![Go Reference](https://pkg.go.dev/badge/github.com/amorey/gobus.svg)](https://pkg.go.dev/github.com/amorey/gobus)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

## Introduction

An event bus carries values from a sender to many receivers. On a gobus every value travels under a **key**, and each bus type has its own rule for what happens when a receiver has not yet read the last value for a key and a new one arrives. That rule is the whole difference between the bus types.

This is the sister library to [`gochan`](https://github.com/amorey/gochan), which covers lower-level channel architectures for values without keys. Currently these are the event bus architectures included in this library:

| Package                             | Senders | Receivers | What a slow receiver sees                                                       |
| ----------------------------------- | ------- | --------- | ------------------------------------------------------------------------------- |
| [`conflate`](./conflate/README.md)  | 1       | many      | The latest value of **every** key it missed, one event per key.                 |
| [`watch`](./watch/README.md)        | 1       | many      | The current value of the **one** key it watches, skipping everything in between. |

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

Every receiver holds one **slot** per key. A slot is a single space that holds at most one unread value, and a new value for that key overwrites whatever is in it. So if a receiver has not yet read key `web-0` and a new value for `web-0` arrives, the new value takes the old one's place. A receiver that falls behind therefore never builds up a backlog: it catches up to the current state of every key, in the order it first saw them.

```go
hub := conflate.New[string, PodStatus]()
defer hub.Close()

tx := hub.Sender()
rx := hub.Receiver()
defer rx.Close()

go func() {
    for {
        ev, err := rx.Recv()
        if err != nil {
            return // gobus.ErrClosed after tx.Close() drains
        }
        // ev.Value is the latest status for ev.Key, not every intermediate one.
        apply(ev.Key, ev.Value)
    }
}()

tx.Send("web-0", PodStatus{Phase: "Running"})
tx.Send("web-0", PodStatus{Phase: "Terminating"}) // replaces the slot if unread
tx.Close()                                        // receivers drain, then ErrClosed
```

By default the newest value wins. Pass `conflate.WithDefaultMerge(fn)` to combine the two values instead, or to drop the key entirely (an "Added" followed by a "Deleted" that nobody saw can simply vanish). Per receiver you can pick one key (`hub.WithKey`), filter keys (`hub.WithKeyFilter`) or override the merge (`hub.WithMerge`). `Peek()` reads the next event without consuming it and `TryRecvAll()` takes the whole backlog in one call.

**[Full documentation →](./conflate/README.md)**

[Recv Example](./conflate/examples/recv/main.go) · [Chan Example](./conflate/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gobus/conflate)

### Watch

A `watch` receiver follows one key and holds one slot, so it has at most one unread value at any time. It is for state, not events: a consumer that falls behind reads the current value and skips the ones it missed. `hub.Watch(key)` subscribes and `rx.Close()` unsubscribes.

```go
hub := watch.New[string, Progress]()
defer hub.Close()

tx := hub.Sender()
rx := hub.Watch("build")
defer rx.Close()

go func() {
    for ev := range rx.Chan() { // closes after tx.Close() hands over the last value
        render(ev.Value) // the current progress, not every step
    }
}()

for pct := 1; pct <= 100; pct++ {
    tx.Send("build", Progress{Percent: pct})
}
tx.Close()
```

If you already hold the current value, pass it with `hub.Watch(key, hub.WithBaseline(cur))`. The bus does not hand that value back; it only uses it to judge what comes next. `Watch` calls no caller code, so you can read your state and subscribe under your own lock with no gap between the two. `watch.WithAccept(fn)` lets you decide which of two values wins, for example by sequence number, so two concurrent sends settle on the same value no matter which arrives first. `hub.WatchAcross()` follows every key with a single slot, so a burst across many keys wakes the receiver once with the last key that landed.

**[Full documentation →](./watch/README.md)**

[Recv Example](./watch/examples/recv/main.go) · [Chan Example](./watch/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gobus/watch)

## Design notes

### Common interfaces

Every `Sender` and `Receiver` implements the interfaces in [`gobus.go`](./gobus.go), so a call site written against them works with either bus type. They mirror `gochan`'s, with a key added:

```go
// The unit of delivery: every receive path returns one of these.
type Event[K comparable, V any] struct {
    Key   K
    Value V
}

type Sender[K comparable, V any] interface {
    Send(k K, v V) error                              // publishes v under k; never blocks
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

`Recv`, `TryRecv`, `RecvContext` and `Chan` all return `Event`, so one handler of type `func(gobus.Event[K, V])` serves every read path. Returning the key with the value also means `V` does not have to carry its own key.

The send side takes `k` and `v` separately because a publisher already has them separately. Building an `Event` at every call site would add nothing.

`Peek` exists on both receiver types but is not on the interface. What "unread" means differs by architecture: `conflate` peeks the head of a queue and `watch` peeks a versioned slot. They also answer differently for a value already handed to the `Chan` feeder. An interface method could not describe both honestly.

There is no shared `Hub` interface. Each package exposes its own concrete `*Hub[K, V]` so you cannot substitute one architecture for another by accident. Every hub has the same shape:

```go
Sender()   *Sender[K, V]    // the single sender
Receiver() *Receiver[K, V]  // a fresh handle per subscriber; watch calls this Watch(key)
Close()                     // closes every live handle; idempotent
```

After `Hub.Close()`, every handle reports `ErrClosed` on use.

### Errors

```go
var ErrClosed = errors.New("gobus: bus closed")
var ErrEmpty  = errors.New("gobus: no pending events")
var ErrFull   = errors.New("gobus: bus full")
```

`conflate` never returns `ErrFull`. It has no capacity argument, because a receiver's memory is bounded by the number of live keys rather than by how much was sent. `ErrFull` is reserved for future bounded bus types.

There is no `ErrLagged`. A receiver that falls behind does not lose values it needs to be told about. It collapses them, and that is the contract rather than an error.

#### Close / cancel precedence

Both `SendContext` and `RecvContext` rank their outcomes the same way: **closed > cancelled > value**. When more than one applies at the moment the call resolves, the higher-ranked one wins every time.

On the send side this means a closed sender returns `ErrClosed` even when `ctx` is already cancelled. `ErrClosed` is the durable answer, and a retry with a fresh context would only return it again. A cancelled `ctx` on a live sender returns `ctx.Err()` and publishes nothing.

`Send` never blocks, so `SendContext` checks `ctx` exactly once, at the point where the send is resolved. On a hub with no receivers that point is a lock-free count and the call returns `nil`. Every other send resolves under the bus lock, so closed and cancelled are read from one consistent view. A `ctx` that was live when you called but cancelled by the time the send got the lock returns `ctx.Err()`. Waiting for the lock is real work, since your `Merge` and key filters run under it, and the context bounds the publish rather than the function entry.

On the receive side, `ErrClosed` wins whenever the receive is terminal: the receiver or hub is closed, or the sender has closed and this receiver has nothing left to drain. So a shutdown loop that cancels its own context still drains to `ErrClosed` instead of spinning on `ctx.Err()`. Otherwise a cancelled `ctx` returns `ctx.Err()` even when an event is pending, and that event stays queued. Without this rule, a consumer looping on `RecvContext` against a fast publisher would take a value on every iteration and never notice its own shutdown.

Because cancellation never consumes anything, `ctx.Err()` is not an end-of-stream. Only draining to `ErrClosed` deregisters a receiver by itself. A caller that stops on `ctx.Err()` must call `Receiver.Close()`, or the handle stays in the hub for the hub's lifetime, still collecting values. `defer rx.Close()` covers this. To consume what is left first, `conflate` receivers offer `TryRecvAll`, whose error tells you which state you stopped in: `ErrEmpty` while the sender is open, `ErrClosed` once it has closed and the queue is drained. On `watch`, or if you want one at a time, loop on `TryRecv` until it returns any error. Neither flush replaces the `Close`: against a still-open sender both end on `ErrEmpty`, which does not deregister.

The ranking also governs a parked receive. A wakeup carries no verdict; it only means "state changed, look again". The receiver re-derives the full answer from the state it sees when it resumes, so a close visible at that point reports `ErrClosed` even if a cancellation is what woke it.

What the bus cannot do is order two terminations the caller never ordered. If a close and a cancellation come from separate goroutines with no happens-before between them, the receive resolves against whichever becomes visible first. Both are terminal, so do not depend on which one you get. What is guaranteed is that whenever both are visible at the moment the receive resolves, `ErrClosed` wins.

Sender-close is the one termination that does not pre-empt a pending event. It is a graceful end-of-stream: queued events drain first and `ErrClosed` follows once nothing is left.

### Close semantics

| Call               | Effect                                                                                                                                 |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `Sender.Close()`   | Graceful end-of-stream. Each receiver drains its pending values once, then sees `ErrClosed` / a closed `Chan`.                           |
| `Receiver.Close()` | This handle only. Other receivers and the sender keep running; this handle's pending values are abandoned and its `Chan` feeder stops.    |
| `Hub.Close()`      | Hard tear-down: the sender plus every live receiver, with no drain. Later `Hub.Receiver()` / `Hub.Watch()` calls return pre-closed handles. |

All three are idempotent. Do not call `Hub.Close` while another goroutine is inside `Send`: it tears down the receivers that send is delivering to. `Sender.Close` is the exception and is safe to race with a `Send`, see [Thread safety](#thread-safety).

A receiver that reaches the terminal `ErrClosed` after a `Sender.Close` drain deregisters itself from the hub, so a long-lived hub does not pin abandoned receivers. On `watch` that also releases the key's state, so a key costs nothing once its last watcher has gone.

On `watch`, `Sender.Close()` drains at most one value per receiver, since the slot holds one. `Hub.Watch` after a `Sender.Close` returns a live handle with nothing unread, so its first read is terminal. Only `Hub.Close` returns pre-closed handles.

### Thread safety

A `Sender` is safe to share across goroutines in both packages. `Send` and `Close` both serialize through the hub lock, and `Send` first reads a lock-free receiver count so it takes that lock only when a receiver is registered.

That extends to closing while a send is in flight. Both packages promise that `Sender.Close` is safe to call concurrently with a `Send` or `SendContext` from another goroutine. The racing send has exactly one of two outcomes: it publishes and returns `nil`, or it publishes nothing and returns `ErrClosed`. Which one you get is unspecified, so a caller that needs a value visible before shutdown must order the two itself. This holds because neither package's `Send` ever parks: all of `Close` runs under the hub lock, and the only step of a send outside it is the atomic load that `Close` poisons. The promise is made by these two bus types, not inherited by future ones, and it does not extend to `Hub.Close`.

A `Receiver` is meant for a single consumer goroutine. `conflate` relies on this, because the receiver owns an ordered queue meant to be popped by one reader. In both packages, mixing `Chan()` with `Recv`, `RecvContext` or `TryRecv` on one receiver is memory-safe, but the two readers split the values: a value taken directly never reaches the channel. Once you read through `Chan()`, read only through it.

### Chan support

`Chan()` returns a **private** channel per receiver, fed by a goroutine per receiver, as in `gochan`'s `broadcast` and `watch`. `Receiver.Close()` closes it. `Sender.Close()` also closes it once the feeder has drained. Always `Close` the receiver when you stop reading, or the feeder goroutine leaks.

The channel is unbuffered on purpose. Values keep coalescing in the receiver's slots while the consumer is busy, so a fast publisher produces no backlog beyond the live key set. One caveat on `conflate`: an event already handed to the feeder has left the receiver's slots, so a `Send` for that key while the feeder waits on delivery enqueues the key afresh instead of merging into the in-flight event.

`watch`'s feeder marks a value read only once the consumer takes it, so a newer value arriving mid-delivery makes the feeder re-read the slot instead of handing over the stale one. That is a latency property, not a guarantee. Once the feeder has committed to a delivery, anything that makes its other `select` arms ready races that delivery, and Go picks at random. So a superseded value is sometimes delivered with the newer one right behind it, and a `Receiver.Close` or `Hub.Close` can deliver one value after it returns. What always holds is that values arrive in order and a consumer that keeps reading converges on the current value.
