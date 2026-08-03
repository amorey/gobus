# watch

*Keyed state bus: one receiver, one key, one slot — always the current value.*

[![Go Reference](https://pkg.go.dev/badge/github.com/amorey/gobus/watch.svg)](https://pkg.go.dev/github.com/amorey/gobus/watch)

Part of [gobus](../README.md).

```go
import "github.com/amorey/gobus/watch"
```

## Contents

- [Overview](#overview)
- [Quick start](#quick-start)
- [Registration is the snapshot](#registration-is-the-snapshot)
- [Accept decides which value wins](#accept-decides-which-value-wins)
- [One key per receiver](#one-key-per-receiver)
- [Inspecting what is unread](#inspecting-what-is-unread)
- [Publishing with no receivers](#publishing-with-no-receivers)
- [Contexts and cancellation](#contexts-and-cancellation)
- [Close semantics](#close-semantics)
- [Chan support](#chan-support)
- [Thread safety](#thread-safety)
- [API reference](#api-reference)
- [Errors](#errors)
- [Examples](#examples)

## Overview

Watch is a single-producer, multi-consumer keyed **state** bus. Where
[`conflate`](../conflate/README.md) streams events, `watch` distributes the
*current value* of a key: each `Receiver` watches exactly one key and holds
**one slot** for it, so a slow consumer skips to the current value rather than
replaying what it missed.

A receiver is created by `Hub.Watch`, which is also where its baseline comes
from — the caller passes the value it has just read — and `Receiver.Close()` is
the matching unwatch.

Pick `watch` over `conflate` when a consumer follows a single object and only
its current state matters. Pick `conflate` for wide subscriptions and for
change streams where a create-then-delete pair must leave no residue.

> **Coming from [`gochan/watch`](https://github.com/amorey/gochan)?** Two rules
> differ. Registration here **does** snapshot (gochan's deliberately does not),
> and the hub holds no seed of its own — the baseline is per receiver, supplied
> by the caller. Don't carry the sister package's rule across.

## Quick start

```go
hub := watch.New[ObjectID](watch.WithAccept(func(prev, next Stamped) bool {
    return next.Seq > prev.Seq
}))
defer hub.Close()

// Read your state and register in ONE critical section: Watch calls no
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

Note the type arguments: `watch.New[ObjectID]` spells only `K`, because
`Option[V]` carries `V` alone and `WithAccept` infers it from its argument.

## Registration is the snapshot

`Watch` takes the value you have just read and **never hands it back** — it is
the baseline, not a delivery. It is the `prev` of the first `Accept` call, and
a receiver reads a value only once a `Send` supersedes it.

Because `Watch` calls no caller code, you can read your state and register in
one critical section, which removes the register-before-read ordering rule
`conflate` imposes. Nothing published in between can be lost, because there is
no "in between".

One consequence to expect: two subscribers registering at different moments
disagree about whether the same publish is news. A value can be new for a
receiver that subscribed early and stale for one that subscribed late — which
is exactly right, since each already holds what it read for itself.

## Accept decides which value wins

Instead of a `Merge`, `watch` takes an optional `Accept func(prev, next V) bool`
at hub construction:

```go
hub := watch.New[ObjectID](watch.WithAccept(func(prev, next Stamped) bool {
    return next.Seq > prev.Seq
}))
```

It runs under the bus lock, once for each receiver watching the key, against
*that receiver's own current value*. This is what makes the settled value
independent of which of two concurrent `Send` calls takes the lock first,
provided your rule is a strict order over `V` — a producer that computes a
change under its own lock and publishes after releasing it can reach `Send` in
the reverse of the order in which the changes became true, and `Accept` is what
resolves that.

Without the option every value is accepted, which is last-writer-wins. The
option may be omitted — a state bus has a meaningful identity rule, so omitting
it is a statement rather than an oversight — but passing a nil `Accept`, or a
nil `Option` to `New`, panics.

`Accept` is caller code running under the bus lock. It must not call back into
the hub, and it must not take any lock a caller may hold while calling `Watch`,
`Send` or `Close` — `Watch` is expressly safe under a producer's lock, so an
`Accept` that takes that same lock inverts the two orders and deadlocks.
Reading its two arguments and nothing else is always safe.

A rejected value changes nothing and the receiver is not told. A panic out of
`Accept` leaves a partial fan-out: the receivers already reached keep the
value, the rest are untouched, the send is not retried, and the hub stays
usable.

## One key per receiver

There is no `Unwatch` and no key set: the constraint is structural, which is
what removes the questions a mutable key set raises. A consumer watching N keys
therefore holds N receivers and, if it uses `Chan()`, N goroutines — so `watch`
is deliberately unsuited to wide subscriptions. A wide change-stream consumer
wants [`conflate`](../conflate/README.md), which has the annihilation that a
create-then-delete pair needs.

`Send` for a key nobody watches is dropped, and nothing is retained: there is
no receiver and therefore no buffer, so a later `Watch` never sees it. Once the
last receiver for a key goes — by `Close` or by reaching a terminal
`ErrClosed` — the hub releases the key entirely, so a key costs nothing after
its last watcher.

## Inspecting what is unread

`Peek()` returns the value a receive would hand back and leaves it unread —
`TryRecv` minus the take, sharing its precedence exactly: `ErrEmpty` when
nothing has superseded what this receiver has already seen, `ErrClosed` when
the receiver or hub is closed or the sender has closed and the final value has
been taken.

```go
ev, err := rx.Peek()   // what would Recv hand me next, without taking it?
```

It reports what is **unread**, not the key's current state. A caught-up
receiver gets `ErrEmpty` even though its slot holds a perfectly good value, and
a closed handle gets `ErrClosed` even with a value waiting. If you want the
current state on demand, keep your own copy of the last value read — the
reading goroutine already has it, and it costs no lock.

Between two `Peek`s the key is fixed, since a receiver watches one key for
life, but the value is not: a `Send` your `Accept` takes replaces the slot, so
the second `Peek` reports the newer value and the older one is never handed
back by either path. That is the same skip-ahead every read on this bus is
subject to, only visible without consuming.

Two cautions, as on `conflate`. `Peek` takes the same hub lock that serializes
the entire `Send` fan-out, so polling it in a loop degrades every publisher and
every other receiver on the bus — call it once per unit of work. And while it
is safe to call from any goroutine, it is only *meaningful* on the receiver's
single consuming goroutine. Unlike `conflate`'s `Peek`, a value already handed
to the `Chan` feeder is still visible here: the feeder marks it read only once
the consumer has taken it.

## Publishing with no receivers

`Send` on a hub with no receiver at all returns `nil` without taking the bus
lock, exactly as `conflate` does — the hub-wide lock is pure cost when there is
nobody to fan out to. The result is unchanged, only the cost. `TrySend` and
`SendContext` take the same path, a cancelled `ctx` is still reported rather
than swallowed, and a closed sender still returns `ErrClosed`.

The subscriber-side ordering rule this forces on `conflate` does **not** apply
here: `Watch` takes your snapshot as its argument, so there is no gap between
reading state and registering for it to fall into. That is the point of
[registration being the snapshot](#registration-is-the-snapshot).

## Contexts and cancellation

Precedence is **closed > cancelled > value** on the receive side and **closed >
cancelled** on the send side; both are pinned by the module's conformance
suite. The full rationale lives in the [root README](../README.md#close--cancel-precedence).
What is watch-specific:

- `SendContext` never blocks, so `ctx` is consulted exactly once, where the
  send is *resolved* — under the bus lock, not on entry. A `ctx` cancelled
  while the call waited for the lock publishes nothing and returns `ctx.Err()`.
  Waiting for that lock is real work: your `Accept` runs under it.
- `RecvContext` returns `ctx.Err()` *even when an unread value is waiting*, and
  leaves that value unread. Without it, a consumer looping against a fast
  publisher would take a value every iteration and never notice its own
  shutdown.
- `ctx.Err()` is not an end-of-stream and does not deregister the receiver. A
  caller that stops on it must `Close` the handle, or it holds its key against
  the hub for the hub's lifetime. `defer rx.Close()` covers it.
- To consume what is left first, loop on `TryRecv` until it returns *any*
  error. That flush is not a substitute for the `Close`: against a still-open
  sender it ends on `ErrEmpty`, which is not terminal and does not deregister.

## Close semantics

| Call               | Effect                                                                                                                       |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `Sender.Close()`   | Graceful end-of-stream. A receiver holding an unread value reads it once more, then sees `ErrClosed`; a caught-up receiver sees `ErrClosed` at once. |
| `Receiver.Close()` | The unwatch. This handle only: any unread value is discarded and the key is dropped from the hub once no other receiver watches it. |
| `Hub.Close()`      | Hard tear-down: sender plus every live receiver, with no drain. Future `Hub.Watch()` calls return pre-closed handles.           |

All idempotent. Don't call `Hub.Close` concurrently with an active `Send` from
another goroutine — it tears down the receivers that send is fanning out to.

`Sender.Close` **is** safe to call concurrently with a `Send` or `SendContext`
from another goroutine. The two serialize, so a racing send resolves to exactly
one of two outcomes — it publishes and returns `nil`, or it returns `ErrClosed`
and publishes nothing. There is no third outcome and no partial one. Which
ordering wins is unspecified: a caller that needs a value visible before
shutdown must order the two itself, and a caller shutting down that doesn't care
whether the last value lands needs no fence on its write path. This holds
because `watch`'s `Send` never parks; it is a promise about this package, not a
module-wide rule.

A receiver's slot holds one value, so `Sender.Close()` drains **at most one
value per receiver**. `Hub.Watch` after a `Sender.Close` returns a *live*
handle that holds nothing unread, so its first read is terminal — only
`Hub.Close` returns pre-closed handles.

A receiver that reaches a terminal `ErrClosed` deregisters itself, releasing
its key, so a long-lived hub pins neither abandoned receivers nor their keys by
either exit path.

"No drain" on `Hub.Close` is a statement about the reading methods, which
report `ErrClosed` at once. A `Chan` consumer can still receive one value after
`Close` returns; see below.

## Chan support

`Chan()` returns a per-receiver **private** channel fed by a per-receiver
goroutine, carrying the same `gobus.Event` values the `Recv` methods return.
Repeated calls return the same channel.

```go
for ev := range rx.Chan() {
    use(ev.Value)
}
```

It is unbuffered, so a fast publisher builds no backlog: while the consumer is
not reading, further sends only update the slot. The feeder marks a value read
only once the consumer has taken it, so a newer value arriving mid-delivery
makes the feeder **re-snapshot** rather than hand over the superseded one —
which is what keeps a `Chan` consumer on the same latest-value footing as a
`Recv` caller. (It is also why `Peek` still sees a value in flight, unlike on
`conflate`.)

That is a latency property, not a guarantee. Once the feeder has committed to a
delivery, anything making its select's other arms ready races that delivery,
and Go chooses between ready arms at random. Two consequences:

- A superseded value is sometimes delivered, with the newer one immediately
  behind it.
- A `Receiver.Close` or `Hub.Close` can lose the race too, so one value can
  still be received after either returns, even though both abandon what is
  unread. The channel closes immediately after.

What holds is that values arrive **in order** and that a consumer which keeps
reading **converges on the current value**.

The channel closes when the feeder observes receiver-close, or
sender/hub-close with nothing left to drain. Abandoning the channel without
calling `Receiver.Close()` pins the feeder goroutine. **Always `Close` the
receiver when you stop reading.**

## Thread safety

`Sender` is safe to share across goroutines: `Send` and `Close` both serialize
through the hub lock, and `Send` first reads a lock-free receiver count so it
takes that lock only when a receiver is registered. `Send` then touches only
the receivers watching its key.

A `Receiver` is intended for a single consumer goroutine, but `watch` treats
that as intent rather than invariant: a receiver using `Chan()` genuinely has
two readers (the feeder and any direct `TryRecv`), so its read position lives
under the hub lock rather than in the reading goroutine. `Peek` and `TryRecv`
are safe from any goroutine; they are only *meaningful* on the consuming one,
since a concurrent reader can take the value between your two calls.

`Watch` calls no caller code and is safe to call while holding your own lock.
`Accept` is the constraint that keeps that true — see
[above](#accept-decides-which-value-wins).

## API reference

### Hub

```go
func New[K comparable, V any](opts ...Option[V]) *Hub[K, V]
func WithAccept[V any](fn Accept[V]) Option[V]
```

`New` panics if any option is nil; `WithAccept` panics if `fn` is nil. The
option is package-level rather than a method on the hub because it has to be
built before the hub exists, and it carries `V` alone so a call site spells
only `K`.

```go
func (h *Hub[K, V]) Sender() *Sender[K, V]
func (h *Hub[K, V]) Watch(k K, initial V) *Receiver[K, V]
func (h *Hub[K, V]) Close()
```

`Sender` returns the singleton send-side handle; repeated calls return the same
one. `Watch` makes a receiver for `k` seeded with `initial` as the baseline;
after `Hub.Close` the returned handle is pre-closed, and after `Sender.Close`
it is live but holds nothing unread.

### Accept

```go
type Accept[V any] func(prev, next V) bool
```

Reports whether `next` replaces `prev` in a receiver's slot. Evaluated per
receiver under the bus lock, against that receiver's own current value.

### Sender

```go
func (tx *Sender[K, V]) Send(k K, v V) error
func (tx *Sender[K, V]) TrySend(k K, v V) error
func (tx *Sender[K, V]) SendContext(ctx context.Context, k K, v V) error
func (tx *Sender[K, V]) Close()
```

`Send` never blocks. `TrySend` is equivalent to it — there is no separate
non-blocking path — and exists to satisfy `gobus.Sender`. All three return
`ErrClosed` once the sender or hub is closed. `watch` never returns `ErrFull`.

### Receiver

```go
func (rx *Receiver[K, V]) Recv() (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) RecvContext(ctx context.Context) (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) TryRecv() (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) Peek() (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) Chan() <-chan gobus.Event[K, V]
func (rx *Receiver[K, V]) Close()
```

`Peek` is a concrete-`*Receiver` accessor, not part of the `gobus.Receiver`
interface; everything else on this list implements it. There is no
`TryRecvAll` — a receiver holds one slot, so there is no backlog to take.

## Errors

| Error             | Returned by                  | Means                                                                                        |
| ----------------- | ---------------------------- | ---------------------------------------------------------------------------------------------- |
| `gobus.ErrClosed` | every send and receive path  | The receiver or hub is closed, or the sender is closed and the final value has been taken.       |
| `gobus.ErrEmpty`  | `TryRecv`, `Peek`            | Nothing has superseded what this receiver has already seen. Not terminal.                        |
| `ctx.Err()`       | `SendContext`, `RecvContext` | The context was cancelled. Not terminal, and consumes nothing.                                   |

`ErrFull` is never returned: a receiver holds one slot, which the next accepted
value overwrites, so there is no capacity to exhaust. There is no `ErrLagged`
equivalent either — skipping to the current value is the contract rather than
an error condition.

## Examples

- [`examples/recv`](./examples/recv/main.go) — a scheduler publishing job state
  outside its own lock, with `Accept` resolving the resulting reordering by
  sequence number, and a graceful `Sender.Close`.
  `go run ./watch/examples/recv`
- [`examples/chan`](./examples/chan/main.go) — the same bus consumed through
  `Chan()` and `select`, showing values skipping forward under a fast producer
  and two different shutdown paths. `go run ./watch/examples/chan`

[Package docs on pkg.go.dev](https://pkg.go.dev/github.com/amorey/gobus/watch)
