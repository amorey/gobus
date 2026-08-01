# Specification: package `gobus/watch`

**Status:** approved for implementation. Decisions D1 to D6 are closed. The
requester answered them in `docs/specs/gobus-keyed-watch-reply.md`, and section
9 records each answer.

One review round followed that reply and changed four requirements: R13a (a
deadlock that R21 invites), the removal of the lock-free `TryRecv` fast path in
section 10, R16 and R16a (an order guarantee that is the caller's, not the
bus's), and R50 (`New` and a nil option). R45 also weakened to R45a to R45c.
The requester relies on the old reading of R45 and must be told. See section 12.
**Package:** `github.com/amorey/gobus/watch`
**Source request:** `docs/gobus-keyed-watch.md` — the original proposal, which
this specification supersedes. In this repository that path still holds the
proposal, not an older copy of this document.
**Requester reply:** `docs/specs/gobus-keyed-watch-reply.md`
**Language:** This document uses Simplified Technical English. Sentences are
short and active. Each term has one meaning. See "Terms".

## 1. Purpose

The `watch` package is a keyed state bus. It holds one value for each watched
key. A receiver reads the current value of one key. A receiver does not read a
history of events.

The package gives a consumer three things:

- a baseline at subscribe time. The consumer supplies the value that it just
  read, and the bus measures every later value against it;
- a skip to the current value, so a slow consumer does not read stale values;
- a caller rule that rejects a value, so a consumer does not read a value that
  its own rule says is old.

The bus does not deliver the baseline back to the consumer. R19 states this.

## 2. Terms

| Term | Meaning |
| --- | --- |
| Hub | The construction handle. It makes one sender and many receivers. |
| Sender | The send-side handle. Each hub has one sender. |
| Receiver | A receive-side handle. Each receiver watches exactly one key. |
| Key | The identity of one watched item. |
| Value | The state of one key at one moment. |
| Watch | The act of making a receiver for one key. |
| Slot | The storage in one receiver. It holds the current value of that receiver's key. |
| Accept | A caller function. It reports whether a new value replaces the value in a slot. |
| Version | An internal counter. The bus owns it. It is not user-facing. |

## 3. Scope

This specification defines the public API and the behavior of the package. It
does not define the internal data structures. Section 10 gives implementation
notes only.

## 4. Relation to `conflate`

The `conflate` package is a keyed **event** bus. Values queue for each key.
Values coalesce through a caller `Merge`. A value leaves the receiver at
delivery.

The `watch` package is a keyed **state** bus. One slot holds the current value
of a key. A new receiver starts from a value. A slow receiver skips to the
current value.

For a gauge, "what happened" is not a question. There is only the current
value. A consumer that uses an event bus for a gauge builds two things by hand:
a value at subscribe time, and a staleness filter. This package gives both.

The difference also decides the memory bound:

- In `conflate`, the key set comes from the traffic. The package cannot know
  when a key ends. Any per-key state must therefore grow without a bound.
- In `watch`, a receiver declares one key. The state for that key exists
  because the receiver exists. The state ends when the receiver closes.

## 5. Public API

```go
package watch

// Accept reports whether next replaces prev in a receiver's slot.
type Accept[V any] func(prev, next V) bool

type Option[V any] func(*config[V])

// WithAccept sets the rule that decides whether a value replaces the value in
// a slot. The default accepts every value. Panics if fn is nil.
func WithAccept[V any](fn Accept[V]) Option[V]

func New[K comparable, V any](opts ...Option[V]) *Hub[K, V]

// Watch makes a receiver for one key, seeded with initial.
func (h *Hub[K, V]) Watch(k K, initial V) *Receiver[K, V]
func (h *Hub[K, V]) Sender() *Sender[K, V]
func (h *Hub[K, V]) Close()

func (tx *Sender[K, V]) Send(k K, v V) error
func (tx *Sender[K, V]) TrySend(k K, v V) error
func (tx *Sender[K, V]) SendContext(ctx context.Context, k K, v V) error
func (tx *Sender[K, V]) Close()

func (rx *Receiver[K, V]) Recv() (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) RecvContext(ctx context.Context) (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) TryRecv() (gobus.Event[K, V], error)
func (rx *Receiver[K, V]) Chan() <-chan gobus.Event[K, V]
func (rx *Receiver[K, V]) Close()
```

Call sites read as follows. Only `K` needs a type argument, because `V` comes
from the option:

```go
h := watch.New[string](watch.WithAccept(func(prev, next Val) bool {
	return next.Seq > prev.Seq
}))

h := watch.New[string, Val]()   // no option: V has nothing to infer from
```

The send side satisfies `gobus.Sender[K, V]`. The receive side satisfies
`gobus.Receiver[K, V]`. No signature carries a version.

The hub has no `Receiver` method. `Watch` takes its place. A receiver is a
watch, and `Receiver.Close` is the unwatch.

## 6. Requirements

### 6.1 One key for each receiver

**R1.** A receiver watches exactly one key. The hub fixes the key at `Watch`,
and the key does not change.

**R2.** The receiver has no `Watch` method and no `Unwatch` method. `Watch` is
a method on the hub. `Receiver.Close` is the unwatch.

Reason: a single-key receiver is a structural rule. A rule that says "do not
call `Watch` twice" is a rule that an implementation can break. This rule a
signature enforces. It also removes four questions that a multi-key receiver
raises: what a repeat `Watch` does, what `Unwatch` does to an unread value, how
the receiver orders two keys, and how a bulk subscribe stays cheap.

**R3.** Many receivers can watch one key. Each receiver holds its own slot and
its own read position. A `Send` reaches all of them.

**R4.** A consumer that watches N keys holds N receivers. The consumer merges
them. The package does not merge them.

The package is therefore not suited to a consumer that watches many keys.
`Chan` starts one goroutine for each receiver, so N keys cost N goroutines. The
package documentation must state this limit. See rejected alternative A2.

**R4a.** R4 must not become a reason to widen this package.

A consumer that watches many keys is a change-stream consumer. It reports
create, modify and delete, and a create-then-delete pair needs annihilation.
`conflate` serves that consumer, and R29 states that this package has no
annihilation. A future request to widen `watch` is a request that belongs to
the other bus. The requester states this in its reply, section 3, D5.

### 6.2 State and memory

**R5.** The bus holds state only for a key that a live receiver watches. When
the last receiver for a key closes, the bus holds nothing for that key.

**R6.** The memory of a receiver is one slot. Write volume does not change it.
The memory of the hub is bounded by the number of live receivers.

**R7.** R5 is a guarantee to the caller. It is not an implementation detail.
The package documentation must state it.

### 6.3 Accept

**R8.** `Accept` decides whether a value enters a slot. The bus calls it at
`Send`, one time for each receiver that watches the key. `prev` is the current
value of that receiver's slot. `next` is the value from `Send`.

**R9.** A `true` result replaces the slot value and marks it unread. A `false`
result changes nothing. The receiver never learns that a value was rejected.

**R10.** `Accept` runs for each receiver, against that receiver's own slot. The
bus must not evaluate it one time for the whole hub.

Reason: two receivers of one key hold different values, because each one
supplied its own `initial` at a different moment. A value can therefore be new
for one receiver and old for another. Only a per-receiver call gives both the
right answer.

**R11.** The default `Accept` returns `true` for every value. A hub built with
no option therefore keeps the newest value.

**R12.** `WithAccept` panics when `fn` is nil. The package does not substitute
the default for a nil function.

Reason: this follows `conflate`, where `WithKeyFilter` and `WithMerge` panic on
a nil function. A caller that passes a policy states a policy.

**R13.** `Accept` runs under the hub lock. It must not call back into the hub.

**R13a.** `Accept` must not take any lock that a caller can hold while it calls
`Watch`, `Send` or a `Close` method.

Reason: R21 invites a caller to call `Watch` under its own producer lock. That
caller therefore takes its lock and then the hub lock. `Send` takes the hub
lock and then calls `Accept`. An `Accept` that takes the producer lock takes
the two locks in the opposite order, and the two orders deadlock.

R13 alone is too weak. It forbids re-entry into the hub, and this hazard needs
no re-entry: an `Accept` that reads the producer's own state is enough.

The safe rule is that `Accept` reads its two arguments and nothing else. A rule
that needs more state must copy that state into `V` at `Send`.

**R14.** A panic out of `Accept` must release the hub lock. A caller that
recovers must not find the hub locked for ever.

**R14a.** A panic out of `Accept` leaves a partial fan-out. Every receiver that
the `Send` reached before the panic keeps the value that `Accept` took. Every
receiver after it is unchanged. The hub stays usable, and the value is not
retried.

The package documentation must state this. A caller that cannot accept a
partial fan-out must not panic in `Accept`.

**R15.** `Accept` receives the value only. It does not receive the key.

Reason: a receiver watches one key, so the key is a constant for every call
against one slot. `conflate.Merge` also takes no key. See D3.

**R16.** `Accept` lets a caller make the settled slot value independent of the
order in which two senders take the hub lock.

Two goroutines that call `Send` at the same time serialize on the hub lock.
Which one runs first is an arrival order, and the bus does not control it. The
second call sees the value of the first as `prev`.

"Settled slot value" means the value in the slot after both calls have
returned. It does not mean the sequence that a reader observes. A reader
between the two calls sees one value under one order and another value under
the other order, and no rule changes that.

**R16a.** R16 is a property of the caller's rule, not of the bus.

The bus gives order-independence only when `Accept` is a strict order over `V`:
`Accept(a, b)` and `Accept(b, a)` must not both be true. The default rule
(R11) is not such an order, so a hub with no option settles on the value of
whichever `Send` takes the lock second.

The package documentation must state this condition. A caller that supplies a
rule that is not a strict order gets last-writer-wins, which is what R11
already gives.

### 6.4 Watch

**R17.** `Watch` makes a receiver, binds it to `k`, and sets the slot to
`initial`.

**R18.** `initial` is the `prev` for the first call to `Accept` on that
receiver. This is true whether or not `Watch` delivers `initial`. See D2.

**R19.** `Watch` marks `initial` as **read**. The bus does not deliver
`initial`. A receiver reads a value only after a `Send` that `Accept` takes.

Reason: the caller supplies `initial`. To deliver it is to return the caller's
own argument to the caller. This differs from `gochan/watch`, where the hub
holds one seed from `New` and a later receiver never had that value.

R19 also gives `gobus.ErrEmpty` its useful meaning. See R27.

**R20.** `Watch` must not call caller code. A caller can therefore call `Watch`
while the caller holds its own lock. R21 depends on this.

`Send` does call caller code, through `Accept`. `Watch` does not.

**R21.** A caller that calls `Watch` under the producer's lock does not lose a
value.

The producer changes the state under its lock, releases the lock, and then
sends. A consumer that holds the same lock therefore blocks any new change. Any
`Send` that follows the consumer's `Watch` finds the receiver in place.

**R22.** A caller that supplies no `Accept` can get one duplicate value.

A change can be complete before the consumer takes the lock, while its `Send`
is not yet made. The `initial` of the consumer then already holds that change,
and the later `Send` delivers it a second time.

An `Accept` that rejects an equal or older value removes this duplicate,
because `initial` is the `prev` of that first call. This is the main reason a
caller supplies `Accept`. A caller that supplies none must compare values
itself.

**R23.** `Watch` returns a pre-closed receiver after `Hub.Close`. It does not
return `nil`, and it does not panic.

### 6.5 Delivery

**R24.** Every receive path returns a `gobus.Event[K, V]`. `Recv`, `TryRecv`,
`RecvContext` and `Chan` all use this type. The `Key` field always holds the
key that the receiver watches.

The key is constant for one receiver. The event type still carries it, because
`gobus.Receiver` defines the type and because a consumer that merges several
receivers needs it.

**R25.** `Send` must not block. A slow receiver must not apply backpressure to
the producer.

**R26.** A receiver that has not read its slot delivers only the newest
accepted value. The receiver skips intermediate values.

**R27.** `TryRecv` returns `gobus.ErrEmpty` when the slot holds no unread
value.

With R19, `gobus.ErrEmpty` means "nothing changed since you subscribed". Had
`Watch` delivered `initial`, it would have meant "you have not read your own
seed", which serves a consumer less well.

**R28.** The package must not return `gobus.ErrFull`.

**R29.** The package has no annihilation. An `Accept` that returns `false`
keeps the old value. It does not remove the key. A producer that must report
"this key is gone" encodes that fact in the value.

The documentation must state that `V` is often a pointer, or a struct with an
"exists" field.

### 6.6 The send fast path

**R30.** `Send` must not take the hub lock when the hub has no live receiver.

This is the check that `conflate.Send` gained in v0.2.1, and it is the check
that the original request asked for.

**R31.** `Send` for a key that no receiver watches should also skip the lock,
where the toolchain allows it. This is an optimization. It is not a guarantee,
and a caller must not depend on it. Section 10 gives the reason and the plan.

**R32.** Neither R30 nor R31 may answer "is the bus closed". Both answer "is
there work". A closed hub must always reach the locked path.

### 6.7 Cancellation and precedence

**R33.** The send side resolves closed before cancelled.

**R34.** The receive side resolves closed before cancelled before value.

**R35.** `SendContext` reads the context at the point where it resolves the
send. It must not read the context only at entry.

Reason: a cancellation that arrives while the call waits for the hub lock must
take effect. The package must not publish for a context that has expired.

**R36.** A cancelled receive must not consume a value. The value stays for a
later receive.

**R37.** `ctx.Err()` is not an end of stream. It does not close the receiver. A
caller that stops on `ctx.Err()` must call `Close`.

R33 to R37 repeat the contract in `gobus.go`. The conformance suite tests them.

### 6.8 Close

**R38.** `Sender.Close` is the soft path. A receiver that holds an unread value
reads it one more time. Later calls return `gobus.ErrClosed`.

**R39.** `Hub.Close` is hard tear-down. The hub closes the sender and every
live receiver at once. There is no drain.

**R40.** `Receiver.Close` is the unwatch. It closes one handle and discards any
unread value. The other receivers and the sender continue.

**R41.** A receiver that leaves the hub removes the key from the index when no
other receiver watches it. R5 depends on this.

A receiver leaves the hub by two paths, and both carry this obligation:
`Receiver.Close`, and a terminal `gobus.ErrClosed` under R43. R5 holds only if
both drop the key.

**R42.** All three `Close` methods are idempotent.

**R43.** A receiver that reaches a terminal `gobus.ErrClosed` closes itself and
leaves the hub, with the index removal that R41 states. The hub must not hold a
drained receiver.

The tear-down must happen under the lock that found the state terminal. This
follows `conflate`, and it is the reason the terminal verdict cannot move to a
lock-free check.

### 6.8a States that would otherwise be undefined

**R43a.** `Watch` after `Sender.Close` returns a live receiver that holds no
unread value. Its first read is therefore a terminal `gobus.ErrClosed`, and
R43 applies at once. R23 covers `Hub.Close` only, and the two differ.

**R43b.** A receiver that is never read still holds its key in the index until
`Receiver.Close`. R5 is a guarantee about closed receivers, not about idle
ones. A caller that abandons a receiver without closing it pins that key, as
R47 says it pins the feeder.

**R43c.** `Send` for a key that no receiver watches is dropped. The bus retains
nothing and no later `Watch` sees it. This differs from a reader's likely
assumption, because `conflate` buffers per receiver; here there is no receiver
and therefore no buffer.

**R43d.** `Hub.Sender` after `Hub.Close` returns the same sender handle, which
reports `gobus.ErrClosed` on use. It does not return `nil`.

**R43e.** Do not call `Hub.Close` at the same time as an active `Send` from
another goroutine. `watch` inherits this from `conflate`, which disclaims it
for the same reason.

### 6.9 Channel

**R44.** `Chan` returns one channel for each receiver. Repeated calls on one
receiver return the same channel.

**R45.** The feeder replaces a value that waits for delivery, where it can. A
newer accepted value replaces an older one that the consumer has not taken.

R45 is a latency property. It is not a correctness property. Two guarantees
follow it, and they are what an implementation can hold:

**R45a.** The feeder never delivers an older value after a newer one. The
sequence that a consumer reads is monotonic under `Accept`.

**R45b.** The feeder always delivers the newest accepted value in the end. A
consumer that keeps reading converges on the current value.

**R45c.** The feeder can still deliver a value that a newer one has
superseded.

Once the feeder is committed in its delivery select and a newer value lands,
both arms of that select are ready, and Go chooses between ready arms at
random. The older value is therefore delivered sometimes. R45b then delivers
the newer one straight after, so the consumer converges either way.

The package documentation must state R45c. A consumer must not read R45 as
"every value I read is current at the moment I read it". No bus that hands a
value to a channel can promise that, because the value is chosen before the
consumer takes it.

**R46.** `Receiver.Close` closes the channel. `Sender.Close` closes the channel
after the feeder drains.

**R47.** A caller that abandons the channel without a `Close` leaks the feeder.
The documentation must state this.

### 6.10 Surface that the package does not have

**R48.** The package has no `Hub.Receiver`, and `Watch` takes no options.
`Watch` fixes the key, so `WithKeyFilter` has no meaning. `Accept` is a hub
option, not a watch option. See D4.

**R49.** The package has no `Peek` in the first release. `Peek` is additive.
The coverage gate makes each exported symbol cost tests.

**R50.** Two functions panic, and no others.

- `WithAccept` panics on a nil `Accept`, per R12.
- `New` panics on a nil `Option`. `New` takes `...Option[V]`, so
  `New[string, Val](nil)` is legal Go. Without a check it panics on a nil
  function call, with no message that names the package.

`conflate.Hub.Receiver` panics explicitly on a nil `ReceiverOption`, and
CLAUDE.md states this as a convention. `New` must do the same, with a message
of the same shape.

`Watch` takes no function and has no nil-policy check.

**R51.** The package must not import `github.com/amorey/gochan`. The two
modules are sister libraries. They are not layers. `internal/buscore` sets the
precedent: `gobus` copies a building block, and it does not import one.

## 7. Documentation requirements

**R52.** The package documentation must state that `Watch` takes the value, and
that registration is therefore the snapshot.

This rule is the opposite of `gochan/watch`. That package documents that
registration does **not** snapshot the slot, because one hub-wide slot already
holds the current value. The two packages have the same name in sister modules
and opposite rules. A reader who knows `gochan/watch` gets this wrong. The rule
belongs in the first paragraph.

**R53.** The documentation must state the whole `Accept` contract:

- R13 and R13a: it runs under the hub lock, it must not call back into the hub,
  and it must take no lock that a caller can hold while it calls `Watch`,
  `Send` or `Close`. State the deadlock that R13a prevents, because R21
  actively invites the pattern that creates it.
- R14a: a panic leaves a partial fan-out.
- R16 and R16a: it lets the caller make the settled slot value independent of
  the senders' arrival order, and only when the rule is a strict order over
  `V`.

**R54.** The documentation must state R22, and it must show an `Accept` that
rejects an equal or older value.

**R55.** The documentation must state R4: one receiver watches one key, and a
wide consumer pays one goroutine for each key.

**R56.** `README.md` must gain a section for the package in the same change
that adds the package.

**R57.** `gobus.go` keeps the module-wide contract. A change to the public
close behavior or cancel behavior updates `gobus.go`, `README.md` and the
conformance suite.

This package needs no such change, and R57 is satisfied without an edit.
`SendContext` is already documented as "consulted exactly once, at the point
the send is resolved", which R35 matches, and `TryRecv`'s `gobus.ErrEmpty`
wording already fits R27. An implementer should not go looking for one.

## 8. Rejected alternatives

### A1. A shared watch for one key

One watch object for each key, shared by every receiver of that key. This looks
better for fan-out. It is rejected for three reasons.

**The seed is per receiver.** Each consumer registers the value that the
consumer read. Two consumers that subscribe at different moments read different
values. A shared watch holds the value of the first caller only. A later
receiver then has two options, and both fail:

- The receiver starts from the shared value. That value holds completed sends
  only, and a send lands after the producer releases its lock. The read of the
  receiver can therefore be newer than the shared value. The receiver starts
  behind, and it stays behind.
- The receiver supplies its own value. The seed is then per receiver again, and
  the shared watch adds nothing.

**A read position is per receiver.** A slow receiver and a fast receiver hold
different read positions for one key. A shared watch cannot hold one position
for both.

**A shared watch needs a reference count.** With one, the package owns a shared
per-key object with a lifecycle, and a teardown races a publish. The design in
this specification has no shared per-key object. There is nothing to count, and
nothing to race.

R10 is the same argument in the `Accept` case: a shared evaluation cannot be
right for two receivers that hold different values.

`Hub.Watch` is hub-level creation without hub-level sharing. R3 gives the
fan-out that A1 wanted: one `Send` reaches every receiver of the key.

### A2. `Watch` and `Unwatch` on the receiver

This is the shape in the original request. A receiver holds a key set that the
caller changes at any time.

It is rejected because it raises four questions that a single-key receiver does
not: whether a repeat `Watch` re-seeds or fails, whether `Unwatch` discards an
unread value, how the receiver orders two ready keys, and how a consumer
subscribes to many keys without a cost that grows with the square of the key
count.

The cost of the rejection is R4. A wide consumer holds many receivers and pays
one goroutine for each. The original request states that its consumers watch
one key each, so the cost does not fall on the requester. Confirm this in D5.

### A3. A caller-supplied order on `Send`

The original request asked for a `uint64` order on `Send` and on `Watch`. A
value at or below the receiver's order was discarded.

`Accept` replaces it and is better in four ways:

- `Send(k, v)` keeps the signature that `gobus.Sender[K, V]` declares, so the
  package needs no separate send-side interface and can take a row in the
  conformance suite.
- The rule is not limited to a `uint64`. It admits a vector clock, a priority,
  a source rank, or a rule such as "prefer a value that is not empty".
- The comparison runs against the receiver's own slot, which R10 requires. An
  order argument would need the same per-receiver evaluation to be correct, so
  it gains nothing here.
- `initial` becomes the first `prev`, which gives it a defined role.

The cost is that the order must ride inside `V`. A caller keeps a wrapper
struct with a sequence field. See D1.

### A4. `gochan/watch` under the hood

A `map[K]*watch.Hub[V]` is rejected. With one key for each receiver, fan-in is
no longer a reason: our receiver would wrap one `gochan` receiver. Three
reasons remain, and the first is decisive.

**It cannot meet R5.** A `gochan` hub holds its slot for as long as the hub
exists. To release the state, the package must destroy the hub when the last
receiver closes. That needs a reference count, and it puts a teardown in a race
with a publish. A1 rejects this. To keep the hub instead is to grow memory with
every key ever watched.

**It cannot meet R8.** `gochan/watch` has no accept rule, and its slot is
hub-wide. A per-receiver rule cannot be added from outside.

**It cannot meet R52.** `gochan/watch` documents that registration does not
snapshot the slot. Our rule is the opposite.

The reference implementation is a template, not a dependency. See section 10.

## 9. Closed decisions

The requester answered all six in `docs/specs/gobus-keyed-watch-reply.md`. Each
entry below records the answer and the requirement that carries it.

**D1. `Accept` serves the requester. Closed: confirmed.**

The rule is `next.Seq > prev.Seq`. It fits `func(prev, next V) bool`, because
the consumer selects one value and never combines two.

The order rides inside `V`. The requester keeps its `stamped` wrapper, which
the original request expected to delete, and prefers the predicate with the
wrapper to an order argument without it.

No consumer needs to know that a value was rejected. R9 stays: the bus is
silent, with no callback and no counter.

**D2. `Watch` marks `initial` as read. Closed: R19 rewritten.**

The requester prefers this reading. The caller supplies `initial`, so to
deliver it is to return the caller's own argument. R19 now states it, R27 records
what it gives `gobus.ErrEmpty`, and R58 needs no extra read in the conformance
row.

R18 holds under both readings, so the reversal costs nothing: `initial` is
still the `prev` of the first `Accept` call.

**D3. `Accept` does not receive the key. Closed: R15 stands.**

The rule compares two values and nothing else, and it is the same rule for
every key. A second consumer inside the requester's system would build its own
hub with its own rule, rather than one hub whose rule varies by key.

**D4. The per-watch `Accept` is deferred. Closed: R48 stands.**

Every watch on the requester's hub uses one rule. No consumer disagrees with a
sibling consumer. A per-watch override stays additive, and it is a method on
the hub when a caller asks for it.

**D5. No consumer watches many keys. Closed: confirmed, and R4a added.**

Each subscriber watches one object id, so it holds one receiver and one
goroutine.

The requester asked that one point be pinned rather than merely observed: its
wide stream is an object list watch, that stream needs annihilation, and it
belongs to `conflate`. R4a now states this, so R4 cannot later be read as a
reason to widen this package.

**D6. `Option[V]` is correct. Closed: confirmed.**

The requester's call site spells `K` only:

```go
watch.New[ObjectID](watch.WithAccept(func(prev, next stamped) bool {
	return next.Seq > prev.Seq
}))
```

The constraint stands: do not add a `K`-dependent hub option without changing
this decision on purpose. Section 10 removes the need for the one such option
that this design considered.

### A measurement the requester will report

This is not a change request, and it changes no requirement. Record it so that
the first performance report has context.

The requester's producer publishes from the work queue that every unit of work
in its control plane passes through. Its common case is zero receivers, which
R30 covers. With one subscriber on one object, every publish for every other
object reaches the locked path, because R31 is an optimization and not a
guarantee.

The requester accepts this and will measure it first. A measurement that lands
badly is the evidence for building R31 under the build tag in section 10.

## 10. Implementation notes

These notes are not requirements. They record the analysis.

A single-key receiver is much smaller than a `conflate` receiver. It needs no
`list.List`, no element map, and no pending map. The receiver holds its key,
its slot, a version and a read position. This is the `gochan/watch` receiver,
one for each key.

The hub holds an index from key to the set of receivers for that key, under one
mutex. `Send` takes that mutex, finds the set, and for each receiver calls
`Accept` and writes the value on a `true`. One mutex for the whole hub follows
`conflate`.

`gochan` is not a dependency (R51), and it is not vendored in this repository.
R51 forbids the import, not the reading. Clone the sister module to read the
template.

Take the following from `gochan/watch` as a template:

- the version mechanic: a version beside the value, and a read position;
- the feeder that re-reads the slot when a newer value arrives during delivery.
  This serves R45. The feeder in `conflate` does not do this.

Do not copy three details from `gochan/watch`:

- its `SendContext` reads the context at entry. R35 forbids this.
- its `Send` always takes the lock. R30 forbids this.
- its `lastSeen` is owned by the consumer goroutine, and its lock-free
  `TryRecv` compares the version against it. Neither transfers. See below.

### The read position and the `TryRecv` fast path

Hold the read position in the receiver, under `s.mu`, as `conflate` holds every
equivalent field. Do not treat it as owned by one goroutine.

`gochan/watch` can own it in the consumer goroutine because its receiver is
one handle for one reader. A `watch` receiver is read by two goroutines
whenever a caller uses `Chan`: the feeder and any direct `TryRecv` on the same
handle. The single-reader rule is an intent, not an invariant, and a field with
two access disciplines is what R51's sister-module note warns against.

Do not copy the lock-free `TryRecv` fast path. It cannot give a correct answer
here:

- It can never return a value, because reading `V` without the lock is a data
  race. The only answer it can give is `gobus.ErrEmpty`.
- `gobus.ErrEmpty` is the answer it gets wrong. When the sender is closed with
  nothing unread, R38 and R43 require a terminal `gobus.ErrClosed`, the
  receiver must close itself, and R41 must drop the key from the index. None of
  that is reachable without the lock: the tear-down has to happen under the lock
  that found the state terminal.
- `gochan/watch` gets away with it because its `txClosed` is an
  `atomic.Bool`. `conflate` keeps that field a plain `bool` under `s.mu` on
  purpose, and its CLAUDE.md gives the reason. Copying the fast path would give
  one field two access disciplines to buy an answer the lock already gives.

Keep `conflate`'s two lock-free checks and no more: `rx.done` on every receive
path, and `liveReceivers` on the send path. Put the version comparison under
`s.mu`, where the value read needs the lock anyway.

### The locked region

`Accept` is caller code, and it runs under the hub lock. Copy `conflate`'s
`sendLocked` shape exactly: the locked region sits in a closure with a deferred
unlock, so a panic out of `Accept` releases the mutex. R14 depends on this.

Do not call into the caller's `context.Context` under the lock. Take an
already-obtained `Done` channel into the locked region, and resolve `ctx.Err()`
after the release, as `conflate` does.

### The send fast path

For R30, copy `conflate.liveReceivers`: a lock-free count of live receivers,
derived under the lock and never incremented, carrying the closed state as a
poison value. R32 is what the poison holds.

R31 wants more: skip the lock for a key that no receiver watches. A fixed array
of atomic counters, indexed by a hash of the key, would give it in O(1) for
each `Watch` and each `Close`. A collision costs one uncontended lock, which is
the safe direction of error. Size the array at a fixed 256 entries, per hub,
allocated in `New`. It is not per receiver.

**The bucket check is subordinate to the poison.** Read `liveReceivers` first.
Consult the bucket array only when that load is above zero. An independent
bucket check reintroduces the bug the poison exists to prevent: a closed hub
whose receivers were torn down has zero in every bucket, and the fast path
would answer `nil` where `gobus.ErrClosed` is the durable answer. R32 covers
both checks, and only this order holds it.

**The buckets are incremented and decremented, not derived.** This is the
opposite of `conflate.liveReceivers`, which is derived under the lock on
purpose. It is sound here only because both mutations happen under `s.mu`, at
the same site that mutates the key index, so a bucket can never disagree with
the index. Do not "fix" this into a derived count: deriving a per-key count
means walking the index, which is the O(1) property the array exists to buy.

That array needs a hash of an arbitrary `comparable` key.
`hash/maphash.Comparable` gives one, but it arrived in Go 1.24. The module
floor is Go 1.21, and CI runs 1.21 to 1.26. Three options exist:

1. Put the array behind a `//go:build go1.24` file, and fall back to R30 alone
   on older toolchains. The check is an optimization, never a verdict, so a
   conservative fallback is correct on every version. This is why R31 is a
   "should" and not a guarantee.
2. Take a hasher from the caller. This adds a `K`-dependent hub option, which
   D6 forbids without a deliberate change.
3. Do not implement R31.

Prefer option 1. Do not use a copy-on-write set of the watched keys: a rebuild
under the lock at each `Watch` is O(keys), so a program that opens N receivers
pays O(N²).

Option 1 is a build tag, not a runtime check. R31 is therefore absent from a
build whose toolchain is below Go 1.24, and the toolchain that decides it is
the caller's, not this module's. Tell the requester: section 9 records that
they will measure the "one subscriber, every other publish takes the lock"
case, and R31 is the fix for exactly that case, but only if their control plane
builds on Go 1.24 or later.

### Cost

Two costs belong in the package documentation, not only here.

**The index is a win over `conflate`.** `conflate.Send` iterates every receiver
on the hub and applies each one's key filter. `watch` indexes by key and
touches only that key's receivers. For one receiver per object over many
objects, that is O(receivers) down to O(1) once the lock is taken. For the
requester's shape this is a larger gain than R31.

**The hub-wide mutex is the real cost.** N receivers on one hot key run the
caller's `Accept` N times inside the single hub lock, and every other key's
publish waits behind it. R10 makes this unavoidable, and it is the right trade,
but it bounds what R31 can do: R31 removes lock acquisitions for cold keys, and
it does not shorten the critical section for a hot one.

## 11. Test requirements

**R58.** Add a row to `architectures` in `conformance_test.go`. The row calls
`Watch`. Under R19 the new receiver holds no unread value, so the row needs no
extra read before the suite's first `TryRecv`.

**R59.** Reach 100% coverage of the package. CI enforces this.

**R60.** Do not use `time.Sleep`. Do not use a timeout whose duration encodes
an assumption about the scheduler. Use channels or observable state.

**R61.** Give each lock-free check a `forTesting` hook. A test must exercise
each race without timing.

**R62.** Test both results of `Accept`. Test R10 with two receivers of one key
that hold different values, where one accepts the value and the other rejects
it. Test R12 with a nil `Accept`, R50 with a nil `Option` passed to `New`, and
R14 with a panic. R14a needs a second receiver, to assert that the fan-out
before the panic kept its value and the one after it did not.

**R63.** Test R16 under the settled-slot reading. Run both lock orders with the
lock hook, let both `Send` calls return, then take one `TryRecv`. Do not assert
the sequence a reader observes between the two calls: R16 does not constrain
it, and R49 removes `Peek`, so the settled value is the only observable.

**R64.** Test R21 and R22 directly. Each case needs a deterministic interleave.
R22 is a permitted duplicate when no `Accept` is set, so that test must assert
that the duplicate occurs. It must not assert that it does not.

**R64a.** Test R45a and R45b. Do not test R45 as "the consumer never reads a
superseded value": R45c permits it, and such a test fails at random. Use the
feeder's exit hook to sequence the delivery, as `conflate`'s
`TestChanFeederCloseWhileDelivering` does.

**R65.** Test R41 on both paths that leave the hub: `Receiver.Close`, and a
receiver that reaches terminal `gobus.ErrClosed` under R43. After the last
receiver for a key leaves by either path, the hub holds nothing for that key.
This is the R5 guarantee, and it needs a test-only helper like `conflate`'s
`forTestingReceiverCount`.

**R65a.** Test the states in section 6.8a. Each is a coverage obligation under
R59, and R43a and R43d are the two that an implementation is most likely to
leave to a nil dereference.

**R66.** If R31 is built behind a build tag, both paths need tests. CI must run
at least one job on a toolchain below Go 1.24, which the 1.21 row already gives.

**R67.** Assert whole `gobus.Event` values. Do not assert the key and the value
apart.

## 12. To tell the requester

Two items in the reply rest on readings that this specification no longer
supports. Neither blocks the requester's design, and neither needs an answer
before implementation starts.

**R45 is weaker than the reply states.** Section 1 of the reply calls R45 "the
last defect closed", and reads it as "a value that `Accept` rejects can
displace nothing". The first half holds: a rejected value never enters a slot,
so it can displace nothing. The second half does not: R45c now records that a
value already committed to the delivery select can still reach the consumer
after a newer value lands, because both arms of that select are ready and Go
chooses at random.

The consumer converges either way, under R45b. The requester's read loop
already drops a repeated value, for the reason its reply section 4 gives, so
its code is correct as written. Only the claim is too strong.

**R31 depends on the requester's own toolchain.** Section 9 records the
measurement they plan. R31 is the fix for that measurement, and section 10
builds it behind a `//go:build go1.24` tag. The toolchain that decides whether
they get it is the one their control plane builds with, not this module's
floor. If that floor is below Go 1.24, the measurement will not improve when
R31 lands.
