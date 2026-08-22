# Make `watch`'s baseline an option, and take the first value unjudged without one

- **Status:** accepted, implemented
- **Date:** 2026-08-22
- **Scope:** `github.com/amorey/gobus`, package `watch`
- **Breaking:** yes. `Hub.Watch` and `Hub.WatchAcross` change shape; every call
  site edits.

## Context

`Hub.Watch(k, initial)` required the caller's current value, and `initial` was
observable in exactly one place: as the `prev` of the first `Accept` call
(`offerLocked`). It is never delivered — registration seeds `version` and
`lastSeen` equal, so every read path reports `ErrEmpty` until a `Send` lands.

On a hub built without `WithAccept` — the default, last-writer-wins — `s.accept`
is nil, `offerLocked` short-circuits, and the value is never read by anything at
all. Those callers were required to produce a value the bus provably ignores.

Callers who have no current value to give had no way to say so. The workaround
is to pass the zero `V`, which is not "nothing": it is a real value that a
caller's `Accept` compares against, so `bySeq` against a zero-`Seq` baseline
accepts anything while against a true baseline it may reject. Silent, and wrong
in a way tests do not catch.

## Decision

```go
func (h *Hub[K, V]) Watch(k K, opts ...WatchOption[K, V]) *Receiver[K, V]
func (h *Hub[K, V]) WatchAcross(opts ...WatchOption[K, V]) *Receiver[K, V]
func (h *Hub[K, V]) WithBaseline(cur V) WatchOption[K, V]
```

A receiver registered without `WithBaseline` holds nothing, and **takes its
first value without consulting `Accept`**. There is no `prev` to pass, and the
receiver has read nothing that the value could fail to improve on. `Accept`
governs every value after the first.

`hasValue` on the receiver carries that state: false until a value lands, true
from the first one on, whether it came from `WithBaseline` or from a `Send`. It
is a separate bit rather than a comparison against the zero `V`, because the
zero `V` is a usable baseline.

`WithBaseline` is a method on the hub, like `conflate`'s receiver options, so
`K` and `V` are fixed and call sites need no type arguments. It is per receiver
because each consumer's baseline is the value it read at its own instant.
`WithAccept`, the rule those values are judged by, stays hub-wide and
package-level for the opposite reason: it is built before the hub exists.

### Rejected

**Deliver the baseline to every receiver.** This answers a different question —
`cur` stays required, so nothing gets more ergonomic — and it costs three
documented behaviors: `Peek` and `TryRecv` no longer report `ErrEmpty` for a
fresh receiver, and `Sender.Close`'s drain table changes because a receiver that
never saw a send now has a value to hand back. It breaks `WatchAcross` outright:
a wildcard receiver has no key until a value lands, so delivering the seed means
handing back an `Event` whose `Key` is the zero `K` paired with a real value.
There is no key to put there, because the caller never named one.

**Fold `cur` into `WithAccept`.** `WithAccept` configures the hub; the baseline
is per receiver. A hub-wide seed is precisely `gochan/watch`'s design, which
this package's doc calls out as the thing it is the opposite of, and it leaves
two consumers registering at different instants unable to express different
baselines.

**A second constructor, `WatchFrom(k, cur)`.** It does remove the argument, but
`WatchAcross` needs the same split, so it lands on four constructors — the
"constructor per combination" growth the conventions in `CLAUDE.md` exist to
prevent.

**Seed the zero `V` when the option is absent.** Rejected for the reason in
*Context*: the zero `V` is a value, and using it as a baseline silently changes
which first value wins.

## Consequences

The common registration becomes `hub.Watch(id)`. A consumer that genuinely has
a current state still says so, and now says it in a way that reads as a
decision: `hub.Watch(id, hub.WithBaseline(cur))`.

"Registration is the snapshot" survives intact — `Watch` still calls no caller
code and is still safe to call under the producer's own lock. What changes is
that a caller is no longer forced to produce a value when it has none.

The new rule has an edge worth stating plainly: with no baseline, the first
value wins even if a later `Accept` would have rejected it. For a monotonic rule
like `next.Seq > prev.Seq` that is correct — nothing has been read, so any value
is news — but a caller whose `Accept` encodes a *floor* ("never accept anything
below Seq 100") does not get that floor applied to the first value. Such a
caller wants `WithBaseline`.

Every `Watch` and `WatchAcross` call site edits. Pre-1.0, and the old form does
not compile against the new signature, so the break is loud rather than silent.
