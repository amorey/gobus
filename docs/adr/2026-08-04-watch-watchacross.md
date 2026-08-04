# Give `watch` a `WatchAcross`, a receiver bound to every key

- **Status:** accepted, implemented
- **Date:** 2026-08-04
- **Scope:** `github.com/amorey/gobus`, package `watch`
- **Relation to prior ADRs:** amends the 2026-08-01 keyed-state-bus ADR, which
  recorded "one key per receiver" as a structural constraint and routed wide
  subscriptions to `conflate`. That routing was right for a change stream and
  wrong for a signal, which is the distinction this ADR adds.

## Context

The requesting consumer is a dependency waker in an embedded control plane. It
subscribes to a hub keyed by `GroupKind` and its entire reaction to any change,
under any key, is to re-read its store. It cannot enumerate the keys it cares
about: a dependency edge may point at a kind no controller watches, so no
per-kind subscription names it.

Before this change `watch` had nothing for it and the README sent it to
`conflate`. Both of `conflate`'s properties are wrong for a signal:

- **Send routing.** `conflate.sendLocked` iterates every receiver and applies
  each one's `keep` inside `enqueueLocked`, so one wide subscriber turns every
  publish into O(receivers) plus a closure call each, under the hub lock.
  `watch.sendLocked` routes through `index[k]`. This hub sits on a commit path,
  so exact routing is the property being protected.
- **Slot cardinality.** `conflate` holds a slot per key, so a burst across N
  keys is N deliveries to a consumer whose reaction to all of them is one
  re-read. `watch`'s one-slot-per-receiver invariant is exactly the collapse
  wanted — the consumer just could not have it without naming a key.

## Decision

**`Hub.WatchAcross(initial) *Receiver[K, V]` mints a receiver bound to every key,
holding one slot.** A burst across many keys leaves exactly one pending value.
That is the contract, stated as such in the doc comment and pinned by a test
that reads twice — a per-key structure would satisfy a single-read assertion by
handing back the newest first.

**The key travels with the value.** `Event.Key` names the key the slot's value
was published under, written in `offerLocked` next to the value and only when
`Accept` takes it. A rejected value leaves the slot entirely alone, key
included, so the pair a read hands back is always the one that landed together.
A receiver still on its baseline has no key, which is unobservable: every read
reports `ErrEmpty` until a value lands.

**Everything else is `Watch`'s.** Registration is the snapshot, no caller code
runs during registration, `Accept` is evaluated against this receiver's own
slot, and all three `Close` methods behave identically — including the
`Sender.Close`-versus-`Send` promise, which the requester cites by name and
which now has its own test against a wildcard receiver.

**Wildcards live in a second map, not in `index`.** `shared.wildcard` sits
beside `shared.index`, and a receiver is in exactly one of the two.
`sendLocked` fans out to `index[k]` and then to all of `wildcard`.

The alternative — reserving an entry in `index`, keyed by the zero `K` — was
rejected. `index`'s key set *is* the hub's live key set: it is what
`deregisterLocked` bounds and what "a key costs nothing once its last watcher
has gone" is a statement about. A wildcard receiver watches no particular key,
so it must not be able to appear there. Under the zero-key scheme it would pin
that key for its whole life, and every send to a caller's legitimate zero key
would fan out to the wildcards twice.

The other alternative — a mutable key set, or an `Unwatch` — stays rejected for
the reasons the 2026-08-01 ADR gives. `WatchAcross` adds a second *fixed* binding,
not a mutable one: a receiver is one-key or all-keys from construction, and the
field recording which is never written again.

**`conformance_test.go` gains a row**, unlike the `Peek` decision. This is not a
new bus type, so the rule "a bus package means a row" does not by itself call
for one. But `WatchAcross` hands callers the same `gobus.Receiver` through its own
registration and routing paths, so the precedence it owes is the interface's.
A row is the cheapest way to keep the two receiver kinds from drifting.

## Naming

The method shipped as `WatchAll` in review and was renamed before release. "All"
sits next to an unstated noun, and the two candidate nouns point opposite ways:
all *keys*, which is true, and all *values*, which is exactly what a one-slot
receiver does not give you. Worse, it reads complete, so nothing makes a skimmer
stop — they fill in the wrong noun and fill it in confidently.

`WatchAcross` was chosen over `WatchWildcard`, `WatchAny`, `WatchLatest`,
`WatchAllKeys` and `WatchAcrossKeys`. Since the alternatives keep suggesting
themselves:

- `WatchAny` reads as *any one key, unspecified* — a choice among keys rather
  than a span over them.
- `WatchLatest` names what every receiver on this bus does. `Watch` is
  latest-value too, so it labels the shared property rather than the
  differentiating one, and implies plain `Watch` is something else.
- `WatchAllKeys` and `WatchAcrossKeys` bind the noun and are unambiguous, at the
  cost of re-encoding what `Hub[K, V]` and the dropped key parameter already
  say. A hub has exactly one dimension to range over, so the preposition has
  exactly one available object.
- `WatchWildcard` was the runner-up and was held briefly. It is a term of art
  rather than a coinage, so callers arriving from NATS, MQTT or informer-style
  APIs recognize it untaught, and it would have made the codebase speak one word
  end to end. It lost on the prior it imports with the recognition: a wildcard
  subscription *there* delivers every matching message. That is the same
  misreading `WatchAll` invited, arriving with more authority because it is true
  in the systems it is borrowed from, and — unlike the pattern-language prior,
  which the signature refutes by having nowhere to put a pattern — nothing at
  the call site contradicts it.
- The bare `WatchAcross` is grammatically incomplete, which was the objection to
  it and turned out to be the argument for it. `errors.Is` and `errors.As` are
  incomplete in the same way and read fine, because a method name is a label
  rather than a sentence. An incomplete name makes a skimmer stop and read the
  doc line, where `WatchAll` let them skim past believing something false.
  Failing puzzling beats failing plausible, and `WatchAcross` is the only
  candidate with no confident misreading available at all.

Its costs, recorded rather than argued away. "Across" is a coinage for this API
that every caller has to learn once, and it is a subtler word than "all" for a
reader whose first language is not English. It also leaves the codebase
bilingual: the mechanism is a wildcard set internally (`shared.wildcard`,
`Receiver.wildcard`, `forTestingWildcardCount`), which is the right word for a
routing structure with no caller to mislead, while the API and its docs say
"across". That split is deliberate — mechanism inside, result outside — and not
an inconsistency to clean up in either direction. Prose written about the
feature drifts toward "wildcard" without meaning to, which happened once in a
doc comment during review; the fix is to catch it, not to rename the method.

**The MQTT/NATS non-guarantees stay documented even though the name no longer
invites them.** A broad-subscription API attracts both assumptions regardless of
what it is called, and the delivery one is the property this whole design turns
on. The method doc and the README state both explicitly.

**`WatchAll` stays unclaimed, and should stay that way.** The feature it would
name — a receiver over every key holding a slot *per* key, delivering pairs in
first-touch order — is `conflate`, and is expressible there today, since an
`Accept` is a `Merge` that never annihilates:

```go
merge := func(prev, next V) (V, bool) {
    if accept(prev, next) { return next, true }
    return prev, true
}
```

Building it inside `watch` would mean giving a receiver a key queue and a slot
map, which is `conflate` reimplemented behind a second constructor and the end
of the single-slot `version`/`lastSeen` invariant the package is built on. If it
ever does land here anyway, it must not be called `WatchAll`: beside
`WatchAcross` that would be two adjacent names with opposite cardinality, a
worse trap than the one this rename fixed. Name it for its cardinality —
`WatchEachKey`.

## Consequences

- `watch` is now a reasonable choice for a "something, anywhere, changed"
  signal, and the READMEs say so. The routing note that sent all wide
  subscriptions to `conflate` is narrowed: `conflate` is for consumers that need
  each key's *own* latest value, or the annihilation a create-then-delete pair
  needs.
- `offerLocked` takes the key as a parameter. For a single-key receiver the
  write is a no-op, since `sendLocked` reached it through `index[k]`.
- A hub with a wildcard receiver has no unwatched key, so the "a `Send` nobody
  watches is dropped" rule can no longer be reached on it.
- `forTestingWildcardCount` exists because a leak in the wildcard set is
  invisible from the handle: a receiver dropped from `receivers` but left in
  `wildcard` is still offered every value, while every read on it reports
  `ErrClosed`.
- Not added: any way to observe which keys a wildcard receiver has missed. The
  collapse is the feature; a consumer that needs the misses is not a signal
  consumer and wants `conflate`.
