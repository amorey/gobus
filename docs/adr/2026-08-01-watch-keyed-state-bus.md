# A keyed latest-value state bus, beside `conflate`

- **Status:** accepted, implemented
- **Date:** 2026-08-01
- **Scope:** `github.com/amorey/gobus`, new package `watch`; `internal/buscore`;
  the root conformance suite
- **Supersedes:** the withdrawn caller-supplied ordering request for `conflate`.
  It stays withdrawn — see *Alternatives*.
- **Relation to prior ADRs:** the 2026-08-01 send-fast-path decision left open
  "does a second bus type want this, and is `internal/buscore` the right home
  when it does?" This answers yes to both: `watch` gates `Send` on the same
  count, and it now lives in `buscore.LiveCount` with `conflate` migrated onto
  it. The 2026-07-28 `Peek` decision is unaffected; `watch` ships no `Peek`.
- **Record:** the downstream consumer's original proposal, the specification
  drafted from it and their reply to that specification are not retained in the
  repository. This ADR is the record; where it says "the requester", the
  position quoted is theirs.

## Context

`conflate` is a keyed **event** bus. Values queue per key, coalesce through a
caller `Merge`, and leave the receiver when delivered. That is the right shape
for a change stream — "added / modified / deleted", where a create-then-delete
pair should annihilate.

It is the wrong shape for a gauge. For a gauge there is no "what happened",
only "what it is now", and every consumer of one rebuilds the same two things
by hand: a snapshot taken at subscribe time, and a filter that drops values its
own read already reflected. The requesting consumer had both, plus a
register-before-read ordering rule subtle enough that an earlier revision of
their own design document had it backwards — silently, and in the one stream
they have with no backstop.

They had also asked for caller-supplied ordering *in* `conflate`, then withdrew
it, and the reason is the load-bearing part of this decision. Rejecting a stale
value for a key means remembering the order last delivered **for that key**;
that memory has to outlive delivery; and `conflate` cannot know when a key is
finished, because its key set comes from traffic. The watermark map therefore
grows without bound over an unbounded key space. `conflate.popLocked` deleting
the key on delivery is exactly what lets that package promise memory bounded by
the live key set, and a watermark is the one thing that cannot be deleted with
it.

A keyed watch inverts that by construction. A key's state exists *because a
consumer asked for it* and ends when that consumer goes away, so the key set is
declared rather than discovered, and per-key state rides a lifetime the
consumer has already stated. The feature that is unsound in `conflate` is free
here — which is the whole argument for a separate package rather than an option
on the existing one.

## Decision

Ship `gobus/watch`. Five choices shape it; the rest follows from them.

**1. One key per receiver.** `hub.Watch(k, initial)` mints a receiver bound to
`k` for its whole life, and `Receiver.Close` is the matching unwatch. There is
no `Unwatch` and no mutable key set.

The constraint is structural rather than documented, which is the point: a rule
saying "do not watch twice" is one an implementation can break, while a
signature that has nowhere to put a second key cannot be. It also disposes of
four questions a mutable key set raises — what a repeat `Watch` does, what
`Unwatch` does to an unread value, how a receiver orders two ready keys, and
how a bulk subscribe stays cheap.

**2. Registration is the snapshot.** The caller passes the value it has just
read, and the bus never hands it back. That value is the baseline the first
`Accept` call measures against, not a delivery.

This is the **opposite** of `gochan/watch`, whose hub holds one seed and whose
registration deliberately does not snapshot. Two sister modules, the same
package name, inverted rules — so it is stated in the first paragraph of the
package doc rather than left to be discovered. A reader carrying the sister
package's rule across gets a subscriber that starts behind and stays there,
which is precisely the bug the requester hit when they prototyped this over
`gochan/watch`.

**3. A caller `Accept`, not a caller order.** `Accept(prev, next V) bool`
decides whether a value replaces the one in a slot; omitting it accepts
everything, which is last-writer-wins.

A producer computes a change under its own lock and publishes after releasing
it — nesting the bus lock inside the producer's would put a second mutex on the
hot path of every state change. Two changes to one key can therefore reach
`Send` in the reverse of the order in which they became true. `Accept` is what
makes the settled value independent of that race, provided the caller's rule is
a strict order over `V`. The bus supplies the evaluation; it never defines what
"older" means.

**4. `Accept` is evaluated per receiver, against that receiver's own slot.**
Two receivers of one key seeded at different moments hold different values, so
one publish can be news for the early subscriber and stale for the late one.
Only a per-receiver call answers both correctly. This is also why no design
that shares one slot per key across receivers can work — see *Alternatives*.

**5. No annihilation.** `keep == false` is a change-stream concept. A producer
that must say "this key is gone" encodes it in the value, which for a gauge is
a pointer or an `exists` field. A consumer that genuinely wants annihilation
wants `conflate`, and that boundary is recorded in both packages' docs so a
later request to widen `watch` is recognised as belonging to the other bus.

## Consumer-side obligations

Three, and the first two are the price of the design rather than incidental.

**Call `Watch` under the producer's lock.** `Watch` calls no caller code, which
is what makes this legal, and it is what closes the window between reading the
state and registering for changes. A consumer that reads first and registers
after loses anything published in the gap; `watch` has no replay, so the loss
is permanent. Registering first and reading after is not an alternative here —
the value read *is* the argument to `Watch`.

**`Accept` must take no lock a caller may hold when entering the bus.** It runs
under the bus lock, so an `Accept` that takes the producer's lock inverts the
order against the `Watch`-under-that-lock pattern above, and the two deadlock.
"Reads its two arguments and nothing else" is always safe; anything more should
be copied into `V` at `Send`. This is stricter than "must not call back into
the hub", and it has to be, because the hazard needs no re-entry.

**Close the receiver.** An abandoned handle holds its key against the hub for
the hub's lifetime, and an abandoned `Chan` also pins its feeder goroutine.
`defer rx.Close()` covers both.

## Consequences

A consumer watching N keys holds N receivers and, using `Chan`, N goroutines.
`watch` is therefore deliberately unsuited to wide subscriptions, and the
package doc says so. The requester's consumers watch one object each, so the
cost does not fall on them; if a wide consumer ever appears, the answer is a
merge helper above the bus, not a mutable key set inside it.

Indexing by key is a real win over `conflate` on the send path.
`conflate.Send` iterates every receiver on the hub and applies each one's key
filter; `watch.Send` touches only the receivers watching that key. For one
receiver per object over many objects that is O(receivers) down to O(1) once
the lock is taken — a larger gain for the requester's shape than any fast path.

The hub-wide mutex remains the real cost. N receivers on one hot key run the
caller's `Accept` N times inside it, and every other key's publish waits behind
that. Per-receiver evaluation makes this unavoidable and it is the right trade,
but it bounds what a per-key fast path could ever buy: such a path removes lock
*acquisitions* for cold keys, and does not shorten the critical section for a
hot one.

A `Chan` consumer can receive one value after `Receiver.Close` or `Hub.Close`
returns. Once the feeder has committed to a delivery, anything making its
select's other arms ready races that delivery and Go picks uniformly. This is
`conflate`'s behaviour too; it is now written down in both.

## Alternatives considered

**A caller-supplied `order uint64` on `Send` and `Watch`** — the original
request. Rejected in favour of `Accept`, which is better on four counts. It
keeps `Send(k, v)`, so the handles satisfy the module-wide `gobus.Sender` and
take a row in the conformance suite instead of forking the contract. It is not
limited to a counter, so it admits vector clocks, priorities and
"prefer a non-empty value". It needs the same per-receiver evaluation to be
correct, so the order argument buys nothing the predicate does not. And it
gives `initial` a defined role as the first `prev`. The cost is that the order
rides inside `V`, so the requester keeps the wrapper struct they had hoped to
delete; they confirmed they prefer the predicate with the wrapper.

**`Watch`/`Unwatch` on the receiver**, the shape the request sketched.
Rejected for the four ambiguities in *Decision 1*, and because the prohibition
would have been a rule rather than a structure. The price is the width limit
above.

**A shared per-key watch object**, so several receivers fan out from one slot.
Rejected three times over: the seed is per receiver, so a later subscriber
either bootstraps from a slot holding only completed sends — starting behind —
or supplies its own, at which point the sharing adds nothing; the read position
is per receiver, so one shared position cannot serve a fast and a slow reader;
and a shared object needs a reference count, which puts a teardown in a race
with a publish. `Hub.Watch` is hub-level *creation* without hub-level
*sharing*, and one `Send` still reaches every receiver of the key.

**`map[K]*gochan.watch.Hub[V]` under the hood.** With one key per receiver the
fan-in objection disappears, so this was reconsidered seriously. It fails on
memory: a `gochan` hub holds its slot for as long as it exists, so releasing
per-key state means destroying hubs on a refcount — the teardown race above —
and keeping them means growing with every key ever watched. It also cannot
express a per-receiver `Accept` from outside, and its registration contract is
the inverted one. `gochan/watch` is a template, not a dependency; `gobus`
imports nothing from `gochan`, consistent with `buscore` re-implementing
`chancore.CloseOnce` rather than importing it.

**Delivering the seed**, as `gochan/watch` does. Rejected on the requester's
preference: the caller supplied the value, so returning it hands back their own
argument. It also gives `ErrEmpty` the more useful meaning — "nothing has
changed since you subscribed" rather than "you have not read your own seed" —
and it keeps the conformance row from needing an extra read before the suite's
first `TryRecv`.

## Implementation

`watch/` is three files, mirroring `conflate/`: `watch.go`, `watch_test.go`,
`helpers_test.go`.

`internal/buscore` gained `LiveCount`, the poisoned lock-free receiver count
that gates both buses' send fast path, and `conflate` was migrated onto it. The
duplication was the trigger — the count, the poison, and the "derived, never
incremented" rationale had been copied verbatim into the second package, which
is what CLAUDE.md's "prefer extending `buscore`" rule exists to prevent.

`LiveCount.Sync` compare-and-swaps rather than load-then-store. A plain store
leaves a window for a `Poison` to land between the two halves and be
overwritten, and that is the one error the type cannot tolerate: an idle count
on a closed bus makes every later send return `nil` instead of `ErrClosed`,
dropping values on a bus with no replay. Both buses call `Sync` under their own
lock, so the window was latent — but the invariant should not depend on a
caller of a shared building block getting that right.

The conformance suite gained `architecture.key`, threaded into `newPair`. A
`watch` receiver binds to one key at registration, and a suite publishing
elsewhere would make its *negative* assertions pass vacuously: a bus that
delivers nothing satisfies "a cancelled send published anyway". Declaring the
key in one place and watching it in another would have reopened that hole, so
both sides read the same field.

Gates: `gofmt`, `go vet`, `staticcheck -checks=all`, `go test -race ./...`, and
100% coverage on all three library packages. Two coverage notes worth keeping.
The feeder's close-during-delivery arm is only reachable when nothing is
reading the channel, so its test waits on the feeder's exit hook rather than
ranging — a reader would make both select arms ready and the arm would be
covered only sometimes. And `LiveCount`'s poison race is exercised through a
hook that lands the `Poison` inside `Sync`'s window, because hammering it
concurrently does not hit a window that narrow.

## Still open

- The per-key send fast path. `Send` currently skips the lock only when the hub
  has no receiver at all, so with one subscriber on one object every publish
  for every other object reaches the lock. The fix is a fixed array of atomic
  counters indexed by a hash of the key, subordinate to the poison — but
  hashing an arbitrary `comparable` needs `maphash.Comparable`, which is Go
  1.24 against a 1.21 floor, so it belongs behind a build tag with the current
  behaviour as the fallback. The requester will measure this first, and the
  measurement is the trigger. Note the toolchain that decides whether they get
  it is theirs, not this module's.
- A per-watch `Accept` override, as `conflate` has `WithMerge` beside its hub
  `Merge`. Additive, and deferred until a consumer of one producer disagrees
  with a sibling consumer.
- Whether `Accept` should receive the key. It does not today: a receiver
  watches one key, so the key is constant for one slot, and `conflate.Merge`
  takes none either. Adding it later changes every `Accept` signature.
- Whether `Option[V]` must ever become `Option[K, V]`. It carries `V` alone,
  which is what lets a call site spell only `K`. A future hub option taking a
  `K` breaks that for every existing call site.
