# Skip the bus lock in `conflate.Send` when no receiver is registered

- **Status:** accepted, implemented
- **Date:** 2026-08-01
- **Scope:** `github.com/amorey/gobus`, package `conflate`
- **Relation to prior ADRs:** none superseded. The 2026-07-28 `Peek` decision is
  unaffected — it adds nothing to the send path, and a hub being peeked has a
  receiver by definition, so its fan-out still takes the lock.

## Context

`Sender.Send` took `s.mu` unconditionally, then called `sendLocked`, which read
`txClosed` and iterated a `receivers` map that may be empty. There was no
lock-free way to learn it was empty: `receivers` is a plain map under `s.mu`,
and the only count in the package (`forTestingReceiverCount`) is unexported and
also takes the lock.

That mutex is hub-wide. It is the same lock every receiver's pop, `Recv`,
`Peek`, `TryRecv` and `Close` takes, so a publisher on an unwatched hub
contended with everything it did not care about and nothing it did.

**The cost landed on the producer's hot path, not the subscriber's.** A producer
paid it per publish whether or not the value had any consumer, so the price of
"a hub exists" was paid by the system that feeds it rather than by the system
that reads it. Measured: 15.34 ns/op for a send nobody was listening to.

The motivating consumer publishes a watch value from inside its work queue's own
critical sections, so *every enqueue in its control plane* would take a hub
mutex — including in the overwhelmingly common case where no process anywhere is
watching. Its workaround was a subscriber count on the queue itself, incremented
and decremented under the queue lock with `Send` skipped at zero: an increment, a
guarded decrement, a panic on negative, a `sync.Once` teardown, and tests for all
of it. The failure modes are asymmetric — a missed decrement wastes work, a
double decrement silently stops delivering to live subscribers with no backstop.
None of that is about the consumer's domain, and **every conflate consumer with a
hot producer would write it again.**

## Decision

Keep a lock-free copy of the receiver count on `shared`, and return early from
`Send` when it reads zero.

```go
const sendPoisoned int64 = -1

type shared[K comparable, V any] struct {
	// ...
	liveReceivers atomic.Int64
}
```

Two rules make it correct, and both are load-bearing.

**The count is derived, never incremented.** `syncLiveLocked` stores
`len(s.receivers)` and is called at the two sites that mutate the map —
`receiver()` and `deregisterLocked()`. It is not an increment/decrement pair.
This is a direct response to the workaround above: a derived value cannot drift
from the map, and only one direction of drift is survivable.

- **Over-reporting is free.** A count that says non-zero on an empty set costs
  one uncontended lock, and `Send` then finds nothing — precisely the old
  behaviour.
- **Under-reporting loses a value permanently.** A publisher that reads zero
  while a receiver is registered drops the value, and a conflated bus has no
  retry: the next send for that key coalesces into a slot the subscriber was
  never told had been skipped.

**Close poisons the count.** `Sender.Close` and `Hub.Close` store
`sendPoisoned`, and `syncLiveLocked` returns early rather than clearing it. A
closed hub therefore never reads zero, always reaches `sendLocked`, and answers
`ErrClosed` from `txClosed` as before.

The poison is what keeps closedness out of the fast path, and it is the reason
`txClosed` stays a plain `bool` with a single access discipline instead of
gaining a second atomic mirror. It also cannot be deferred to a later commit:
`Hub.Close` empties `receivers` outright, and a `Receiver.Close` *after* a
`Sender.Close` deregisters through `syncLiveLocked`, so without the early return
either one writes a zero over the poison and turns `ErrClosed` into `nil`. The
existing `TestSenderCloseDrainsThenErrClosed` catches that the moment `Send`
reads the count.

### The fast path returns exactly one value: `nil`

`SendContext` inherits the path, but **only for a live context**. A cancelled
one falls through to the lock.

This is not a cost decision, it is a correctness one, and it is the subtlest
part of the change. The fast path reads two things at two moments: the count,
then `ctxDone`. A `Sender.Close` landing between them makes a cancellation
verdict correct at *neither* — `nil` is the answer at the load, `ErrClosed` is
the answer by the select — so returning `ctx.Err()` there reverses the
documented **closed > cancelled** order for a caller that ordered the close
first. Only `sendLocked` reads `txClosed` and `ctxDone` under one acquisition,
which is where that precedence can be derived instead of assembled.

So the invariant is: *the fast path produces no error verdict.* Every
`ErrClosed` and every `ctx.Err()` in the package still comes from the locked
core. Any future edit that lets the fast path return an error re-opens this.

`Send` itself is not exposed to the same defect: it makes a single read and
derives its verdict from that one moment, so a close landing afterwards is an
ordinary racing close — indistinguishable from one landing after the lock.

### What that costs the library

One field, one constant, two helpers, two call sites for each, and one extra
call to an existing test seam. No new option, no new receiver state, no new
exported symbol, no change to `enqueueLocked`, `popLocked`, `sendLocked`,
`Merge`, delivery order, or the memory bound. The entire receive side is
untouched, `gobus.go` is untouched, and `conformance_test.go` gained no row —
the common interfaces make no promise about lock use, and close/cancel
precedence is unchanged.

Nothing moved into `internal/buscore`. The fast path is one atomic field plus a
hub-specific poison rule, and it is not shared logic until a second bus type
wants it.

## Consumer-side obligations

**Register before you snapshot.** A subscriber must call `Hub.Receiver` before
it takes its own snapshot of the producer's state, never after:

```go
rx := hub.Receiver()   // register FIRST
state := snapshot()    // then read
```

A value published in the gap reaches no receiver, and conflate has no replay.

This requirement is not new — a conflate receiver has always observed only what
was sent after it was created — but the fast path makes it load-bearing in a
place a reader will not think to look, which is why it is stated in `Send`'s doc
comment rather than left implicit in the delivery model. A publisher that skips
the lock cannot notice a late subscriber, so the wrong order loses the value
permanently rather than merely racily.

The property the library owes in return is that **a registration completed
before a `Send` is always observed by that `Send`.** `receiver()` stores the new
count under `s.mu`; Go's `sync/atomic` operations are sequentially consistent, so
all operations on `liveReceivers` form one total order consistent with each
goroutine's program order. When the consumer supplies any happens-before edge
between its registration and the publish — returning from `Hub.Receiver` and
then synchronizing with the producer, which in the motivating case is the queue
lock the producer also takes — the load cannot return a stale zero. Where
registration and publication are genuinely concurrent, either answer is correct,
and the obligation above is what makes that acceptable.

## Consequences

**A cancelled send on an unwatched hub now takes the lock.** It did not need to
before, because everything took the lock. This is the price of resolving
precedence from one consistent view, and cancellation is not the hot path.

**The count is bus state read outside the lock**, which conflate previously did
not have — `rx.done` was the only lock-free pre-check. `CLAUDE.md`'s claims to
the contrary were amended in the same change rather than appended to, since that
file exists to prevent exactly this drift.

**The convention that every lock-free check is re-checked under `s.mu` still
holds**, and the fast path is not an exception to it. The count read is a
*no-op* check, not a closed check: it asks whether there is any work, never
whether the bus is alive, and the poison is what keeps closedness out of it. It
hands back no value, so there is nothing to re-check.

**A stale-high count is a permanent state after `Hub.Close`.** `Hub.Close` nils
the map without calling `syncLiveLocked`, so the count is left over-reporting
until the poison store lands in the same critical section. Harmless by the
asymmetry above, but it means a test asserting `ErrClosed` after `Hub.Close`
passes for two independent reasons — hence a separate pin on the poison itself.

**Measured** (`-benchtime 2000000x -count 3`, against `afe7951`):

| Benchmark | Before | After |
| --- | --- | --- |
| `SendNoReceivers` | 15.34 ns/op | 2.38 ns/op |
| `SendOneReceiver` | 76.17 ns/op | 76.03 ns/op |

6.4× on the unwatched hub. The locked path gains one uncontended atomic load,
which does not register against a mutex acquisition. Neither path allocates.
Take the baseline from a worktree at the parent commit — stashing `conflate.go`
does not undo a fast path already committed, and reports a false "before".

## Alternatives considered

**A second atomic mirroring `txClosed`.** The obvious way to keep `ErrClosed`
correct: check `liveReceivers == 0 && !txClosedAtomic`. Rejected because it gives
one field two access disciplines — the exact reason this package deliberately
does *not* copy gochan's `atomic.Bool` for `txClosed`, whose `watch`/`broadcast`
need it for a lock-free `TryRecv` path conflate has no equivalent of. Folding
closedness into the poison keeps `txClosed` read only under `s.mu`, and needs one
load rather than two.

**An increment/decrement counter.** What the downstream workaround did, and what
a bus-side version would inherit: a decrement that runs twice under-reports, and
under-reporting is the failure that loses values silently and forever. Deriving
from `len(s.receivers)` removes the failure mode rather than testing for it.

**An exported `ReceiverCount()`.** Rejected, and worth recording as rejected
because it is the shape a consumer will ask for. A count read outside the
consumer's own lock sits outside the order that makes its subscribe sequence
safe. Either the check lives inside `Send`, where the bus orders it against its
own registration, or it lives under the consumer's lock. A public count between
the two is the worst of both.

**A registration generation rather than a counter.** A generation would also let
a publisher cheaply detect *change* in the receiver set. Nothing needs that, and
a counter answers the only question the send side asks.

**Re-checking the count on the cancellation arm.** The narrower fix for the
two-read defect: reload `liveReceivers` when the cancellation wins, and fall
through if it changed. Correct, but it preserves the two-read shape and adds a
branch that has to stay right forever. Making the fast path incapable of
returning an error is a stronger invariant and less code.

**Putting the fast path in every bus type.** conflate is the only bus type
today. The problem is general to fanning out under one mutex, so a later bus may
lift the pattern into `internal/buscore` — but not before a second caller
exists.

## Implementation

Landed in five red/green cycles plus one fix: the derived count; running the
existing send seam from `Send` (without which the next cycle's lock counter is
vacuous — it reads zero whether or not a fast path exists); the fast path and its
poison together; `SendContext`; the docs; then the cancelled-send correction
above.

Two cycles came out differently from the plan, both because the existing suite
answered first. The fast path and the poison could not be separate commits, for
the reason given under *Decision*. And the `Hub.Close` test was green before the
change — the stale-high count already routed the send to the lock — so a
separate assertion on the poison was added rather than trusting a value that
over-reporting also produces.

`TestLiveCountTracksTheReceiverSet` is the real pin: it asserts `liveReceivers`
against `len(s.receivers)` after every mutation site, so a missed
`syncLiveLocked` fails at the site that is wrong. The happens-before property
above cannot be made to fail deterministically in a Go test, so
`TestRegistrationBeforeSendIsAlwaysObserved` is documented in-file as a `-race`
smoke test rather than as the guarantee.

Gates: `gofmt`, `go vet`, `staticcheck -checks=all`, `go test -race ./...`, and
100% coverage on the library packages — both fast-path returns, the poisoned
early return in `syncLiveLocked`, both `Close` poison sites, and the
cancellation fall-through.

## Still open

- Does a second bus type want this, and is `internal/buscore` the right home
  when it does?
- Is the register-before-snapshot obligation discoverable enough in `Send`'s doc
  comment, given that violating it now loses values permanently rather than
  racily?
- The downstream consumer's subscriber count, the `sync.Once` teardown and their
  tests are still to be deleted against this. The
  registration-before-read ordering stays there, because that is about the
  receiver's existence rather than about a count.
