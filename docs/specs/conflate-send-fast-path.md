# Spec: a lock-free no-receiver fast path for conflate.Send

- **Status:** ready to implement — **upstream**
- **Date:** 2026-08-01
- **Scope:** `github.com/amorey/gobus`, package `conflate`
- **Files:** `conflate/conflate.go`, `conflate/conflate_test.go`,
  `conflate/helpers_test.go`, `README.md`, `CLAUDE.md`, `docs/adr/`
- **Related:** `schedule-push`, a beehive spec. This change deletes the
  workaround in that spec ("This count is a workaround, and it has a clean
  upstream fix").

**`schedule-push` is not in this repository.** It is a beehive document, and
gobus is the upstream library. Sections 2 and 10 report its content and are not
verifiable here. Nothing in sections 3 to 9 depends on it: the design, the
correctness argument and the test plan stand on this repository alone. Treat
section 2 as motivation only, and confirm section 10 against beehive before you
delete anything there.

This document is written in Simplified Technical English. Each requirement is
one sentence. The word **must** marks a requirement. The word **may** marks a
permitted option.

## 1. Problem

`conflate.Sender.Send` takes the hub mutex on every call:

```go
func (tx *Sender[K, V]) Send(k K, v V) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.sendLocked(nil, k, v)
	return err
}
```

`sendLocked` then reads `txClosed` and iterates `s.receivers`. The map can be
empty. A publisher cannot learn that the map is empty without the lock.
`s.receivers` is a plain map under `s.mu`. The only count in the package is
`forTestingReceiverCount`, which is unexported and also takes the lock.

The mutex is hub-wide. Every receiver pop, `Recv`, `Peek`, `TryRecv` and `Close`
takes the same lock. A publisher on an unwatched hub therefore contends with
work that it does not use.

The cost falls on the producer, not on the consumer. The producer pays the cost
for each publish, also when no consumer exists. The system that feeds the hub
pays the price of the hub, and the system that reads the hub does not.

## 2. Why beehive needs it

`SchedulesWatch` reports the next-requeue time of the work queue. Under
`schedule-push` (beehive), beehive publishes that value from inside the
critical sections of the queue. Every enqueue in the control plane then takes a
hub mutex. Almost always, no process watches a schedule.

The workaround in beehive is a subscriber count on `workQueue`. Beehive
increments and decrements the count under the queue lock, and skips `Send` at
zero. The workaround operates, and it costs:

- one increment, one guarded decrement and one panic on a negative count;
- one `sync.Once` teardown that must run exactly one time;
- tests for all of the above.

The failure modes are asymmetric. A missed decrement wastes work. A double
decrement stops delivery to live subscribers, and no backstop exists.

None of this logic is about schedules. It repeats a check that the bus can make
better. Each conflate consumer with a hot producer writes this logic again.

## 3. Design

Keep a lock-free copy of the receiver count on `shared`. Return early from
`Send` when the copy is zero.

### 3.1 The poison rule

`ErrClosed` must not become `nil`. A closed sender must report `ErrClosed`, also
when the receiver set is empty. `Hub.Close` also clears `s.receivers`, so a
count-first check without protection turns `ErrClosed` into `nil`. This is the
primary trap of this change.

Do not solve this with a second atomic mirror of `txClosed`. `txClosed` must
keep one access discipline: read and written under `s.mu` only. (gochan makes
its equivalent field an `atomic.Bool` because its `watch`/`broadcast` read it
from a lock-free `TryRecv` path. conflate has no such path.)

Solve it with a poison value in the same field. Once the send side closes, the
count becomes `-1` permanently. The count is then never zero, so the fast path
never fires on a closed hub, and `Send` reaches `sendLocked` and returns
`ErrClosed` as before.

### 3.2 New state

Add one field to `shared[K, V]` and one package constant:

```go
// sendPoisoned is the value liveReceivers holds after the send side closes. It
// is not a count: it only has to be non-zero, so that a closed hub always takes
// the locked path and reports ErrClosed from sendLocked.
//
// The type is explicit so that Load() == sendPoisoned and Store(sendPoisoned)
// need no conversion at the call sites.
const sendPoisoned int64 = -1

type shared[K comparable, V any] struct {
	mu        sync.Mutex
	// ...

	// liveReceivers is a lock-free copy of len(receivers), for the no-receiver
	// fast path in Send. It is written only under mu, at each site that mutates
	// receivers, and read without mu by the send side. A zero value means "no
	// receiver is registered and the send side is open", which is the only state
	// in which a publisher may skip the lock. Sender.Close and Hub.Close set it
	// to sendPoisoned, and no later write clears that: a closed hub must reach
	// sendLocked to report ErrClosed, and an empty receiver map would otherwise
	// report nil.
	liveReceivers atomic.Int64
}
```

The zero value of `atomic.Int64` is `0`, and a new hub has no receivers.
Therefore `New` must not initialize the field.

Add `"sync/atomic"` to the imports of `conflate/conflate.go`.

### 3.3 New helpers

```go
// syncLiveLocked refreshes the lock-free receiver count from the map that owns
// the truth. It is a no-op after the send side has poisoned the count. Caller
// holds s.mu.
func (s *shared[K, V]) syncLiveLocked() {
	if s.liveReceivers.Load() == sendPoisoned {
		return
	}
	s.liveReceivers.Store(int64(len(s.receivers)))
}

// poisonLiveLocked stops the send fast path for the life of the hub. Caller
// holds s.mu.
func (s *shared[K, V]) poisonLiveLocked() { s.liveReceivers.Store(sendPoisoned) }
```

`syncLiveLocked` derives the count from `len(s.receivers)`. It does not
increment or decrement. This is deliberate: a derived value cannot drift from
the map, and drift downward loses values permanently (see section 4). This is
also the property that beehive's manual count does not have.

### 3.4 Call sites

The count must change only where `s.receivers` changes. All of these sites
already hold `s.mu`.

| Site | Function | Action |
| --- | --- | --- |
| `conflate.go` `receiver()` | after `h.s.receivers[rx] = struct{}{}` | `h.s.syncLiveLocked()` |
| `conflate.go` `deregisterLocked()` | after `delete(rx.s.receivers, rx)` | `rx.s.syncLiveLocked()` |
| `conflate.go` `Sender.Close()` | after `s.txClosed = true` | `s.poisonLiveLocked()` |
| `conflate.go` `Hub.Close()` | after `s.txClosed = true` | `s.poisonLiveLocked()` |

`deregisterLocked` covers `Receiver.Close` and every terminal `ErrClosed`
verdict in `recvLoop`, `TryRecv`, `Peek` and `feed`. Do not add the call to
those paths one by one.

`Hub.Receiver` on a closed hub returns a pre-closed handle and does not register
it. That branch must not touch the count.

### 3.5 Send

```go
func (tx *Sender[K, V]) Send(k K, v V) error {
	s := tx.s
	if s.liveReceivers.Load() == 0 {
		return nil // no receiver, and the send side is open: nothing to fan out
	}
	if s.forTestingBeforeSendLock != nil {
		s.forTestingBeforeSendLock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.sendLocked(nil, k, v)
	return err
}
```

`TrySend` delegates to `Send` and needs no change.

`sendLocked` needs no change. The fast path is in front of the lock, not inside
it, and the locked path keeps today's order of checks.

### 3.6 SendContext

The fast path must not hide a cancellation. A cancelled context must report
`ctx.Err()`, also on an unwatched hub.

```go
func (tx *Sender[K, V]) SendContext(ctx context.Context, k K, v V) error {
	s := tx.s
	ctxDone := ctx.Done()
	if s.liveReceivers.Load() == 0 {
		// A zero count means the send side was open at the load, which is all the
		// precedence needs: this send resolves at that load, and there is no later
		// point at which it could observe more. Only the cancellation remains.
		select {
		case <-ctxDone:
			return ctx.Err()
		default:
			return nil
		}
	}
	// ... today's body, unchanged.
}
```

The order stays **closed > cancelled > value** on both paths:

- A zero count proves that the send side was open **at the load**, because close
  poisons the count. `ErrClosed` is therefore not a possible answer on the fast
  path. The load is where this send resolves, so a `Sender.Close` that lands
  after it is a close that races the call, exactly as today.
- A poisoned or positive count sends the call to `sendLocked`, which tests
  `txClosed` before it tests `ctxDone`.

`ctx.Err()` is still called outside `s.mu`, because the fast path takes no lock.
`ctx.Done()` is still taken before any lock. The rule that the bus never calls
into a caller's context under `s.mu` is unchanged.

### 3.7 The test seam

`forTestingBeforeSendLock` becomes the seam that proves the fast path skips the
lock. Two rules make this work:

1. `Send` must call the hook, in addition to `SendContext`.
2. Neither fast path may call the hook.

A test then arms the hook with a counter. Zero calls mean zero lock
acquisitions. This needs no timing and no new field.

Update the doc comment of `forTestingBeforeSendLock`. State that `Send` and
`SendContext` both run it, and that it runs only on the locked path.

The existing hook tests keep their meaning. Both
`TestSendContextChecksCancellationAtTheLockNotAtEntry` cases have a live
receiver or a closed sender, so both keep taking the locked path.

## 4. The correctness question

**Only one direction of error is safe.**

- **Over-report is free.** If the count reads non-zero while the set is empty,
  `Send` takes the lock and finds nothing. That result is today's behaviour, so
  a stale-high count costs one uncontended lock. The permanent poison after
  close is exactly this case.
- **Under-report loses a value permanently.** If a publisher reads zero while a
  receiver is registered, the bus drops the value. A conflated bus has no retry:
  the next send for that key coalesces into a slot, and the subscriber never
  learns that the bus skipped a value.

The implementation must therefore hold this property:

> A registration that completed before a `Send` is always observed by that
> `Send`.

### 4.1 The argument

1. `receiver()` stores the new count under `s.mu`, after it adds `rx` to the
   map. The stored value is at minimum `1`.
2. The Go memory model specifies that the operations of `sync/atomic` behave
   like sequentially consistent atomics. All operations on `liveReceivers`
   therefore form one total order, and that order agrees with the program order
   of each goroutine.
3. "The registration completed before the `Send`" means that a happens-before
   edge exists from the store to the load. `Hub.Receiver` returns to the
   subscriber, and the subscriber then synchronizes with the publisher. In
   beehive, that edge is the queue lock: the subscriber registers and then reads
   the queue under the queue lock, and the publisher writes the queue under the
   same lock before it publishes.
4. With such an edge, the load must return the stored value or a later value in
   the total order. A later value is `1` or more, or the poison. Each of these
   is non-zero. The `Send` therefore takes the lock and observes the receiver.

The `s.mu` release in `receiver()` is a second, independent edge for any
publisher that takes `s.mu`. The atomic order is what protects the publishers
that do **not** take it.

### 4.2 The concurrent case

A registration and a publication may be truly concurrent, with no
happens-before edge between them. Then both answers are correct. The value is
delivered, or the value is dropped, and the caller cannot have depended on
which. This is only sound because of the consumer contract in section 4.3.

### 4.3 The consumer contract

A bus that drops a publish when no receiver is registered requires each
subscriber to **register before it takes its snapshot**, not after. This is
already true of conflate today. `schedule-push` (beehive) already
requires the same order of beehive. An earlier revision of that spec had the
order reversed, and would have lost a move permanently.

The fast path does not create this requirement. It makes the requirement
load-bearing at a place where a reader does not expect it. The implementation
must therefore state the contract in the doc comment of `Send` (section 6.1).

## 5. What does not change

- **Semantics.** A `Send` to a hub with no receivers already fans out to nobody
  and returns `nil`. The fast path gives the same answer without a lock.
- **`gobus.go`.** The common interfaces make no promise about lock use, and the
  close and cancel order is unchanged. Do not edit `gobus.go`.
- **`conformance_test.go`.** No new row and no new assertion. The suite drives
  the same public behaviour.
- **The receive side.** No file outside `conflate/conflate.go` changes, except
  the tests and the documents in section 6.
- **`internal/buscore`.** Nothing moves there. The fast path is one atomic field
  with a hub-specific poison rule, and it is not yet shared logic.

## 6. Documentation deliverables

Sections 6.1 to 6.7 are **not optional**. Four existing statements in this
repository become false with this change. Each one is a statement that a later
reader will trust, and two of them are in the file that exists to prevent this
class of drift. Sections 6.2, 6.5 and 6.6 correct them. An implementer who
follows `CLAUDE.md` and an implementer who follows this spec must produce the
same code.

### 6.1 `Send` doc comment

Extend the comment on `Sender.Send`. Add these points:

- A `Send` to a hub with no live receiver returns `nil` and does nothing.
- A subscriber must call `Hub.Receiver` **before** it takes its own snapshot of
  the producer state. A value that is published between the snapshot and the
  registration is not delivered, and conflate has no replay.
- The fast path does not change the result, only the cost.

### 6.2 `SendContext` doc comment — correction

The comment on `Sender.SendContext` currently makes two claims that the fast
path breaks:

> That single check is made where the send is *resolved*, under `s.mu`, not on
> entry.

> […] and never gates the call on anything but the bus lock.

Both sentences must change. The principle behind them survives, and the
statement of it does not:

- The check is still made where the send **resolves**. On the fast path, the
  send resolves at the atomic load, not under `s.mu`. Rewrite the claim in terms
  of the resolution point, and not in terms of the lock.
- On the locked path, nothing changes. Keep the whole existing argument for why
  the check sits under `s.mu` rather than at entry, and keep the reference to
  `TestSendContextChecksCancellationAtTheLockNotAtEntry`.
- State that a hub with no live receiver resolves the send at the load, and that
  it then reports a cancelled `ctx` and publishes nothing.

Do not touch the comment on `sendLocked`. It describes the locked core only,
and its claim of "a single consistent view of `txClosed`" stays true.

### 6.3 Package doc

Add one short paragraph to the `# Semantics` section of the package comment in
`conflate/conflate.go`. Use the heading idea "A send with no receivers is
cheap". State the fast path, and state the register-before-snapshot contract.

### 6.4 `README.md`

Add a subsection under **Conflate**, after "Inspecting the backlog head". Cover:

- `Send` on a hub with no live receiver takes no lock.
- Why this matters: the hub lock serializes the whole fan-out, so a hot producer
  on an unwatched hub pays for a bus that nobody reads.
- The register-before-snapshot contract, with the reason.

Add one sentence to **Thread safety**: `Send` reads a lock-free receiver count
first and takes the hub lock only when a receiver exists.

The **Close / cancel precedence** subsection says that `SendContext` resolves
`ctx` "under the bus lock, not on entry". Qualify it in the same way as section
6.2: on a hub with no live receiver there is no lock, and the send resolves at
the count read.

Do not document the count as a public value. See section 7.

### 6.5 `CLAUDE.md` — Architecture

**Amend the existing paragraph. Do not only append a bullet.** Two sentences in
the `SendContext` paragraph become false:

> […] so no bus state is read outside the lock and `txClosed` stays a plain
> `bool`.

`liveReceivers` **is** bus state that is read outside the lock. Rewrite the
clause. The conclusion about `txClosed` survives, and the premise does not: the
reason `txClosed` stays a plain `bool` is that the fast path carries closedness
in the poison, and not that no state is read outside the lock.

> […] conflate has no such path (its only lock-free pre-check is `rx.done`)
> […]

conflate now has a second lock-free pre-check. Rewrite the parenthesis to name
both `rx.done` and `liveReceivers`. Keep the conclusion, and give it its new
reason: an atomic `txClosed` would still buy nothing, because the poison already
routes every closed hub to the locked path, where the plain `bool` is read.

Then add one bullet to **Architecture**, after "Coalescing happens at Send, not
at Recv". Record the invariant, because it is not local to one function:

- `liveReceivers` is a lock-free copy of `len(s.receivers)`, written only under
  `s.mu`, at each site that mutates the map.
- `syncLiveLocked` derives the count and never increments it.
- Close poisons the count permanently, so `ErrClosed` cannot become `nil`. The
  early return in `syncLiveLocked` is what holds the poison: without it, a
  `Receiver.Close` after a `Sender.Close` restores a zero count.
- Under-report loses a value permanently, and over-report costs one lock.

### 6.6 `CLAUDE.md` — Conventions

**Amend the existing convention.** It reads:

> Lock-free closed checks are always re-checked under `s.mu` before handing back
> a value. […] Every such re-check has a `forTesting*` hook so the race can be
> exercised deterministically.

The send fast path returns without any lock, so it has no re-check. This is
consistent with the convention rather than an exception to it, and the file must
say why:

- The count read is **not** a closed check. It is a no-op check: it asks whether
  there is any work, and not whether the bus is alive.
- The poison is what keeps closedness out of it. A closed hub never reads zero,
  so a closed check is never resolved without the lock.
- The convention therefore holds unchanged for closed checks. Add the count read
  as the one lock-free check that hands back **no value**, so there is nothing to
  re-check.
- The hook rule also holds. `forTestingBeforeSendLock` covers this path, and it
  is the seam that proves the lock was skipped (section 3.7).

### 6.7 ADR

Add `docs/adr/2026-08-01-conflate-send-fast-path.md` when the change is
accepted. Mirror `docs/adr/2026-07-28-conflate-peek.md`: Context, Decision, what
it costs the library, Alternatives. Record the two rejected alternatives from
section 7.

## 7. Decisions on the earlier open questions

**A counter, not a generation.** A counter answers the only question that the
send side asks. A generation would also let a publisher detect *change* in the
receiver set, and nothing needs that today.

**`conflate` only.** conflate is the only bus type in the module today. The
problem is general to fan-out under one mutex. A later bus type may lift the
pattern into `internal/buscore`, and it must not do so before a second caller
exists.

**No exported `ReceiverCount()`.** Do not add one, and do not replace beehive's
count with one. A count that is read outside the queue lock of the consumer sits
outside the order that makes the beehive subscribe sequence safe. The check
belongs inside `Send`, where the bus orders it against its own registration, or
under the lock of the consumer. A public count between the two is the worst
option.

**No second atomic for `txClosed`.** See section 3.1.

## 8. Test plan

Tests go in `conflate/conflate_test.go`. One new helper goes in
`conflate/helpers_test.go`. CI enforces 100% coverage of library packages, so
each new branch needs a test. Use testify. Do not use sleeps or timeouts to
coordinate goroutines.

| Test | Pins |
| --- | --- |
| `TestLiveCountTracksTheReceiverSet` | The structural invariant of section 8.2: `liveReceivers == len(receivers)` after each mutation site, while the count is not poisoned. |
| `TestSendWithNoReceiversSkipsTheBusLock` | `Send` and `TrySend` on a hub with no receiver return `nil`, and the hook fires zero times. |
| `TestSendContextWithNoReceiversSkipsTheBusLock` | A live ctx returns `nil`, and the hook fires zero times. |
| `TestSendContextCancelledWithNoReceiversReportsCancellation` | A cancelled ctx returns `ctx.Err()` on an empty hub. |
| `TestClosedSenderReportsErrClosedWithNoReceivers` | `Sender.Close`, then `Receiver.Close`, then `Send` returns `ErrClosed`. This is the poison rule and the main trap. |
| `TestHubCloseReportsErrClosedWithNoReceivers` | `Hub.Close` clears `receivers`, and `Send` still returns `ErrClosed`. |
| `TestDrainToErrClosedKeepsTheSenderClosed` | The terminal-`ErrClosed` route into `deregisterLocked`, which `TestClosedSenderReportsErrClosedWithNoReceivers` does not take: the receiver drains itself out of the map instead of being closed by the caller. |
| `TestFastPathFollowsTheReceiverSet` | The hook count shows: no receiver → no lock; one receiver → lock; last receiver closed → no lock; new receiver → lock again. |
| `TestRegistrationBeforeSendIsAlwaysObserved` | A smoke test of section 4 under `-race`. See section 8.3 for what it does **not** pin. |

The behaviour of a hub with one receiver is already pinned by the existing
suite: `TestBasicDelivery`, `TestCoalesceLatestWins`, `TestAnnihilation`,
`TestWithKeyFilterFiltersAtEnqueue` and `TestWithMergeIsPerReceiver` all take
the locked path unchanged. Add no test for that. A regression there fails the
suite that exists.

### 8.1 The hook counter

```go
var locks atomic.Int64
h.s.forTestingBeforeSendLock = func() { locks.Add(1) }
```

The hook runs on the sending goroutine, so a plain `int` counter races as soon
as a test has two publishers. Use `atomic.Int64` even in a single-goroutine
test, so a later edit that adds a goroutine does not introduce a silent data
race.

Arm the hook only while no send is in flight. The field is hub-wide and is read
outside `s.mu`, as its own comment states.

### 8.2 The structural invariant — the real pin

The property of section 4 is a happens-before property, and no Go test can make
its violation deterministic. What **is** deterministic is the invariant that the
property rests on:

> While the count is not poisoned, `liveReceivers` equals `len(s.receivers)`
> after every mutation of the map.

Add one test-only accessor next to `forTestingReceiverCount` in
`conflate/helpers_test.go`:

```go
// forTestingLiveReceivers returns the lock-free receiver count. It takes s.mu so
// that a test reads it and len(s.receivers) as one consistent pair; the fast
// path itself reads the field without the lock, which is the point of it.
func (h *Hub[K, V]) forTestingLiveReceivers() int64 {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return h.s.liveReceivers.Load()
}
```

`TestLiveCountTracksTheReceiverSet` then asserts the pair after each mutation
site: a fresh hub, one register, a second register, one `Receiver.Close`, a
drain to terminal `ErrClosed`, and both `Close` paths. A missed
`syncLiveLocked` call site fails this test immediately, and it does so at the
site that is wrong.

This test carries the weight. Section 8.3 does not.

### 8.3 The ordering smoke test

`TestRegistrationBeforeSendIsAlwaysObserved` states the section 4 property in
the shape a consumer meets it:

1. Start a publisher goroutine. The publisher waits on a channel, then sends one
   value for one key.
2. On the main goroutine, create the receiver with `hub.Receiver()`.
3. Close the channel. The channel close is the happens-before edge, and it
   models the queue lock of beehive.
4. Assert that the receiver receives the value.
5. Repeat the sequence in a loop, with a fresh hub each round.

Run it under `-race`. The loop count must be a constant in the test, not a
duration.

Know its limit. It fails only if the store is dropped completely, and no loop
count makes a memory-ordering violation deterministic. It is a smoke test and a
readable statement of intent. Section 8.2 is the pin.

Do not assert the concurrent case of section 4.2. Both outcomes are correct
there, so a test of it pins nothing.

### 8.4 Benchmarks

The whole justification of this change is the cost argument of section 1. Do not
assert that argument without a measurement.

```go
func BenchmarkSendNoReceivers(b *testing.B)  // fast path
func BenchmarkSendOneReceiver(b *testing.B)  // locked path
```

Run both, and put the before and after numbers in the pull request. The
benchmarks need not be committed, and they do not count for coverage.

Measured on this change, `-benchtime 2000000x -count 3`, against `afe7951` as
the baseline:

| Benchmark | Before | After |
| --- | --- | --- |
| `BenchmarkSendNoReceivers` | 15.34 ns/op | 2.38 ns/op |
| `BenchmarkSendOneReceiver` | 76.17 ns/op | 76.03 ns/op |

The unwatched hub is 6.4 times faster. The locked path is unchanged inside the
noise: it gains one uncontended atomic load, which does not register against a
mutex acquisition. Neither path allocates.

Take the baseline from a git worktree at the parent commit. A `git stash` of
`conflate.go` alone does not undo a fast path that is already committed, and it
reports a false "before".

## 9. Acceptance checklist

- [ ] `conflate/conflate.go`: field, constant, two helpers, four call sites,
      `Send`, `SendContext`, the hook rule, the doc comments.
- [ ] `conflate/conflate_test.go`: the tests of section 8.
- [ ] `conflate/helpers_test.go`: `forTestingLiveReceivers`.
- [ ] `README.md`: sections 6.4.
- [ ] `CLAUDE.md`: sections 6.5 and 6.6, as amendments and not appends.
- [ ] `conflate/conflate.go` doc comments: sections 6.1, 6.2 and 6.3.
- [ ] Benchmark numbers, before and after, in the pull request (section 8.4).
- [ ] `test -z "$(gofmt -l .)"`
- [ ] `go vet ./...`
- [ ] `staticcheck -checks=all ./...`
- [ ] `go test -race ./...`
- [ ] 100% coverage of `conflate` and the root package, with
      `-coverpkg=./...`.
- [ ] `docs/adr/2026-08-01-conflate-send-fast-path.md` on acceptance.

## 10. What this deletes downstream

In beehive, the whole "no-subscriber case" of
`schedule-push` (beehive): the count, the increment, the guarded
decrement, the panic on a negative count, and the tests of all three.

The register-before-read order stays. That order is about the existence of the
receiver, and not about a count.

## 11. Implementation plan — red/green cycles

Build the change in five cycles. Each cycle is one red test, one green
implementation, and one commit. Each cycle is small enough to read alone.

A cycle is only valid if the red test fails **for the stated reason**. A test
that passes before the implementation pins nothing. Section 11.3 and section
11.4 both depend on the seam of section 11.2, so keep the order.

### 11.1 Cycle 1 — count the receivers

- **Red:** `TestLiveCountTracksTheReceiverSet`. The test does not compile,
  because the field and the accessor do not exist.
- **Green:** add `liveReceivers`, `syncLiveLocked` **without** the poison guard,
  the two map-mutation call sites (`receiver`, `deregisterLocked`), and
  `forTestingLiveReceivers`.
- **Behaviour:** unchanged. Nothing reads the count yet.
- **Commit:** `feat(conflate): count the live receivers outside the bus lock`

### 11.2 Cycle 2 — make the lock observable

- **Red:** `TestSendTakesTheBusLockWithAReceiver`. `Send` does not run the seam
  today, so the counter stays at zero.
- **Green:** call `forTestingBeforeSendLock` from `Send`, before the lock.
  Update the doc comment of the field.
- **Behaviour:** unchanged in production, where the hook is nil.
- **Commit:** `test(conflate): run the send seam from Send as well`

This cycle exists because the cycle 3 test is vacuous without it. With no seam
in `Send`, a lock counter reads zero whether or not the fast path exists.

### 11.3 Cycle 3 — the fast path and its poison

The fast path and the poison are one cycle, and not two. The existing
`TestSenderCloseDrainsThenErrClosed` fails the moment `Send` reads the count, so
a fast path without the poison cannot be committed green. The suite already
pins the trap of section 3.1.

- **Red:** `TestSendWithNoReceiversSkipsTheBusLock` and
  `TestFastPathFollowsTheReceiverSet`, because `Send` takes the lock with no
  receiver. Then `TestClosedSenderReportsErrClosedWithNoReceivers` and
  `TestDrainToErrClosedKeepsTheSenderClosed`, which get `nil` where `ErrClosed`
  is required.
- **Green:** return `nil` from `Send` when the count reads zero. Add
  `sendPoisoned`, `poisonLiveLocked`, the two `Close` call sites, and the early
  return in `syncLiveLocked`. Extend `TestLiveCountTracksTheReceiverSet` with
  the poison assertions.
- **Commit:** `perf(conflate): skip the bus lock when no receiver listens`

The first test must close the **receiver after the sender**. That order is what
makes the early return in `syncLiveLocked` load-bearing: without it, the
deregistration writes a zero over the poison.

`TestHubCloseReportsErrClosedWithNoReceivers` is green before the change, and it
stays. `Hub.Close` sets `s.receivers` to nil without a `syncLiveLocked` call, so
the count is left stale-high and the send takes the lock. That is
over-reporting, which section 4 permits. Add
`TestHubCloseAlsoPoisonsTheCount` next to it, to pin the poison at that site
directly rather than through a value that a stale count also produces.

### 11.4 Cycle 4 — the fast path in SendContext

- **Red:** `TestSendContextWithNoReceiversSkipsTheBusLock`. `SendContext` takes
  the lock with no receiver.
- **Green:** add the zero-count branch, with the cancellation check inside it.
  Add `TestSendContextCancelledWithNoReceiversReportsCancellation` in the same
  cycle. That test passes before the change and guards the branch after it, so
  write it green and say so.
- **Commit:** `perf(conflate): skip the bus lock in SendContext as well`

### 11.5 Cycle 5 — the documents

- **Red:** none. `TestRegistrationBeforeSendIsAlwaysObserved` is the smoke test
  of section 8.3, and it is green by construction.
- **Green:** all of section 6, and the smoke test.
- **Commit:** `docs(conflate): record the send fast path and its contract`

### 11.6 After the last cycle

Run the full gate of section 9. Then measure the benchmarks of section 8.4.
