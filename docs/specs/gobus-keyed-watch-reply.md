# Reply: `gobus/watch` specification, decisions D1 to D6

**Specification:** `docs/gobus-keyed-watch.md`
**From:** the requester (beehive)
**Language:** Simplified Technical English, to match the specification.

## 1. Verdict

The specification works for us. We ask for no change to sections 1 to 8.

`Accept` answers the problem that the order argument answered in the request.
It answers it better. The order argument gave the bus a number to compare. A
predicate gives the bus nothing to define. The bus does not need to know what
"old" means for our value.

R45 closes the last defect. An earlier draft let the feeder replace a waiting
value with any newer value. A reordered publish could therefore displace a
correct value that the consumer had not yet read. R45 now gates that
replacement on an accepted value, so a value that `Accept` rejects can displace
nothing.

## 2. The requirement that changed our design

**R18 is the one to keep.** `initial` is the `prev` for the first call to
`Accept`.

Our consumer reads its state under its own lock and calls `Watch` in the same
critical section. R18 makes that read the baseline for staleness. R21 makes the
same critical section safe against a concurrent producer.

Together they remove three things from our code:

- a per-stream version variable;
- a comparison in the read loop;
- a rule that says "register the receiver before you read the state", with the
  argument for it and the test that holds a goroutine in the window between the
  two.

We had that rule backwards in one revision of our own design document. The
error was silent. R18 and R21 remove the class.

R10 matters for the same reason. Two of our consumers can watch one key and
seed at different moments. A hub-wide evaluation would give one of them the
wrong answer.

## 3. Decisions

### D1. `Accept` serves the requester. Confirmed.

**Q1. Does the rule fit `func(prev, next V) bool`?**

Yes. Our rule is `next.Seq > prev.Seq`. We select one value. We never combine
two values.

**Q2. Can the order ride inside `V`?**

Yes. We keep our `stamped` wrapper. The request expected to delete it. We
prefer the predicate and the wrapper to an order argument and no wrapper.

**Q3. Does any consumer need to know that a value was rejected?**

No. A rejected value is a value that our own producer superseded. We would not
report it and we would not act on a count of it.

### D2. `Watch` should mark `initial` as read.

We prefer the alternative in D2, not R19.

The reason is that the caller supplies `initial`. To deliver it through the
channel is to return the caller's own argument to the caller. This differs from
`gochan/watch`, where the hub holds one seed from `New` and a later receiver
does not have that value.

Two smaller reasons support it:

- `TestSendContextPrecedenceConformance` needs no extra read.
- `TryRecv` that returns `gobus.ErrEmpty` then means "nothing changed since you
  subscribed". Under R19 it means "you have not read your own seed". The first
  meaning is more useful to a consumer.

R18 is true under both readings, so this decision costs us nothing either way.
We state a preference, not a requirement.

### D3. `Accept` does not need the key. Confirmed.

Our rule compares two values and nothing else. It is the same rule for every
key.

A second consumer of this package inside beehive would build its own hub with
its own rule. We do not foresee one hub with a rule that treats keys
differently.

### D4. Defer the per-watch `Accept`.

Agreed. Every watch on our hub uses one rule. We have no consumer that
disagrees with a sibling consumer.

### D5. No consumer watches many keys. Confirmed.

Our watch method takes one object id. Each subscriber therefore holds one
receiver and one goroutine.

State one point so that it stays true: our wide stream is an object list watch.
That stream reports create, modify and delete, and it needs annihilation for a
create-then-delete pair. Section 4 assigns that stream to `conflate`, and we
agree. R4 must not become a reason to widen this package later. The wide
consumer belongs to the other bus.

### D6. `Option[V]` is correct. Confirmed.

Our call site is:

```go
watch.New[ObjectID](watch.WithAccept(func(prev, next stamped) bool {
	return next.Seq > prev.Seq
}))
```

`V` comes from the function. The call site spells `K` only. We foresee no hub
option that depends on `K`.

## 4. What we keep on our side

Record this so that a reader does not read it as redundant.

**We keep a value comparison in the read loop.** It is not a staleness check.
`Accept` owns staleness.

The comparison exists because of R26. Our value can change from A to B and back
to A while the consumer does not read. The receiver then delivers A only, and A
equals the value that the consumer last used. Our contract states that a
repeated value must be impossible, so the consumer drops it.

This is a property of a gauge, not of the bus. No bus requirement removes it.

## 5. One measurement, not a change request

R30 guarantees the fast path when the hub has no live receiver. R31 makes the
per-key skip an optimization and not a guarantee. We accept that.

We note where it lands for us. Our producer publishes from the work queue, and
every unit of work in the control plane passes through that queue. Our common
case is zero receivers, which R30 covers. With one subscriber on one object,
every publish for every other object reaches the locked path.

This does not block us and we do not ask for a guarantee. It is the first
number we will measure, and we will report it.

## 6. Housekeeping

The specification header names `docs/gobus-keyed-watch.md` as the source
request. That is now the path of the specification itself. The original request
was replaced in place. A reader who follows the reference finds the
specification again.

## Appendix: our call site

This is the consumer, in the shape the specification produces. `workQueue` is
our producer. Its lock is `q.mu`. `gauge` owns the state that the stream
reports.

### The hub

```go
// stamped is a schedule plus the order it moved in. Seq is assigned under
// q.mu, so it is the producer's order and not an arrival order.
type stamped struct {
	Schedule Schedule
	Seq      uint64
}

hub := watch.New[ObjectID](watch.WithAccept(func(prev, next stamped) bool {
	return next.Seq > prev.Seq
}))
```

### The producer

```go
// publish hands a move to the hub after q.mu is released. Send takes the hub
// lock, and to nest that inside q.mu would put a second lock on the hot path
// of every enqueue in the system.
//
// Two publishes for one key can therefore reach Send in the reverse of the
// order in which they became true. Accept resolves that, per R16.
func (q *workQueue) publish(id ObjectID, s stamped) {
	if q.schedules == nil {
		return // no hub: this kind has no controller
	}
	_ = q.schedules.Send(id, s)
}
```

### The subscriber

```go
// watchSchedule reads the current schedule and registers the watch in one
// critical section. R20 makes Watch safe under our lock. R18 makes cur the
// prev of the first Accept call, so a publish that predates this read is
// rejected by the same rule that rejects a reordered one.
func (q *workQueue) watchSchedule(id ObjectID) (*watch.Receiver[ObjectID, stamped], stamped) {
	q.mu.Lock()
	defer q.mu.Unlock()
	cur := q.gauge.at(id)
	return q.schedules.Watch(id, cur), cur
}

func (c *clientImpl[Spec, Status]) SchedulesWatch(ctx context.Context, id ObjectID) (<-chan Schedule, error) {
	r, ok := c.bh.reconcilerFor(c.gk)
	if !ok {
		return nil, ErrNoController
	}
	rx, cur := r.work.watchSchedule(id)

	out := make(chan Schedule)
	go func() {
		defer close(out)
		defer rx.Close()

		last := cur.Schedule
		if !sendOrDone(ctx, out, cur.Schedule) {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case ev, open := <-rx.Chan():
				if !open {
					return
				}
				// No staleness check. Accept rejected every value that our
				// producer superseded. This comparison covers R26 only: the
				// gauge can move away and back while nobody reads, and a
				// repeated value must not reach the consumer.
				if ev.Value.Schedule == last {
					continue
				}
				last = ev.Value.Schedule
				if !sendOrDone(ctx, out, ev.Value.Schedule) {
					return
				}
			}
		}
	}()
	return out, nil
}
```

### Shutdown

```go
// stop cancels the timers, then publishes the final state of each key that
// held one. Sender.Close then ends each stream after the feeder drains, per
// R38 and R46.
func (q *workQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	final := q.gauge.clearAllAlarms()
	q.mu.Unlock()

	for _, f := range final {
		q.publish(f.ID, f.stamped)
	}
}
```
