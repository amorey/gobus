// watch/examples/recv demonstrates the Recv()-based API for a keyed state bus
// — the "subscribe to one object's current state" pattern.
//
// A scheduler owns the state of several jobs under its own lock. It computes a
// change under that lock and publishes *after* releasing it, because nesting
// the bus lock inside the scheduler's lock would put a second mutex on the hot
// path of every state change. Two changes to one job can therefore reach Send
// in the reverse of the order in which they became true.
//
// That reordering is what Accept resolves. Each change carries the sequence
// number it was assigned under the scheduler's lock, and Accept keeps the
// higher one — so the value a subscriber ends on is the value that was true
// last, not the one that happened to win the race for the bus lock.
//
// Two properties fall out of Watch taking the caller's own value:
//
//   - A subscriber reads the current state and registers in ONE critical
//     section. Watch calls no caller code, so it is safe under the producer's
//     lock, and nothing published in between is lost.
//   - That value is the baseline, not a delivery. The bus never hands it back;
//     the subscriber already has it. A value only arrives once it supersedes
//     the baseline, which is why two subscribers registering at different
//     moments disagree about whether the same publish is news.
//
// Graceful shutdown uses Sender.Close (the soft path): a subscriber holding an
// unread value reads it once more before Recv returns ErrClosed. Hub.Close is
// hard tear-down and skips that.
//
// Run:
//
//	go run ./watch/examples/recv
package main

import (
	"errors"
	"fmt"
	"sync"

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/watch"
)

// Schedule is one job's current state. Seq is assigned under the scheduler's
// lock, so it is the order the change became true — not the order it reached
// the bus.
type Schedule struct {
	State string // "queued", "running", "done"
	Seq   uint64
}

type scheduler struct {
	hub *watch.Hub[string, Schedule]
	tx  *watch.Sender[string, Schedule]

	mu    sync.Mutex
	seq   uint64
	state map[string]Schedule
}

func newScheduler() *scheduler {
	// Accept is the whole policy: keep the change that happened later,
	// whichever order the two Sends arrive in. Without the option every value
	// would be accepted, which is last-writer-wins on arrival order.
	hub := watch.New[string](watch.WithAccept(func(prev, next Schedule) bool {
		return next.Seq > prev.Seq
	}))
	return &scheduler{hub: hub, tx: hub.Sender(), state: map[string]Schedule{}}
}

// commit applies a change under the scheduler's lock and returns it stamped.
// It does not publish: see publish.
func (s *scheduler) commit(job, state string) Schedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	v := Schedule{State: state, Seq: s.seq}
	s.state[job] = v
	return v
}

// publish hands a committed change to the bus, outside the scheduler's lock.
func (s *scheduler) publish(job string, v Schedule) { _ = s.tx.Send(job, v) }

func (s *scheduler) advance(job, state string) { s.publish(job, s.commit(job, state)) }

// watchJob reads the job's current state and registers the watch in one
// critical section. This is the ordering rule that Watch's signature removes:
// there is no window between the read and the registration for a publish to
// fall into.
func (s *scheduler) watchJob(job string) (*watch.Receiver[string, Schedule], Schedule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hub.Watch(job, s.state[job]), s.state[job]
}

// subscriber reads one job until the sender closes. Each holds one receiver,
// because a receiver watches exactly one key.
type subscriber struct {
	name string
	rx   *watch.Receiver[string, Schedule]
	// seen is signalled after every value is applied, so this example can show
	// each transition instead of letting them coalesce. A real consumer needs
	// no such handshake — falling behind is the point of a state bus, and the
	// chan example shows what that looks like.
	seen chan struct{}
}

func (s *scheduler) subscribe(name, job string, wg *sync.WaitGroup) *subscriber {
	rx, seed := s.watchJob(job)
	sub := &subscriber{name: name, rx: rx, seen: make(chan struct{})}

	// The seed is ours already — the bus will not send it back.
	fmt.Printf("%s: starts from %q (seq %d)\n", name, seed.State, seed.Seq)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer rx.Close() // the unwatch: it releases the key's state too
		for {
			ev, err := rx.Recv()
			if err != nil {
				if !errors.Is(err, gobus.ErrClosed) {
					fmt.Printf("%s: recv error: %v\n", name, err)
				}
				fmt.Printf("%s: stream ended\n", name)
				return
			}
			fmt.Printf("%s: %s is now %q (seq %d)\n", name, ev.Key, ev.Value.State, ev.Value.Seq)
			sub.seen <- struct{}{}
		}
	}()
	return sub
}

func main() {
	s := newScheduler()
	// Hard tear-down as a backstop. By the time it fires the soft shutdown
	// below has already ended every stream.
	defer s.hub.Close()

	var wg sync.WaitGroup

	// "early" subscribes before anything has happened, so its baseline is the
	// zero value and every change is news to it.
	early := s.subscribe("early", "build", &wg)

	s.advance("build", "queued")
	<-early.seen
	s.advance("build", "running")
	<-early.seen

	// "late" subscribes now. Its baseline is already "running", so the two
	// subscribers hold different values for the same key — which is why Accept
	// is evaluated per receiver rather than once for the hub.
	late := s.subscribe("late", "build", &wg)

	// A reordered publish, staged by hand. Both changes are committed under
	// the scheduler's lock in order, then published backwards — as concurrent
	// producers do when the older change loses the race for the bus lock.
	stopped := s.commit("build", "stopped")
	done := s.commit("build", "done")
	fmt.Printf("-- publishing seq %d then seq %d --\n", done.Seq, stopped.Seq)
	s.publish("build", done)
	s.publish("build", stopped)
	<-early.seen
	<-late.seen

	// Both ended on "done": the older change was rejected on arrival, for each
	// subscriber, against that subscriber's own current value.
	fmt.Println("-- seq", stopped.Seq, "was rejected: nobody saw \"stopped\" --")

	// A different job. Nobody watches it, so this Send is discarded: there is
	// no receiver and therefore no buffer to hold it.
	s.advance("compact", "queued")

	// Soft shutdown: an unread value is read once more, then Recv reports
	// ErrClosed. hub.Close() would abandon it instead.
	s.tx.Close()

	wg.Wait()
	fmt.Println("done")
}
