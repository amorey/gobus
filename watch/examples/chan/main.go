// watch/examples/chan demonstrates the Chan()-based API for a keyed state bus,
// with a subscriber composing Chan() with a cancel signal via select for
// graceful early shutdown.
//
// Chan() on a watch receiver is a PRIVATE channel fed by a per-receiver
// goroutine. Receiver.Close() shuts that feeder down (and closes Chan());
// Sender.Close() also closes Chan() once the feeder has handed over the final
// value. Always Close the receiver when you stop reading or the feeder leaks.
//
// The channel is unbuffered on purpose, and a watch receiver holds one slot
// rather than a queue — so a consumer that falls behind does not accumulate a
// backlog. It skips to the current value. The producer below publishes far
// faster than the consumers apply, and the printed percentages jump forward:
// the intermediate ones were superseded in the slot before anybody read them.
//
// Note what the channel does *not* promise. Once the feeder has committed to a
// delivery, a newer value arriving makes both arms of its internal select
// ready, and Go picks between ready arms at random — so an occasional
// superseded value is still delivered, with the newer one immediately behind
// it. Values arrive in order, and a consumer that keeps reading converges on
// the current value; that is the guarantee.
//
// A receiver watches exactly one key, so a consumer interested in two jobs
// holds two receivers and two feeders. That is why this package suits narrow
// subscriptions and conflate suits wide ones.
//
// The two subscribers shut down differently: one exits early on a cancel
// signal, the other runs until Sender.Close hands over the final value and
// closes its channel.
//
// Run:
//
//	go run ./watch/examples/chan
package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/amorey/gobus/watch"
)

// Progress is a job's completion percentage. Seq is assigned under the
// builder's lock, so it orders changes by when they became true rather than
// by when they reached the bus.
type Progress struct {
	Percent int
	Seq     uint64
}

type builder struct {
	hub *watch.Hub[string, Progress]
	tx  *watch.Sender[string, Progress]

	mu    sync.Mutex
	seq   uint64
	state map[string]Progress
}

func newBuilder() *builder {
	hub := watch.New[string](watch.WithAccept(func(prev, next Progress) bool {
		return next.Seq > prev.Seq
	}))
	return &builder{hub: hub, tx: hub.Sender(), state: map[string]Progress{}}
}

// advance commits a change under the builder's lock, then publishes after
// releasing it — so the bus lock is never nested inside the builder's.
func (b *builder) advance(job string, pct int) {
	b.mu.Lock()
	b.seq++
	v := Progress{Percent: pct, Seq: b.seq}
	b.state[job] = v
	b.mu.Unlock()
	_ = b.tx.Send(job, v)
}

// watchJob reads the job's current progress and registers in one critical
// section. Watch calls no caller code, so holding the builder's lock across it
// is safe — and it is what closes the window between the read and the watch.
func (b *builder) watchJob(job string) (*watch.Receiver[string, Progress], Progress) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hub.Watch(job, b.state[job]), b.state[job]
}

func main() {
	b := newBuilder()
	// Hard tear-down as a backstop; the soft shutdown below has already ended
	// both streams by the time it fires.
	defer b.hub.Close()

	// Only "test" is cancellable. "build" passes a nil channel, which is never
	// ready in a select — so one subscriber exits early on the signal and the
	// other runs to the sender's close, showing both shutdown paths in one run.
	cancel := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(cancel)
	}()

	var wg sync.WaitGroup
	for _, job := range []string{"build", "test"} {
		var stop <-chan struct{}
		if job == "test" {
			stop = cancel
		}
		rx, seed := b.watchJob(job)
		// Call Chan() here, not inside the goroutine, so the feeder is running
		// before the producer starts.
		ch := rx.Chan()
		fmt.Printf("%s: starts from %d%% (seq %d)\n", job, seed.Percent, seed.Seq)

		wg.Add(1)
		go func(job string, stop <-chan struct{}) {
			defer wg.Done()
			defer rx.Close() // stops this receiver's feeder and unwatches the key

			for {
				select {
				case ev, ok := <-ch:
					if !ok {
						// Sender closed and the final value was handed over.
						fmt.Printf("%s: stream ended\n", job)
						return
					}
					// Slow work. Values published for this job during the
					// sleep replace each other in its slot rather than
					// queueing, so the next read jumps to the current one.
					time.Sleep(20 * time.Millisecond)
					fmt.Printf("%s: %d%% (seq %d)\n", job, ev.Value.Percent, ev.Value.Seq)
				case <-stop:
					// Graceful early exit. The deferred rx.Close shuts the
					// feeder down; abandoning ch without it would leak.
					fmt.Printf("%s: cancelled\n", job)
					return
				}
			}
		}(job, stop)
	}

	// A producer faster than the consumers. Send never blocks, so it never
	// waits on a slow consumer, and no backlog builds up behind one — the
	// percentages the consumers print jump forward past everything that was
	// superseded in the slot before they read it.
	for pct := 1; pct <= 100; pct++ {
		b.advance("build", pct)
		b.advance("test", pct)
		time.Sleep(2 * time.Millisecond)
	}

	// Nobody watches this job, so the Send is discarded rather than retained.
	b.advance("docs", 100)

	// Soft shutdown: a consumer still holding an unread value gets it before
	// its channel closes. hub.Close() would close the channels immediately and
	// abandon it.
	b.tx.Close()

	wg.Wait()
	fmt.Println("done")
}
