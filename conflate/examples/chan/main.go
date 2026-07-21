// conflate/examples/chan demonstrates the Chan()-based API for a keyed
// latest-value bus, with the subscriber composing Chan() with a cancel signal
// via select for graceful early shutdown.
//
// Chan() on a conflate receiver is a PRIVATE channel fed by a per-receiver
// goroutine. Receiver.Close() shuts that feeder down (and closes Chan());
// Sender.Close() also closes Chan() after the feeder drains the receiver's
// pending per-key state. Always Close the receiver when you stop reading or
// the feeder will leak.
//
// Note that the channel is unbuffered on purpose: coalescing keeps happening
// in the receiver's per-key slots while the consumer is busy, so a fast
// publisher produces no backlog beyond the live key set.
//
// Run:
//
//	go run ./conflate/examples/chan
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/amorey/gobus/conflate"
)

type Update struct {
	Phase string // "Added", "Modified", "Deleted"
	Rev   int
}

// merge is the coalescing policy: the newer revision supersedes the older,
// except that an Added the consumer never saw followed by a Deleted
// annihilates the key.
func merge(prev, next Update) (Update, bool) {
	if prev.Phase == "Added" && next.Phase == "Deleted" {
		return Update{}, false
	}
	return next, true
}

func main() {
	hub := conflate.New[string](merge)
	// hub.Close() is idempotent close-all and hard tear-down (skips the drain
	// that tx.Close gives subscribers). Deferring is still safe as a backstop
	// because by the time it fires the soft shutdown below has completed.
	defer hub.Close()

	cancel := make(chan struct{})
	go func() {
		time.Sleep(200 * time.Millisecond)
		close(cancel)
	}()

	// Two subscribers, each with its own feeder goroutine. This one watches
	// every pod; the second uses the WithKeyFilter option to subscribe to one key,
	// which means unwanted keys are dropped at Send time and never occupy a
	// slot in its buffer at all.
	var wg sync.WaitGroup
	watchAll := hub.Receiver()
	watchOne := hub.Receiver(
		hub.WithKeyFilter(func(pod string) bool { return pod == "db-0" }),
	)

	for id, rx := range []*conflate.Receiver[string, Update]{watchAll, watchOne} {
		// Call Chan() here (not inside the goroutine) so the feeder is running
		// before the producer loop starts.
		ch := rx.Chan()
		wg.Add(1)
		go func(id int, rx *conflate.Receiver[string, Update]) {
			defer wg.Done()
			defer rx.Close() // stops the per-receiver feeder goroutine
			for {
				select {
				case ev, ok := <-ch:
					if !ok {
						// Sender closed and the feeder drained: done.
						return
					}
					// Simulate slow work. Updates arriving for this pod during
					// the sleep coalesce into its slot rather than queueing up.
					time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
					fmt.Printf("sub %d apply: pod=%s phase=%s rev=%d\n",
						id, ev.Key, ev.Value.Phase, ev.Value.Rev)
				case <-cancel:
					// Graceful early exit. Deferred rx.Close shuts the feeder.
					return
				}
			}
		}(id, rx)
	}

	tx := hub.Sender()
	pods := []string{"web-0", "web-1", "db-0"}
	for rev := 1; rev <= 30; rev++ {
		if err := tx.Send(pods[rev%len(pods)], Update{Phase: "Modified", Rev: rev}); err != nil {
			fmt.Println("send failed:", err)
			break
		}
	}

	// Soft shutdown: subscribers drain their pending per-pod state via Chan()
	// before the channel closes.
	tx.Close()

	wg.Wait()
	fmt.Println("done")
}
