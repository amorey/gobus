// conflate/examples/recv demonstrates the Recv()-based API for a keyed
// latest-value bus — the classic "resource watch" pattern.
//
// A producer streams status updates for a set of pods far faster than the
// subscribers can apply them. Because conflate keeps one slot per key, a
// subscriber that falls behind does not accumulate a backlog: repeated
// updates for the same pod coalesce into that pod's slot, and the subscriber
// catches up to the *current* state of every pod, in first-touch order.
//
// The merge function also annihilates: a pod that is added and then deleted
// before the subscriber ever observed it disappears entirely, so consumers
// never see a phantom resource.
//
// Graceful shutdown uses Sender.Close (the soft path): each receiver drains
// its pending updates once before Recv returns ErrClosed. Hub.Close is hard
// tear-down and skips that drain.
//
// Run:
//
//	go run ./conflate/examples/recv
package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/conflate"
)

type Update struct {
	Phase string // "Added", "Modified", "Deleted"
	Rev   int
}

// merge is the coalescing policy: the newer revision supersedes the older,
// except that an Added the consumer never saw followed by a Deleted
// annihilates the key — there is no point telling a consumer about a
// resource that came and went behind its back.
func merge(prev, next Update) (Update, bool) {
	if prev.Phase == "Added" && next.Phase == "Deleted" {
		return Update{}, false
	}
	return next, true
}

func main() {
	hub := conflate.New[string](conflate.WithDefaultMerge(merge))
	// hub.Close() is idempotent close-all and hard tear-down (skips the drain
	// that tx.Close gives subscribers). Deferring is still safe as a backstop
	// because by the time it fires the soft shutdown below has completed.
	defer hub.Close()

	// Subscribers: each gets its own Receiver with its own per-key slots, so
	// a slow subscriber coalesces more aggressively than a fast one without
	// affecting anybody else.
	const subscribers = 2
	var wg sync.WaitGroup
	for id := 0; id < subscribers; id++ {
		rx := hub.Receiver()
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer rx.Close()
			for {
				ev, err := rx.Recv()
				if err != nil {
					if !errors.Is(err, gobus.ErrClosed) {
						fmt.Printf("sub %d recv error: %v\n", id, err)
					}
					return
				}
				// Simulate slow work. Updates arriving for this pod during the
				// sleep coalesce into its slot rather than queueing up.
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
				fmt.Printf("sub %d apply: pod=%s phase=%s rev=%d\n",
					id, ev.Key, ev.Value.Phase, ev.Value.Rev)
			}
		}(id)
	}

	// Producer: a burst of updates across a small pod set. Send never blocks,
	// so a fast producer never waits on a slow subscriber.
	tx := hub.Sender()
	pods := []string{"web-0", "web-1", "db-0"}
	for rev := 1; rev <= 30; rev++ {
		pod := pods[rev%len(pods)]
		if err := tx.Send(pod, Update{Phase: "Modified", Rev: rev}); err != nil {
			// ErrClosed here means the bus was torn down — stop producing.
			fmt.Println("send failed:", err)
			break
		}
	}

	// A pod that appears and disappears within one burst: the merge function
	// annihilates the pair, so no subscriber ever hears about "ephemeral-0".
	_ = tx.Send("ephemeral-0", Update{Phase: "Added", Rev: 31})
	_ = tx.Send("ephemeral-0", Update{Phase: "Deleted", Rev: 32})

	// Soft shutdown: each subscriber drains its pending per-pod state once
	// before its next Recv returns ErrClosed. For hard tear-down (pending
	// state abandoned) we'd call hub.Close() instead.
	tx.Close()

	wg.Wait()
	fmt.Println("done")
}
