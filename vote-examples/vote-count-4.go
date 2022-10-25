package main

import (
	"math/rand"
	"sync"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	count := 0
	finished := 0
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for i := 0; i < 10; i++ {
		// In a real system, this might be RPC calls rather than Goroutines
		go func() {
			vote := requestVote()
			mu.Lock()
			defer mu.Unlock()
			if vote {
				count++
			}
			finished++
			// Notify listeners (see cond.Wait())
			cond.Broadcast()
		}()
	}

	// Required for initial check of the condition
	mu.Lock()

	// Not a spinning lock, because of cond.Wait()
	for count < 5 && finished != 10 {
		// Release lock, BLOCK until notified
		cond.Wait()
		// Before cond.Wait() CONTINUES, it captures the lock to be able to evaluate the condition
	}
	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
	mu.Unlock()
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
