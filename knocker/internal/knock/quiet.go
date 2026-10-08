package knock

import (
	"runtime/debug"
	"sync"
)

// While a burst is on, the replies wait to be handed to Python: writing
// each one down takes the server's one processor from the burst's sends.
// In run57, at four sends a millisecond, the replies coming back held the
// sends up for 10..20 ms at a time once the first ones returned.
var (
	quietMu  sync.Mutex
	bursting int
	quiet    = closedChannel()
)

// Go's garbage collection is held off for the same window: the burst's
// own allocations set it off, and on the one processor a collection holds
// the timing thread up for as long as it marks (run59: a 14.5 ms one woke
// it up to 25 ms late). The burst's garbage is collected once it is over.
var (
	gcMu    sync.Mutex
	gcHeld  int
	gcSaved int
)

// holdGC stops collections, first waiting out one in progress; call it
// before the burst, not in it.
func holdGC() {
	gcMu.Lock()
	defer gcMu.Unlock()
	if gcHeld == 0 {
		gcSaved = debug.SetGCPercent(-1)
	}
	gcHeld++
}

func releaseGC() {
	gcMu.Lock()
	defer gcMu.Unlock()
	if gcHeld == 0 {
		return
	}
	gcHeld--
	if gcHeld == 0 {
		debug.SetGCPercent(gcSaved)
	}
}

func closedChannel() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

// Quiet is closed while no burst is on.
func Quiet() <-chan struct{} {
	quietMu.Lock()
	defer quietMu.Unlock()
	return quiet
}

func burstOn() {
	quietMu.Lock()
	defer quietMu.Unlock()
	if bursting == 0 {
		quiet = make(chan struct{})
	}
	bursting++
}

func burstOff() {
	quietMu.Lock()
	defer quietMu.Unlock()
	if bursting == 0 {
		return
	}
	bursting--
	if bursting == 0 {
		close(quiet)
	}
}
