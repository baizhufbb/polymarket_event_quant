package knock

import "sync"

// While a burst is on, the replies wait to be handed to Python: writing
// each one down takes the server's one processor from the burst's sends.
// In run57, at four sends a millisecond, the replies coming back held the
// sends up for 10..20 ms at a time once the first ones returned.
var (
	quietMu  sync.Mutex
	bursting int
	quiet    = closedChannel()
)

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
