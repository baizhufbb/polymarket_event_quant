//go:build !linux

package knock

import (
	"runtime"
	"time"
)

// Off Linux the library only runs under local tests, where Go's own
// timers are precise enough.
var epoch = time.Now()

func now() int64 { return int64(time.Since(epoch)) }

func sleepUntil(t int64) {
	if wait := time.Duration(t - now()); wait > 0 {
		time.Sleep(wait)
	}
}

func pinTimingThread() { runtime.LockOSThread() }
