//go:build linux

package knock

import (
	"runtime"
	"syscall"
	"unsafe"
)

const (
	clockMonotonic  = 1
	timerAbstime    = 1
	prSetTimerslack = 29
)

// now reads CLOCK_MONOTONIC, the clock the timing thread sleeps on.
func now() int64 {
	var ts syscall.Timespec
	syscall.Syscall(syscall.SYS_CLOCK_GETTIME, clockMonotonic, uintptr(unsafe.Pointer(&ts)), 0)
	return ts.Nano()
}

// sleepUntil sleeps the calling thread until the monotonic instant t.
// Sleeping to an absolute time makes a wake-up by a signal harmless: the
// retry aims at the same instant.
func sleepUntil(t int64) {
	ts := syscall.NsecToTimespec(t)
	for {
		_, _, errno := syscall.Syscall6(
			syscall.SYS_CLOCK_NANOSLEEP,
			clockMonotonic,
			timerAbstime,
			uintptr(unsafe.Pointer(&ts)),
			0, 0, 0,
		)
		if errno != syscall.EINTR {
			return
		}
	}
}

// pinTimingThread keeps the calling goroutine on its own OS thread for
// good (the thread ends with it) and cuts that thread's timer slack from
// the default 50 us to the minimum, so it wakes when asked. Go's own timers
// round a wait to the millisecond.
func pinTimingThread() {
	runtime.LockOSThread()
	syscall.Syscall(syscall.SYS_PRCTL, prSetTimerslack, 1, 0)
}
