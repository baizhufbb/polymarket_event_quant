//go:build linux

package knock

import (
	"os"
	"runtime"
	"strconv"
	"strings"
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

// threadWait reads how long the calling thread has waited in the kernel's
// run queue for a processor (/proc/thread-self/schedstat, second field),
// so the trace can tell a slot the processor was busy for from one the Go
// scheduler kept waiting. Opened on the timing thread itself.
type threadWait struct{ f *os.File }

func openThreadWait() threadWait {
	f, err := os.Open("/proc/thread-self/schedstat")
	if err != nil {
		return threadWait{}
	}
	return threadWait{f}
}

// ns is the thread's total run-queue wait so far, or 0 when unknown.
func (w threadWait) ns() int64 {
	if w.f == nil {
		return 0
	}
	var buf [96]byte
	n, _ := w.f.ReadAt(buf[:], 0)
	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		return 0
	}
	v, _ := strconv.ParseInt(fields[1], 10, 64)
	return v
}

func (w threadWait) close() {
	if w.f != nil {
		w.f.Close()
	}
}
