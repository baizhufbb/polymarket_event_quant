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

// threadWait reads how long the calling thread has waited in the kernel's
// run queue for a processor (/proc/thread-self/schedstat, second field),
// so the trace can tell a slot the processor was busy for from one the Go
// scheduler kept waiting. Opened on the timing thread itself.
//
// It is read on every slot, so it allocates nothing: once a held-off
// collection resumes, any allocation on the timing thread would be made to
// do marking work first.
type threadWait struct{ fd int }

func openThreadWait() threadWait {
	fd, err := syscall.Open("/proc/thread-self/schedstat", syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return threadWait{fd: -1}
	}
	return threadWait{fd: fd}
}

// ns is the thread's total run-queue wait so far; ok is false when it
// could not be read.
func (w threadWait) ns() (waited int64, ok bool) {
	if w.fd < 0 {
		return 0, false
	}
	var buf [96]byte
	n, err := syscall.Pread(w.fd, buf[:], 0)
	if err != nil || n <= 0 {
		return 0, false
	}
	i := 0
	for i < n && buf[i] != ' ' {
		i++
	}
	for i < n && buf[i] == ' ' {
		i++
	}
	digits := i
	for i < n && buf[i] >= '0' && buf[i] <= '9' {
		waited = waited*10 + int64(buf[i]-'0')
		i++
	}
	return waited, i > digits
}

func (w threadWait) close() {
	if w.fd >= 0 {
		syscall.Close(w.fd)
	}
}
