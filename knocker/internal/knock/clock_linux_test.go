//go:build linux

package knock

import (
	"runtime"
	"testing"
)

func TestTheTimingThreadCanReadItsOwnRunQueueWait(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	w := openThreadWait()
	defer w.close()
	first := w.ns()
	for range 1000 {
		runtime.Gosched()
	}
	if first <= 0 || w.ns() < first {
		t.Fatalf("run-queue wait %d then %d", first, w.ns())
	}
}
