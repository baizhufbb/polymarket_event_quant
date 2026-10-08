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
	first, ok := w.ns()
	for range 1000 {
		runtime.Gosched()
	}
	then, ok2 := w.ns()
	if !ok || !ok2 || first <= 0 || then < first {
		t.Fatalf("run-queue wait %d (%v) then %d (%v)", first, ok, then, ok2)
	}
	if allocs := testing.AllocsPerRun(100, func() { w.ns() }); allocs != 0 {
		t.Errorf("reading the wait allocates %.0f times", allocs)
	}
}
