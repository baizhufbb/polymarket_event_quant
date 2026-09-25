package knock

// A moment this close after a slot still counts as that slot. With whole
// nanoseconds there is no rounding noise to absorb; the slack is kept so
// the timetable lands where the Python one did.
const gridTolerance = 1000

// SlotAtOrAfter is the first slot at or after moment on the timetable
// origin + phase + k*interval, all in nanoseconds of one clock.
//
// Fleet members share the origin and differ only in phase, so their sends
// stay that far apart however long each one's warm-up took, and a member
// that misses slots resumes on its own next one rather than starting a
// fresh timetable at "now".
func SlotAtOrAfter(moment, origin, phase, interval int64) int64 {
	offset := moment - origin - phase - gridTolerance
	return origin + phase + ceilDiv(offset, interval)*interval
}

func ceilDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a > 0 {
		q++
	}
	return q
}
