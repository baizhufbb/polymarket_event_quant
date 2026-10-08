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

// stretch is a run of slots: from + phase + k*interval, before until.
type stretch struct{ from, until, phase, interval int64 }

// timetable is one member's slots, in nanoseconds of the monotonic clock:
// the bursts after a preview, if any, then the cadence for good.
type timetable struct {
	bursts []stretch
	// The cadence: origin + phase + k*interval.
	origin, phase, interval int64
}

// at is the member's first slot at or after moment.
func (t timetable) at(moment int64) int64 {
	for _, b := range t.bursts {
		if moment >= b.until {
			continue
		}
		if slot := SlotAtOrAfter(max(moment, b.from), b.from, b.phase, b.interval); slot < b.until {
			return slot
		}
	}
	return SlotAtOrAfter(max(moment, t.origin), t.origin, t.phase, t.interval)
}

// after is the slot that follows the one at slot.
func (t timetable) after(slot int64) int64 {
	return t.at(slot + gridTolerance + 1)
}

func ceilDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a > 0 {
		q++
	}
	return q
}
