package knock

import (
	"encoding/json"
	"os"
	"testing"
)

// slots.json holds the Python timetable's answers (submission_slot).
func TestTheTimetableLandsWhereThePythonOneDid(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/slots.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Moment   int64 `json:"moment_ns"`
		Origin   int64 `json:"origin_ns"`
		Phase    int64 `json:"phase_ns"`
		Interval int64 `json:"interval_ns"`
		Slot     int64 `json:"slot_ns"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if got := SlotAtOrAfter(c.Moment, c.Origin, c.Phase, c.Interval); got != c.Slot {
			t.Errorf("slot at or after %d: %d, want %d", c.Moment, got, c.Slot)
		}
	}
}

func TestAdvancingTheTimetableNeverSkipsASlot(t *testing.T) {
	const interval = 25_000_000
	origin := int64(1_234_567_000_000_000)
	slot := SlotAtOrAfter(origin, origin, 5_000_000, interval)
	for range 10_000 {
		next := SlotAtOrAfter(slot+interval, origin, 5_000_000, interval)
		if next != slot+interval {
			t.Fatalf("after %d came %d", slot, next)
		}
		slot = next
	}
}
