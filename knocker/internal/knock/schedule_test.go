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

func TestATimetableRunsItsBurstsThenTheCadence(t *testing.T) {
	const ms = 1_000_000
	table := timetable{
		bursts: []stretch{
			{from: 100 * ms, until: 110 * ms, phase: 1 * ms, interval: 3 * ms},
			{from: 110 * ms, until: 130 * ms, phase: 5 * ms, interval: 10 * ms},
		},
		origin: 130 * ms, phase: 2 * ms, interval: 25 * ms,
	}
	var got []int64
	for slot := table.at(0); len(got) < 8; slot = table.after(slot) {
		got = append(got, slot/ms)
	}
	want := []int64{101, 104, 107, 115, 125, 132, 157, 182}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slots %v, want %v", got, want)
		}
	}
	// A moment inside a burst lands on its next slot; one past every burst
	// on the cadence.
	if s := table.at(105 * ms); s != 107*ms {
		t.Errorf("at 105 ms: %d", s/ms)
	}
	if s := table.at(126 * ms); s != 132*ms {
		t.Errorf("at 126 ms: %d", s/ms)
	}
	// Without bursts it is the plain cadence.
	plain := timetable{origin: 0, phase: 5 * ms, interval: 25 * ms}
	if s := plain.at(31 * ms); s != SlotAtOrAfter(31*ms, 0, 5*ms, 25*ms) {
		t.Errorf("plain cadence at 31 ms: %d", s/ms)
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
