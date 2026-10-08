package knock

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"polymarket_event_quant/knocker/internal/venue"
)

// At most this many asks for the record are out at once, so one slow
// answer does not hold up the next ask.
const previewAsksInFlight = 4

// check refuses a preview the knock could not follow.
func (p *Preview) check() error {
	if p.URL == "" {
		return errors.New("a preview needs the market's record url")
	}
	if !(p.PollMs > 0) {
		return errors.New("preview poll_ms must be above 0")
	}
	until := math.Inf(-1)
	for _, b := range p.Bursts {
		if !(b.IntervalMs > 0) || !(b.FromMs < b.UntilMs) || b.FromMs < until {
			return fmt.Errorf("preview bursts must follow one another, each with an interval above 0: %+v", p.Bursts)
		}
		until = b.UntilMs
	}
	return nil
}

// The fields of a market's record on Gamma that the watch reads.
type record struct {
	Active    bool   `json:"active"`
	StartDate string `json:"startDate"`
}

type answer struct {
	asked, returned time.Time
	status          int
	body            []byte
	err             error
}

// watch asks for the market's record every PollMs until it turns active,
// then hands the timing thread the moment the bursts are timed from. A
// failed ask or an answer it cannot read is counted and the asking goes
// on; the knock's deadline is what ends a record that never turns.
func (r *run) watch() {
	defer func() {
		if p := recover(); p != nil {
			r.fail(fmt.Errorf("preview watch: %v", p))
		}
	}()
	client, err := venue.RecordClient(r.plan.CAFile)
	if err != nil {
		r.fail(fmt.Errorf("preview watch: %w", err))
		return
	}
	answers := make(chan answer, previewAsksInFlight)
	out := 0
	ask := func() {
		if out >= previewAsksInFlight {
			r.notePreview(func(s *PreviewSeen) { s.Skipped++ })
			return
		}
		out++
		r.notePreview(func(s *PreviewSeen) { s.Asks++ })
		asked := time.Now()
		go func() {
			a := answer{asked: asked}
			defer func() {
				if p := recover(); p != nil {
					a.err = fmt.Errorf("ask failed: %v", p)
				}
				a.returned = time.Now()
				// Exactly one answer per ask, and it never blocks: no more
				// answers are owed than the channel holds.
				answers <- a
			}()
			a.status, a.body, a.err = venue.AskRecord(client, r.plan.Preview.URL, asked)
		}()
	}
	ticker := time.NewTicker(time.Duration(r.plan.Preview.PollMs * float64(time.Millisecond)))
	defer ticker.Stop()
	ask()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			ask()
		case a := <-answers:
			out--
			var rec record
			if a.err != nil || a.status != 200 || json.Unmarshal(a.body, &rec) != nil {
				r.notePreview(func(s *PreviewSeen) { s.Failed++ })
				continue
			}
			if !rec.Active {
				continue
			}
			anchor := r.turned(a, rec)
			r.venue.WarmNow()
			r.anchor <- anchor
			return
		}
	}
}

// turned notes the answer that found the record active and returns the
// moment on the monotonic clock that the bursts are timed from: the
// record's startDate, or the moment the answer came back when startDate is
// missing or later than that - the bursts never wait for a date this
// machine's clock has not reached.
func (r *run) turned(a answer, rec record) int64 {
	seen := a.returned.UnixNano()
	anchor := seen
	startMs := 0.0
	if start, err := time.Parse(time.RFC3339Nano, rec.StartDate); err == nil {
		startMs = float64(start.UnixNano()) / 1e6
		anchor = min(start.UnixNano(), seen)
	}
	r.notePreview(func(s *PreviewSeen) {
		s.StartDateMs = startMs
		s.AskedMs = a.asked.UnixMilli()
		s.SeenMs = a.returned.UnixMilli()
		s.AnchorMs = float64(anchor) / 1e6
	})
	return now() + (anchor - time.Now().UnixNano())
}

// timeBursts lays each member's bursts out from anchor, a moment on the
// monotonic clock. Within each burst the members keep their cadence order
// and spread evenly across its interval; the cadence carries on from the
// end of the last burst.
func (r *run) timeBursts(tables []timetable, anchor int64) {
	bursts := r.plan.Preview.Bursts
	for i, m := range r.members {
		share := math.Mod(float64(m.phase)/float64(r.interval), 1)
		t := &tables[i]
		t.bursts = nil
		for _, b := range bursts {
			interval := int64(math.Round(b.IntervalMs * 1e6))
			t.bursts = append(t.bursts, stretch{
				from:     anchor + int64(math.Round(b.FromMs*1e6)),
				until:    anchor + int64(math.Round(b.UntilMs*1e6)),
				phase:    int64(math.Round(share * float64(interval))),
				interval: interval,
			})
		}
		t.origin = anchor
		if n := len(bursts); n > 0 {
			t.origin = anchor + int64(math.Round(bursts[n-1].UntilMs*1e6))
		}
	}
}

func (r *run) notePreview(change func(*PreviewSeen)) {
	r.seenMu.Lock()
	change(r.seen)
	r.seenMu.Unlock()
}

// fail hands the coordinator a failure that ends the knock for everyone.
func (r *run) fail(err error) {
	select {
	case r.failed <- err:
	case <-r.done:
	}
}
