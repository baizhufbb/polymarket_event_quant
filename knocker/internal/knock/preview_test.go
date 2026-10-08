package knock_test

import (
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"polymarket_event_quant/knocker/internal/fakevenue"
	"polymarket_event_quant/knocker/internal/knock"
)

func previewPlan(fake *fakevenue.Venue, interval, knockFor time.Duration, bursts []knock.Burst, members ...knock.Member) knock.Plan {
	p := plan(fake, interval, knockFor, members...)
	p.Preview = &knock.Preview{URL: fake.URL + fakevenue.RecordPath, PollMs: 10, Bursts: bursts}
	return p
}

// sinceMs is each arrival in ms after t.
func sinceMs(requests []fakevenue.Request, t time.Time) []float64 {
	out := []float64{}
	for _, r := range requests {
		out = append(out, float64(r.Arrived.Sub(t))/float64(time.Millisecond))
	}
	return out
}

func gaps(times []float64) []float64 {
	out := []float64{}
	for i := 1; i < len(times); i++ {
		out = append(out, times[i]-times[i-1])
	}
	sort.Float64s(out)
	return out
}

func TestNothingIsSentUntilTheRecordTurnsThenTheBurstsRunFromItsStartDate(t *testing.T) {
	fake := start(t)
	turns := time.Now().Add(150 * time.Millisecond)
	// Gamma shows the record a little after the startDate it carries.
	startDate := turns.Add(-50 * time.Millisecond)
	fake.ActivateAt(turns, startDate)
	fake.OpenAt(startDate.Add(300 * time.Millisecond))
	a, b := member(t, "a", 0, "up"), member(t, "b", 12.5, "up")
	bursts := []knock.Burst{{FromMs: 200, UntilMs: 260, IntervalMs: 6}, {FromMs: 260, UntilMs: 400, IntervalMs: 20}}
	result := run(t, previewPlan(fake, 25*time.Millisecond, 5*time.Second, bursts, a, b), &trace{})

	for i, got := range result.Members {
		if len(got.Accepted) != 1 || got.GaveUp {
			t.Fatalf("member %d: %+v", i, got)
		}
	}
	seen := result.Preview
	if seen == nil {
		t.Fatal("no preview in the result")
	}
	want := float64(startDate.UnixMicro()) / 1e3
	if math.Abs(seen.StartDateMs-want) > 0.001 || seen.AnchorMs != seen.StartDateMs {
		t.Errorf("startDate %.3f anchor %.3f, want both %.3f", seen.StartDateMs, seen.AnchorMs, want)
	}
	if seen.SeenMs < turns.UnixMilli() || seen.AskedMs > seen.SeenMs || seen.Asks == 0 || seen.Failed != 0 {
		t.Errorf("preview %+v (turned at %d)", seen, turns.UnixMilli())
	}

	all := sinceMs(fake.Requests(), startDate)
	if len(all) == 0 || all[0] < 195 {
		t.Fatalf("orders arrived %v ms after startDate; the first burst starts at 200", all)
	}
	early := sinceMs(requestsOf(fake, a.Account), startDate)
	inFirst := []float64{}
	for _, ms := range early {
		if ms < 259 {
			inFirst = append(inFirst, ms)
		}
	}
	if len(inFirst) < 8 || len(inFirst) > 11 {
		t.Errorf("member a sent %d times in the first burst, want 10: %v", len(inFirst), early)
	}
	if g := gaps(inFirst); len(g) > 2 && (g[len(g)/2] < 4 || g[len(g)/2] > 9) {
		t.Errorf("member a sent every %.1f ms (median) in the first burst, want 6: %v", g[len(g)/2], inFirst)
	}
	// b sits half an interval after a in each burst.
	late := sinceMs(requestsOf(fake, b.Account), startDate)
	if len(late) == 0 || late[0] < 201 || late[0] > 212 {
		t.Errorf("member b first sent %v ms after startDate, want about 203", late)
	}
}

func TestARecordFoundLateSkipsTheBurstsAlreadyGone(t *testing.T) {
	fake := start(t)
	// The record turned long ago; the bursts are all in the past.
	startDate := time.Now().Add(-5 * time.Second)
	fake.ActivateAt(time.Now(), startDate)
	fake.OpenAt(time.Now().Add(100 * time.Millisecond))
	m := member(t, "a", 0, "up")
	bursts := []knock.Burst{{FromMs: 200, UntilMs: 400, IntervalMs: 2}}
	began := time.Now()
	result := run(t, previewPlan(fake, 25*time.Millisecond, 5*time.Second, bursts, m), &trace{})
	if got := result.Members[0]; len(got.Accepted) != 1 {
		t.Fatalf("%+v", got)
	}
	sent := sinceMs(requestsOf(fake, m.Account), began)
	if len(sent) == 0 || sent[0] > 100 {
		t.Fatalf("first send %v ms after the start; the cadence should have begun at once", sent)
	}
	if g := gaps(sent); len(g) > 2 && g[len(g)/2] < 15 {
		t.Errorf("sent every %.1f ms (median), want the 25 ms cadence: %v", g[len(g)/2], sent)
	}
}

func TestARecordThatNeverTurnsSendsNothingAndGivesUp(t *testing.T) {
	fake := start(t)
	m := member(t, "a", 0, "up")
	bursts := []knock.Burst{{FromMs: 200, UntilMs: 300, IntervalMs: 5}}
	result := run(t, previewPlan(fake, 25*time.Millisecond, 300*time.Millisecond, bursts, m), &trace{})
	got := result.Members[0]
	if !got.GaveUp || got.Attempts != 0 || len(fake.Requests()) != 0 {
		t.Fatalf("%+v, %d orders reached the venue", got, len(fake.Requests()))
	}
	if errors := texts(got.Errors); len(errors) != 1 || errors[0] != knock.KnockBudgetError {
		t.Errorf("errors %v", errors)
	}
	if seen := result.Preview; seen == nil || seen.StartDateMs != 0 || seen.Asks < 10 {
		t.Errorf("preview %+v: asked every 10 ms for 300 ms", seen)
	}
}

func TestAsksSkipTheCacheAndFailuresAreCountedNotFatal(t *testing.T) {
	fake := start(t)
	startDate := time.Now()
	fake.ActivateAt(startDate, startDate)
	fake.OpenAt(startDate)
	fake.RespondRecord = func(n int, _ fakevenue.Request) (fakevenue.Response, bool) {
		switch n {
		case 0:
			return fakevenue.Response{Status: 503, Body: "busy"}, true
		case 1:
			return fakevenue.Response{Status: 200, Body: "<html>"}, true
		}
		return fakevenue.Response{}, false
	}
	m := member(t, "a", 0, "up")
	bursts := []knock.Burst{{FromMs: 50, UntilMs: 100, IntervalMs: 5}}
	result := run(t, previewPlan(fake, 25*time.Millisecond, 5*time.Second, bursts, m), &trace{})
	if got := result.Members[0]; len(got.Accepted) != 1 {
		t.Fatalf("%+v", got)
	}
	if seen := result.Preview; seen.Failed != 2 || seen.StartDateMs == 0 {
		t.Errorf("preview %+v: two failed asks, then the record", seen)
	}
	asks := fake.Asks()
	stamps := map[string]bool{}
	for _, ask := range asks {
		if !strings.HasPrefix(ask.Body, "_cb=") || ask.Header.Get("Cache-Control") != "no-cache" {
			t.Errorf("ask %q %v does not skip the cache", ask.Body, ask.Header)
		}
		stamps[ask.Body] = true
	}
	if len(stamps) != len(asks) {
		t.Errorf("asks repeat a cache-busting stamp: %d stamps for %d asks", len(stamps), len(asks))
	}
}

func TestAStartDateAheadOfOurClockIsTimedFromWhenTheRecordWasSeen(t *testing.T) {
	fake := start(t)
	turns := time.Now().Add(50 * time.Millisecond)
	fake.ActivateAt(turns, turns.Add(time.Hour))
	fake.OpenAt(turns)
	m := member(t, "a", 0, "up")
	bursts := []knock.Burst{{FromMs: 100, UntilMs: 200, IntervalMs: 5}}
	result := run(t, previewPlan(fake, 25*time.Millisecond, 5*time.Second, bursts, m), &trace{})
	if got := result.Members[0]; len(got.Accepted) != 1 {
		t.Fatalf("%+v", got)
	}
	seen := result.Preview
	if math.Abs(seen.AnchorMs-float64(seen.SeenMs)) >= 1 {
		t.Errorf("anchored at %.3f, seen at %d", seen.AnchorMs, seen.SeenMs)
	}
	sent := sinceMs(requestsOf(fake, m.Account), time.UnixMilli(seen.SeenMs))
	if len(sent) == 0 || sent[0] < 95 || sent[0] > 140 {
		t.Errorf("first send %v ms after the record was seen, want about 100", sent)
	}
}

func TestStopAllWhileWaitingForTheRecordEndsTheKnock(t *testing.T) {
	fake := start(t)
	m := member(t, "a", 0, "up")
	bursts := []knock.Burst{{FromMs: 200, UntilMs: 300, IntervalMs: 5}}
	go func() {
		time.Sleep(200 * time.Millisecond)
		knock.StopAll()
	}()
	began := time.Now()
	result := run(t, previewPlan(fake, 25*time.Millisecond, 10*time.Second, bursts, m), &trace{})
	if took := time.Since(began); took > 1500*time.Millisecond {
		t.Errorf("returned %v after the start, stop came at 200 ms", took)
	}
	got := result.Members[0]
	if got.Attempts != 0 || got.GaveUp {
		t.Fatalf("%+v", got)
	}
	if errors := texts(got.Errors); len(errors) != 1 || errors[0] != knock.StoppedError {
		t.Errorf("errors %v", errors)
	}
}

func TestAPreviewItCannotFollowIsRefused(t *testing.T) {
	fake := start(t)
	m := member(t, "a", 0, "up")
	for _, bursts := range [][]knock.Burst{
		{{FromMs: 300, UntilMs: 200, IntervalMs: 5}},
		{{FromMs: 200, UntilMs: 300, IntervalMs: 0}},
		{{FromMs: 200, UntilMs: 300, IntervalMs: 5}, {FromMs: 250, UntilMs: 400, IntervalMs: 5}},
	} {
		if _, err := knock.Run(previewPlan(fake, 25*time.Millisecond, time.Second, bursts, m), func(knock.Attempt) {}); err == nil {
			t.Errorf("bursts %+v were taken", bursts)
		}
	}
	p := previewPlan(fake, 25*time.Millisecond, time.Second, nil, m)
	p.Preview.PollMs = 0
	if _, err := knock.Run(p, func(knock.Attempt) {}); err == nil {
		t.Error("a preview asked for every 0 ms was taken")
	}
	if len(fake.Requests())+len(fake.Asks()) != 0 {
		t.Error("a refused plan reached the venue")
	}
}

func isClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

func TestRepliesAreHeldFromPythonWhileABurstIsOn(t *testing.T) {
	fake := start(t)
	startDate := time.Now().Add(100 * time.Millisecond)
	fake.ActivateAt(startDate, startDate)
	// The door stays shut: every send is "not ready" and the knock gives up.
	fake.OpenAt(startDate.Add(time.Hour))
	m := member(t, "a", 0, "up")
	bursts := []knock.Burst{{FromMs: 100, UntilMs: 200, IntervalMs: 5}}
	type sample struct {
		ms    float64
		quiet bool
	}
	var mu sync.Mutex
	var samples []sample
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			s := sample{float64(time.Since(startDate)) / float64(time.Millisecond), isClosed(knock.Quiet())}
			mu.Lock()
			samples = append(samples, s)
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()
	run(t, previewPlan(fake, 25*time.Millisecond, 1500*time.Millisecond, bursts, m), &trace{})
	close(stop)
	<-sampled
	if !isClosed(knock.Quiet()) {
		t.Fatal("the trace is still held after the knock returned")
	}
	for _, s := range samples {
		switch {
		case s.ms < -10 && !s.quiet:
			t.Fatalf("held %.0f ms before the record turned", -s.ms)
		case s.ms > 120 && s.ms < 290 && s.quiet:
			t.Fatalf("not held %.0f ms after startDate, inside the burst and its replies", s.ms)
		case s.ms > 340 && !s.quiet:
			t.Fatalf("still held %.0f ms after startDate; the replies were in by 300", s.ms)
		}
	}
}

func TestTheTraceIsReleasedWhenTheDoorOpensInABurst(t *testing.T) {
	fake := start(t)
	startDate := time.Now()
	fake.ActivateAt(startDate, startDate)
	fake.OpenAt(startDate.Add(120 * time.Millisecond))
	m := member(t, "a", 0, "up")
	bursts := []knock.Burst{{FromMs: 100, UntilMs: 5000, IntervalMs: 5}}
	got := run(t, previewPlan(fake, 25*time.Millisecond, 10*time.Second, bursts, m), &trace{}).Members[0]
	if len(got.Accepted) != 1 {
		t.Fatalf("%+v", got)
	}
	// The timing thread lets go once it sees the knock end, a moment after
	// it returns.
	deadline := time.Now().Add(200 * time.Millisecond)
	for !isClosed(knock.Quiet()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !isClosed(knock.Quiet()) {
		t.Error("the trace is still held 200 ms after the member registered and the knock returned")
	}
}

func TestEverySendCarriesWhereItsTimeWent(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now().Add(100 * time.Millisecond))
	m := member(t, "a", 0, "up")
	sink := &trace{}
	got := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), sink).Members[0]
	list := sink.waitFor(func(l []knock.Attempt) bool { return len(l) >= got.Attempts })
	if len(list) == 0 {
		t.Fatal("no attempts traced")
	}
	for _, a := range list {
		if a.WokeUs == 0 || !(a.WokeUs <= a.HandedUs && a.HandedUs <= a.SentUs && a.SentUs <= a.ReturnedUs) {
			t.Errorf("stamps out of order: %+v", a)
		}
		if a.SentUs/1000 != a.SentMs || a.SlotLateUs < 0 || a.SlotLateUs > 50_000 {
			t.Errorf("stamps disagree: %+v", a)
		}
	}
}
