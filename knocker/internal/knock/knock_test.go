package knock_test

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"polymarket_event_quant/knocker/internal/fakevenue"
	"polymarket_event_quant/knocker/internal/knock"
)

const secret = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="

type trace struct {
	mu   sync.Mutex
	list []knock.Attempt
}

func (t *trace) put(a knock.Attempt) {
	t.mu.Lock()
	t.list = append(t.list, a)
	t.mu.Unlock()
}

func (t *trace) all() []knock.Attempt {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]knock.Attempt(nil), t.list...)
}

// waitFor polls until ok or a few seconds pass.
func (t *trace) waitFor(ok func([]knock.Attempt) bool) []knock.Attempt {
	deadline := time.Now().Add(6 * time.Second)
	for {
		list := t.all()
		if ok(list) || time.Now().After(deadline) {
			return list
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func start(t *testing.T) *fakevenue.Venue {
	t.Helper()
	v, err := fakevenue.Start(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.Close)
	return v
}

// member builds an account named after the test, so no two tests share an
// account's place on the timetable.
func member(t *testing.T, name string, phaseMs float64, legs ...string) knock.Member {
	account := t.Name() + "/" + name
	m := knock.Member{
		Account:    account,
		PhaseMs:    phaseMs,
		Address:    "0xSigner",
		APIKey:     "key",
		APISecret:  secret,
		Passphrase: "pass",
	}
	for _, outcome := range legs {
		body, _ := json.Marshal(map[string]string{"account": account, "outcome": outcome})
		m.Legs = append(m.Legs, knock.Leg{Outcome: outcome, Body: string(body)})
	}
	return m
}

func plan(fake *fakevenue.Venue, interval, knockFor time.Duration, members ...knock.Member) knock.Plan {
	now := time.Now()
	return knock.Plan{
		Market:       "test",
		IntervalMs:   float64(interval) / float64(time.Millisecond),
		KnockUntilMs: now.Add(knockFor).UnixMilli(),
		MarketEndMs:  now.Add(time.Hour).UnixMilli(),
		BaseURL:      fake.URL,
		CAFile:       fake.CAFile,
		Members:      members,
	}
}

func run(t *testing.T, p knock.Plan, sink *trace) knock.Result {
	t.Helper()
	result, err := knock.Run(p, sink.put)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func texts(items []knock.Item) []string {
	var out []string
	for _, item := range items {
		switch {
		case item.Text != nil:
			out = append(out, *item.Text)
		case item.Status != nil:
			out = append(out, *item.Body)
		}
	}
	return out
}

func requestsOf(fake *fakevenue.Venue, account string) []fakevenue.Request {
	var out []fakevenue.Request
	for _, r := range fake.Requests() {
		if strings.Contains(r.Body, `"account":"`+account+`"`) {
			out = append(out, r)
		}
	}
	return out
}

func TestTheFleetKnocksUntilTheDoorOpensAndKeepsWhatRegistered(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now().Add(200 * time.Millisecond))
	a, b := member(t, "a", 0, "up", "down"), member(t, "b", 12.5, "up", "down")
	sink := &trace{}
	result := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, a, b), sink)

	for i, m := range []knock.Member{a, b} {
		got := result.Members[i]
		if got.Account != m.Account || len(got.Accepted) != 2 || got.GaveUp || got.RegisteredMs == nil {
			t.Fatalf("%s: %+v", m.Account, got)
		}
		for j, order := range got.Accepted {
			if order.Outcome != m.Legs[j].Outcome || order.OrderID != fakevenue.OrderID(m.Legs[j].Body) {
				t.Errorf("%s: accepted %+v", m.Account, order)
			}
		}
		if len(got.Ambiguous) != 0 {
			t.Errorf("%s: ambiguous %v", m.Account, texts(got.Ambiguous))
		}
		if errors := texts(got.Errors); len(errors) != 1 || errors[0] != `{"error":"invalid token id"}` {
			t.Errorf("%s: errors %v", m.Account, errors)
		}
	}

	// Every send is in the trace once, with what its reply was.
	total := result.Members[0].Attempts + result.Members[1].Attempts
	list := sink.waitFor(func(l []knock.Attempt) bool { return len(l) >= total })
	if len(list) != total {
		t.Fatalf("%d attempts traced, %d sent", len(list), total)
	}
	seen := map[string]bool{}
	firstAccepted := map[string]int64{}
	for _, attempt := range list {
		key := fmt.Sprintf("%s#%d", attempt.Account, attempt.Attempt)
		if seen[key] || len(attempt.Results) != 1 || attempt.ReturnedMs < attempt.SentMs {
			t.Errorf("attempt %+v", attempt)
		}
		seen[key] = true
		if attempt.Results[0] == "accepted" || attempt.Results[0] == "duplicate" {
			if first, ok := firstAccepted[attempt.Account]; !ok || attempt.ReturnedMs < first {
				firstAccepted[attempt.Account] = attempt.ReturnedMs
			}
		}
	}
	// The registration time is the earliest reply that carried an order.
	for i, m := range []knock.Member{a, b} {
		if got := *result.Members[i].RegisteredMs; got != firstAccepted[m.Account] {
			t.Errorf("%s registered at %d, earliest accepting reply at %d", m.Account, got, firstAccepted[m.Account])
		}
		if result.Members[i].HeldBack != 0 {
			t.Errorf("%s: a venue answering promptly held back %d slots", m.Account, result.Members[i].HeldBack)
		}
	}

	// The two members sit half a cadence apart on one timetable.
	gaps := []float64{}
	early, late := requestsOf(fake, a.Account), requestsOf(fake, b.Account)
	for _, r := range late {
		best := time.Duration(-1)
		for _, e := range early {
			if d := r.Arrived.Sub(e.Arrived); d >= 0 && (best < 0 || d < best) {
				best = d
			}
		}
		if best >= 0 {
			gaps = append(gaps, float64(best)/float64(time.Millisecond))
		}
	}
	sort.Float64s(gaps)
	if len(gaps) < 4 {
		t.Fatalf("too few sends to measure: %v", gaps)
	}
	if median := gaps[len(gaps)/2]; median < 8 || median > 17 {
		t.Errorf("members %.1f ms apart (median), want about 12.5: %v", median, gaps)
	}
}

func TestLegsTakeTurnsUntilOneRegisters(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now().Add(150 * time.Millisecond))
	m := member(t, "a", 0, "up", "down")
	result := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), &trace{})
	if len(result.Members[0].Accepted) != 2 {
		t.Fatalf("%+v", result.Members[0])
	}
	sent := requestsOf(fake, m.Account)
	for i := range min(4, len(sent)) {
		want := m.Legs[i%2].Body
		if sent[i].Body != want {
			t.Errorf("send %d was %s, want %s", i, sent[i].Body, want)
		}
	}
}

func TestKnockingGivesUpAfterItsBudget(t *testing.T) {
	fake := start(t)
	m := member(t, "a", 0, "up", "down")
	result := run(t, plan(fake, 25*time.Millisecond, 200*time.Millisecond, m), &trace{})
	got := result.Members[0]
	if !got.GaveUp || len(got.Accepted) != 0 || got.Attempts == 0 {
		t.Fatalf("%+v", got)
	}
	if errors := texts(got.Errors); errors[len(errors)-1] != knock.KnockBudgetError {
		t.Errorf("errors %v", errors)
	}
}

func TestAMarketThatEndsStopsTheKnock(t *testing.T) {
	fake := start(t)
	m := member(t, "a", 0, "up")
	p := plan(fake, 25*time.Millisecond, time.Hour, m)
	p.MarketEndMs = time.Now().Add(200 * time.Millisecond).UnixMilli()
	got := run(t, p, &trace{}).Members[0]
	if got.GaveUp || len(got.Accepted) != 0 {
		t.Fatalf("%+v", got)
	}
	if errors := texts(got.Errors); errors[len(errors)-1] != knock.MarketEndedError {
		t.Errorf("errors %v", errors)
	}
}

func TestABusinessRejectionStopsKnocking(t *testing.T) {
	fake := start(t)
	fake.Respond = func(int, fakevenue.Request) (fakevenue.Response, bool) {
		return fakevenue.Response{Status: 400, Body: `{"error":"not enough balance / allowance"}`}, true
	}
	m := member(t, "a", 0, "up", "down")
	got := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), &trace{}).Members[0]
	if got.GaveUp || len(got.Accepted) != 0 || got.Attempts > 3 {
		t.Fatalf("%+v", got)
	}
	if errors := texts(got.Errors); len(errors) != 1 || !strings.Contains(errors[0], "not enough balance") {
		t.Errorf("errors %v", errors)
	}
}

func TestATransientReplyIsNoVerdict(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now())
	fake.Respond = func(n int, _ fakevenue.Request) (fakevenue.Response, bool) {
		if n < 3 {
			return fakevenue.Response{Status: 503, Body: ""}, true
		}
		return fakevenue.Response{}, false
	}
	m := member(t, "a", 0, "up", "down")
	got := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), &trace{}).Members[0]
	if len(got.Accepted) != 2 {
		t.Fatalf("%+v", got)
	}
	if len(got.Ambiguous) != 1 || *got.Ambiguous[0].Status != 503 {
		t.Errorf("ambiguous %+v", got.Ambiguous)
	}
}

func TestAnAccountAtItsCeilingGivesUpSlotsInsteadOfQueueing(t *testing.T) {
	fake := start(t)
	release := make(chan struct{})
	fake.Respond = func(int, fakevenue.Request) (fakevenue.Response, bool) {
		<-release
		return fakevenue.Response{Status: 400, Body: `{"error":"invalid token id"}`}, true
	}
	m := member(t, "a", 0, "up")
	go func() {
		time.Sleep(1200 * time.Millisecond)
		close(release)
	}()
	got := run(t, plan(fake, time.Millisecond, time.Second, m), &trace{}).Members[0]
	if got.Attempts != knock.InFlightCap || got.HeldBack == 0 || !got.GaveUp {
		t.Fatalf("attempts %d held back %d gave up %v", got.Attempts, got.HeldBack, got.GaveUp)
	}
}

func TestRepliesInFlightWhenSendingStopsAreStillCollected(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now())
	fake.Respond = func(n int, r fakevenue.Request) (fakevenue.Response, bool) {
		if n == 0 {
			id := fakevenue.OrderID(r.Body)
			return fakevenue.Response{Status: 200, Body: `{"orderID":"` + id + `","success":true}`, Delay: 500 * time.Millisecond}, true
		}
		return fakevenue.Response{}, false
	}
	m := member(t, "a", 0, "up", "down")
	sink := &trace{}
	began := time.Now()
	got := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), sink).Members[0]
	took := time.Since(began)
	if len(got.Accepted) != 2 || len(got.Ambiguous) != 0 {
		t.Fatalf("%+v", got)
	}
	if took < 450*time.Millisecond || took > 2500*time.Millisecond {
		t.Errorf("returned after %v: should wait for the slow reply, and not for the whole drain", took)
	}
	for _, attempt := range sink.all() {
		if attempt.Attempt == 1 && attempt.ReturnedMs-attempt.SentMs < 450 {
			t.Errorf("the slow reply came back after %d ms", attempt.ReturnedMs-attempt.SentMs)
		}
	}
}

func TestAReplyLandingAfterTheDrainIsStillTraced(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now())
	fake.Respond = func(n int, r fakevenue.Request) (fakevenue.Response, bool) {
		if n == 0 {
			id := fakevenue.OrderID(r.Body)
			return fakevenue.Response{Status: 200, Body: `{"orderID":"` + id + `","success":true}`, Delay: knock.DrainTime + 600*time.Millisecond}, true
		}
		return fakevenue.Response{}, false
	}
	m := member(t, "a", 0, "up")
	sink := &trace{}
	began := time.Now()
	got := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), sink).Members[0]
	took := time.Since(began)
	if len(got.Accepted) != 1 || took < knock.DrainTime-200*time.Millisecond || took > knock.DrainTime+500*time.Millisecond {
		t.Fatalf("returned after %v with %+v", took, got)
	}
	list := sink.waitFor(func(l []knock.Attempt) bool {
		for _, a := range l {
			if a.Attempt == 1 {
				return true
			}
		}
		return false
	})
	found := false
	for _, a := range list {
		if a.Attempt == 1 {
			found = a.Results[0] == "accepted"
		}
	}
	if !found {
		t.Errorf("the late reply never reached the trace: %+v", list)
	}
}

func TestConflictingOrderIDsStopTheMemberAndSaySo(t *testing.T) {
	fake := start(t)
	fake.Respond = func(n int, _ fakevenue.Request) (fakevenue.Response, bool) {
		switch n {
		case 0:
			return fakevenue.Response{Status: 200, Body: `{"orderID":"0x` + strings.Repeat("a", 64) + `","success":true}`, Delay: 150 * time.Millisecond}, true
		default:
			return fakevenue.Response{Status: 200, Body: `{"orderID":"0x` + strings.Repeat("b", 64) + `","success":true}`}, true
		}
	}
	m := member(t, "a", 0, "up")
	got := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), &trace{}).Members[0]
	ambiguous := texts(got.Ambiguous)
	if len(ambiguous) != 1 || !strings.HasPrefix(ambiguous[0], "conflicting up order ids: 0xbbbb") {
		t.Fatalf("ambiguous %v", ambiguous)
	}
}

func TestAVersionMismatchIsFlaggedInTheTrace(t *testing.T) {
	fake := start(t)
	fake.Respond = func(int, fakevenue.Request) (fakevenue.Response, bool) {
		return fakevenue.Response{Status: 200, Body: `{"error":"order_version_mismatch","success":false}`}, true
	}
	m := member(t, "a", 0, "up")
	sink := &trace{}
	got := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), sink).Members[0]
	if len(got.Accepted) != 0 {
		t.Fatalf("%+v", got)
	}
	list := sink.waitFor(func(l []knock.Attempt) bool { return len(l) >= got.Attempts })
	if len(list) == 0 || !list[0].VersionMismatch || list[0].Results[0] != "rejected" {
		t.Errorf("trace %+v", list)
	}
}

func TestANewMarketNeverStartsInsideAnAccountsLastSpacing(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now())
	first := member(t, "a", 0, "up")
	second := first
	second.Legs = []knock.Leg{{Outcome: "up", Body: `{"account":"` + first.Account + `","market":2}`}}
	run(t, plan(fake, 100*time.Millisecond, 5*time.Second, first), &trace{})
	run(t, plan(fake, 100*time.Millisecond, 5*time.Second, second), &trace{})
	sent := requestsOf(fake, first.Account)
	if len(sent) < 2 {
		t.Fatalf("sent %d", len(sent))
	}
	last := sent[0]
	for _, r := range sent {
		if !strings.Contains(r.Body, `"market":2`) {
			last = r
		}
	}
	for _, r := range sent {
		if strings.Contains(r.Body, `"market":2`) {
			if gap := r.Arrived.Sub(last.Arrived); gap < 90*time.Millisecond {
				t.Errorf("the next market sent %v after the last send", gap)
			}
			break
		}
	}
}

func TestOurOwnBadSecretStopsTheMemberAndSaysSo(t *testing.T) {
	fake := start(t)
	m := member(t, "a", 0, "up")
	m.APISecret = "not base64 !!"
	sink := &trace{}
	got := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), sink).Members[0]
	ambiguous := texts(got.Ambiguous)
	if len(got.Accepted) != 0 || len(ambiguous) != 1 || !strings.Contains(ambiguous[0], "not base64") {
		t.Fatalf("%+v %v", got, ambiguous)
	}
	if len(fake.Requests()) != 0 {
		t.Errorf("something reached the venue")
	}
	list := sink.waitFor(func(l []knock.Attempt) bool { return len(l) >= got.Attempts })
	if list[0].Results[0] != "transport_error" {
		t.Errorf("trace %+v", list[0])
	}
}

func TestSendingResumesOnceRepliesComeBack(t *testing.T) {
	fake := start(t)
	release := make(chan struct{})
	fake.Respond = func(int, fakevenue.Request) (fakevenue.Response, bool) {
		<-release
		return fakevenue.Response{Status: 400, Body: `{"error":"invalid token id"}`}, true
	}
	m := member(t, "a", 0, "up")
	go func() {
		time.Sleep(700 * time.Millisecond)
		close(release)
	}()
	got := run(t, plan(fake, time.Millisecond, 2*time.Second, m), &trace{}).Members[0]
	if got.HeldBack == 0 || got.Attempts <= knock.InFlightCap {
		t.Fatalf("attempts %d held back %d: holding back must end once replies come back", got.Attempts, got.HeldBack)
	}
}

func TestStopAllEndsTheKnockAndKeepsWhatRegistered(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now())
	registers, knocks := member(t, "registers", 0, "up"), member(t, "knocks", 12.5, "up")
	fake.Respond = func(_ int, r fakevenue.Request) (fakevenue.Response, bool) {
		if strings.Contains(r.Body, knocks.Account) {
			return fakevenue.Response{Status: 400, Body: `{"error":"invalid token id"}`}, true
		}
		return fakevenue.Response{}, false
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		knock.StopAll()
	}()
	began := time.Now()
	result := run(t, plan(fake, 25*time.Millisecond, 10*time.Second, registers, knocks), &trace{})
	if took := time.Since(began); took > 1500*time.Millisecond {
		t.Errorf("returned %v after the start, stop came at 300 ms", took)
	}
	if got := result.Members[0]; len(got.Accepted) != 1 {
		t.Errorf("the registered order was lost: %+v", got)
	}
	got := result.Members[1]
	if got.GaveUp || len(got.Accepted) != 0 {
		t.Fatalf("%+v", got)
	}
	if errors := texts(got.Errors); errors[len(errors)-1] != knock.StoppedError {
		t.Errorf("errors %v", errors)
	}
}

func TestARedirectIsAnAnswerNotAPlaceToSendTheOrder(t *testing.T) {
	fake := start(t)
	fake.Respond = func(int, fakevenue.Request) (fakevenue.Response, bool) {
		return fakevenue.Response{Status: 302, Body: "moved", Location: "/order-elsewhere"}, true
	}
	m := member(t, "a", 0, "up")
	sink := &trace{}
	got := run(t, plan(fake, 25*time.Millisecond, 5*time.Second, m), sink).Members[0]
	if len(got.Accepted) != 0 || len(got.Errors) != 1 || *got.Errors[0].Status != 302 {
		t.Fatalf("%+v", got)
	}
	list := sink.waitFor(func(l []knock.Attempt) bool { return len(l) >= got.Attempts })
	if list[0].Status != 302 || list[0].Results[0] != "rejected" {
		t.Errorf("trace %+v", list[0])
	}
}

func TestTheDeadlineHoldsWithoutASlotToCheckIt(t *testing.T) {
	fake := start(t)
	m := member(t, "a", 0, "up")
	began := time.Now()
	got := run(t, plan(fake, 10*time.Second, 300*time.Millisecond, m), &trace{}).Members[0]
	if took := time.Since(began); took > 2*time.Second {
		t.Errorf("returned after %v, the budget was 300 ms and the next slot 10 s away", took)
	}
	if !got.GaveUp || got.Attempts != 1 {
		t.Errorf("%+v", got)
	}
}

func TestRepliesPastAMembersOwnDrainNoLongerChangeItsResult(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now())
	quick, slow := member(t, "quick", 0, "up"), member(t, "slow", 12.5, "up")
	var mu sync.Mutex
	sent := map[string]int{}
	fake.Respond = func(_ int, r fakevenue.Request) (fakevenue.Response, bool) {
		if strings.Contains(r.Body, slow.Account) {
			return fakevenue.Response{Status: 400, Body: `{"error":"invalid token id"}`}, true
		}
		mu.Lock()
		n := sent[quick.Account]
		sent[quick.Account]++
		mu.Unlock()
		if n == 0 {
			// Answers after the member's own drain, with an order id that
			// contradicts the one it kept.
			return fakevenue.Response{
				Status: 200,
				Body:   `{"orderID":"0x` + strings.Repeat("a", 64) + `","success":true}`,
				Delay:  knock.DrainTime + 700*time.Millisecond,
			}, true
		}
		return fakevenue.Response{Status: 200, Body: `{"orderID":"0x` + strings.Repeat("b", 64) + `","success":true}`}, true
	}
	sink := &trace{}
	result := run(t, plan(fake, 25*time.Millisecond, knock.DrainTime+2*time.Second, quick, slow), sink)
	got := result.Members[0]
	if len(got.Accepted) != 1 || got.Accepted[0].OrderID != "0x"+strings.Repeat("b", 64) || len(got.Ambiguous) != 0 {
		t.Fatalf("%+v %v", got, texts(got.Ambiguous))
	}
	if !result.Members[1].GaveUp {
		t.Errorf("the other member should have knocked on to its budget: %+v", result.Members[1])
	}
	list := sink.waitFor(func(l []knock.Attempt) bool {
		for _, a := range l {
			if a.Account == quick.Account && a.Attempt == 1 {
				return true
			}
		}
		return false
	})
	traced := false
	for _, a := range list {
		traced = traced || (a.Account == quick.Account && a.Attempt == 1)
	}
	if !traced {
		t.Errorf("the late reply is missing from the trace")
	}
}
