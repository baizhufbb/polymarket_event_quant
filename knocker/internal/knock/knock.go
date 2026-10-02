// Package knock sends one market's signed orders on the fleet's timetable
// until the venue registers them, and says what came of it.
package knock

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"polymarket_event_quant/knocker/internal/venue"
)

const (
	// DrainTime is how long a member keeps collecting replies after it
	// stops sending. The request that actually registered an order is
	// usually an earlier one whose reply has not arrived yet, and it is
	// the one that shows which send won the queue slot.
	DrainTime = 3 * time.Second
	// The largest fleet planned.
	fleetAccounts = 5
	// InFlightCap is the most requests one account may have outstanding:
	// its even share of the connections' streams at the full fleet. At the
	// ceiling a send would have to wait for a stream and leave late, so
	// the slot is given up instead.
	InFlightCap = venue.Lanes * venue.StreamsPerLane / fleetAccounts
	// The timing thread sleeps on Go's own timer, which can wake a
	// millisecond late, until this long before a slot, and the rest on the
	// precise clock. On the precise clock it holds on to its processor, and
	// the server has one: everything else in the knock waits meanwhile. On
	// Go's timer it holds nothing.
	preciseStretch = 1500 * time.Microsecond

	KnockBudgetError = "no acceptance within the knocking budget"
	MarketEndedError = "market ended before both orders were accepted"
	StoppedError     = "knocking stopped: the bot is shutting down"
)

// nextSlot is, per account, the first slot it may use on the monotonic
// clock. It outlives a knock, so a new market never starts an account
// inside the spacing of its previous send.
var (
	nextMu   sync.Mutex
	nextSlot = map[string]int64{}
)

// running is every knock in progress, for StopAll.
var (
	runningMu sync.Mutex
	running   = map[*run]bool{}
)

// StopAll has every knock in progress stop sending now. Each still
// collects the replies in flight for DrainTime and returns what
// registered, so the bot can record those orders - and cancel them - on
// its way out.
func StopAll() {
	runningMu.Lock()
	defer runningMu.Unlock()
	for r := range running {
		r.stopOnce.Do(func() { close(r.stopping) })
	}
}

type member struct {
	plan  Member
	creds venue.Creds
	phase int64
	// Everything below is the coordinator's alone.
	attempts   int
	heldBack   int
	inFlight   int
	accepted   map[string]AcceptedOrder
	registered int64
	errors     items
	ambiguous  items
	stopped    bool
	gaveUp     bool
	drainUntil time.Time
	// Read by the timing thread: false once the member stops sending.
	sending atomic.Bool
}

type slot struct{ member int }

type reply struct {
	member   int
	outcome  string
	returned int64
	status   int
	body     []byte
	class    venue.Class
	// Why nothing came back, for the log.
	failure error
	// Our own failure before anything was sent.
	local error
}

type run struct {
	plan     Plan
	venue    *venue.Venue
	interval int64
	deadline time.Time
	members  []*member
	emit     func(Attempt)
	slots    chan slot
	replies  chan reply
	failed   chan error
	stopping chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// Run knocks one market for every member and returns once each has stopped
// sending and collected its replies, or DrainTime has passed since it
// stopped. Every reply goes to emit when it lands, including the ones that
// land after Run has returned.
func Run(plan Plan, emit func(Attempt)) (Result, error) {
	interval := int64(math.Round(plan.IntervalMs * 1e6))
	if interval <= 0 {
		return Result{}, errors.New("interval_ms must be above 0")
	}
	if len(plan.Members) == 0 {
		return Result{}, errors.New("a knock needs at least one member")
	}
	v, err := venue.For(plan.BaseURL, plan.CAFile)
	if err != nil {
		return Result{}, err
	}
	r := &run{
		plan:     plan,
		venue:    v,
		interval: interval,
		deadline: time.UnixMilli(min(plan.KnockUntilMs, plan.MarketEndMs)),
		emit:     emit,
		slots:    make(chan slot, 1024),
		replies:  make(chan reply, 4096),
		failed:   make(chan error, 1),
		stopping: make(chan struct{}),
		done:     make(chan struct{}),
	}
	for _, m := range plan.Members {
		if len(m.Legs) == 0 {
			return Result{}, fmt.Errorf("member %s has no orders", m.Account)
		}
		state := &member{
			plan: m,
			creds: venue.Creds{
				Address:    m.Address,
				APIKey:     m.APIKey,
				Secret:     m.APISecret,
				Passphrase: m.Passphrase,
			},
			phase:     int64(math.Round(m.PhaseMs * 1e6)),
			accepted:  map[string]AcceptedOrder{},
			errors:    newItems(),
			ambiguous: newItems(),
		}
		state.sending.Store(true)
		r.members = append(r.members, state)
	}
	runningMu.Lock()
	running[r] = true
	runningMu.Unlock()
	defer func() {
		runningMu.Lock()
		delete(running, r)
		runningMu.Unlock()
	}()
	defer close(r.done)

	v.Warm()
	go r.tick()
	// The coordinator gets a goroutine of its own: the caller's is tied to
	// the Python thread that made the call, and every wake-up would first
	// have to find that thread.
	finished := make(chan struct{})
	var failure error
	go func() {
		defer close(finished)
		defer func() {
			if p := recover(); p != nil {
				failure = fmt.Errorf("coordinator: %v", p)
			}
		}()
		r.coordinate()
	}()
	<-finished
	result := r.result()
	if failure != nil {
		// What was recorded stands; a member still sending says why it
		// ended without a verdict.
		for i, m := range r.members {
			if !m.stopped {
				text := "knock failed: " + failure.Error()
				result.Members[i].Ambiguous = append(result.Members[i].Ambiguous, Item{Text: &text})
			}
		}
	}
	return result, nil
}

// tick is the timing thread: it wakes on each member's next slot and hands
// it to the coordinator, members interleaved in time order.
func (r *run) tick() {
	defer func() {
		if p := recover(); p != nil {
			select {
			case r.failed <- fmt.Errorf("timing thread: %v", p):
			case <-r.done:
			}
		}
	}()
	pinTimingThread()
	origin := now()
	earliest := make([]int64, len(r.members))
	nextMu.Lock()
	for i, m := range r.members {
		earliest[i] = nextSlot[m.plan.Account]
	}
	nextMu.Unlock()
	next := make([]int64, len(r.members))
	for i, m := range r.members {
		next[i] = SlotAtOrAfter(max(origin, earliest[i]), origin, m.phase, r.interval)
	}
	coarse := time.NewTimer(time.Hour)
	coarse.Stop()
	for {
		due := -1
		for i, m := range r.members {
			if m.sending.Load() && (due < 0 || next[i] < next[due]) {
				due = i
			}
		}
		if due < 0 {
			return
		}
		if wait := time.Duration(next[due]-now()) - preciseStretch; wait > 0 {
			coarse.Reset(wait)
			select {
			case <-coarse.C:
			case <-r.done:
				return
			}
			if !r.members[due].sending.Load() {
				continue
			}
		}
		sleepUntil(next[due])
		// A knock that finished while this thread slept must not move the
		// account's next slot any more: the next market may already use it.
		select {
		case <-r.done:
			return
		default:
		}
		select {
		case r.slots <- slot{due}:
		case <-r.done:
			return
		}
		// The slot after this one, never one already gone: a stall skips
		// the slots it ate instead of re-basing the timetable, so the
		// offsets between members survive it.
		after := SlotAtOrAfter(max(next[due]+r.interval, now()), origin, r.members[due].phase, r.interval)
		next[due] = after
		nextMu.Lock()
		nextSlot[r.members[due].plan.Account] = after
		nextMu.Unlock()
	}
}

// coordinate owns every member's state; slots and replies come to it.
func (r *run) coordinate() {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	stopping := r.stopping
	for {
		wake, finished := r.pending()
		if finished {
			return
		}
		timer.Reset(time.Until(wake))
		select {
		case s := <-r.slots:
			r.onSlot(s.member)
		case rep := <-r.replies:
			r.onReply(rep)
		case err := <-r.failed:
			for _, m := range r.members {
				if !m.stopped {
					m.ambiguous.text("knock failed: " + err.Error())
					r.stop(m)
				}
			}
		case <-stopping:
			stopping = nil
			for _, m := range r.members {
				if !m.stopped {
					m.errors.text(StoppedError)
					r.stop(m)
				}
			}
		case <-timer.C:
			// The deadline holds even if no slot comes to check it.
			for _, m := range r.members {
				if !m.stopped {
					r.pastDeadline(m)
				}
			}
		}
		timer.Stop()
	}
}

// pending says whether every member is done, and otherwise when the
// coordinator must look again without a slot or reply: at the next drain's
// end, or at the knock's deadline while a member is still sending.
func (r *run) pending() (wake time.Time, finished bool) {
	finished = true
	current := time.Now()
	for _, m := range r.members {
		var due time.Time
		switch {
		case !m.stopped:
			due = r.deadline
		case m.inFlight > 0 && current.Before(m.drainUntil):
			due = m.drainUntil
		default:
			continue
		}
		finished = false
		if wake.IsZero() || due.Before(wake) {
			wake = due
		}
	}
	return wake, finished
}

// pastDeadline stops a member whose market ended or whose knocking budget
// ran out.
func (r *run) pastDeadline(m *member) bool {
	wall := time.Now().UnixMilli()
	if wall >= r.plan.MarketEndMs {
		m.errors.text(MarketEndedError)
		r.stop(m)
		return true
	}
	if wall >= r.plan.KnockUntilMs {
		// The door did not open inside the knocking budget; the caller
		// skips the market. Since 2026-09-05 the venue sometimes opens a
		// book minutes to hours after the listing.
		m.errors.text(KnockBudgetError)
		m.gaveUp = true
		r.stop(m)
		return true
	}
	return false
}

func (r *run) onSlot(index int) {
	m := r.members[index]
	if m.stopped || r.pastDeadline(m) {
		return
	}
	if m.inFlight >= InFlightCap {
		// The venue is answering slower than this cadence sends: give the
		// slot up rather than deepen the queue, and stay on the timetable.
		m.heldBack++
		return
	}
	// One leg per slot, turning over the legs not registered yet: a
	// registered leg stops using the account's budget and the other
	// inherits every slot.
	remaining := make([]Leg, 0, len(m.plan.Legs))
	for _, leg := range m.plan.Legs {
		if _, ok := m.accepted[leg.Outcome]; !ok {
			remaining = append(remaining, leg)
		}
	}
	if len(remaining) == 0 {
		remaining = m.plan.Legs
	}
	leg := remaining[m.attempts%len(remaining)]
	m.attempts++
	m.inFlight++
	go r.send(index, m.creds, m.plan.Account, leg, m.attempts, r.venue.Pick())
}

// send posts one order and hands the reply to the coordinator if the knock
// is still running, then to the trace.
func (r *run) send(index int, creds venue.Creds, account string, leg Leg, attempt int, lane *venue.Lane) {
	rep := reply{member: index, outcome: leg.Outcome}
	sent := time.Now().UnixMilli()
	defer func() {
		if p := recover(); p != nil {
			rep.local = fmt.Errorf("send failed: %v", p)
		}
		if rep.local != nil {
			rep.status = 0
			rep.class = venue.Class{Trace: venue.TransportError, Transient: true}
		}
		if rep.returned == 0 {
			rep.returned = time.Now().UnixMilli()
		}
		// The verdict first: a trace reader that falls behind must not hold
		// up the knock.
		select {
		case r.replies <- rep:
		case <-r.done:
		}
		record := Attempt{
			Account:         account,
			Attempt:         attempt,
			Legs:            []string{leg.Outcome},
			SentMs:          sent,
			ReturnedMs:      rep.returned,
			Results:         []string{rep.class.Trace},
			Status:          rep.status,
			VersionMismatch: rep.class.VersionMismatch,
		}
		if rep.status != 200 && rep.status != 0 {
			record.Body = string(rep.body)
		}
		switch {
		case rep.local != nil:
			record.Error = rep.local.Error()
		case rep.failure != nil:
			record.Error = rep.failure.Error()
		}
		r.emit(record)
	}()
	status, body, err := r.venue.SendOrder(lane, creds, []byte(leg.Body))
	rep.returned = time.Now().UnixMilli()
	switch {
	case err != nil && venue.IsSecretError(err):
		rep.local = err
	case err != nil:
		// Anything that stops the reply coming back is a failed send, not
		// a verdict on the market: the same order simply goes out again.
		rep.failure = err
		rep.class = venue.FromHTTP(0, nil)
	default:
		rep.status, rep.body = status, body
		rep.class = venue.FromHTTP(status, body)
	}
}

func (r *run) onReply(rep reply) {
	m := r.members[rep.member]
	m.inFlight--
	if m.stopped && time.Now().After(m.drainUntil) {
		// Past this member's own drain the reply is in the trace, but it no
		// longer changes what the member came to - as when each member ran
		// its own loop and left after its drain.
		return
	}
	class := rep.class
	switch {
	case rep.local != nil:
		// Our own failure: stop, and say so.
		m.ambiguous.text(rep.local.Error())
		r.stop(m)
		return
	case class.Transient:
		// No verdict; the same signed order goes out again on the next slot.
		m.ambiguous.reply(rep.status, rep.body)
		return
	}
	if class.Accepted {
		if earlier, ok := m.accepted[rep.outcome]; ok && earlier.OrderID != class.OrderID {
			m.ambiguous.text(fmt.Sprintf(
				"conflicting %s order ids: %s, %s", rep.outcome, earlier.OrderID, class.OrderID,
			))
			r.stop(m)
		} else {
			m.accepted[rep.outcome] = AcceptedOrder{
				Outcome: rep.outcome,
				OrderID: class.OrderID,
				Status:  rep.status,
				Body:    string(rep.body),
			}
			if m.registered == 0 || rep.returned < m.registered {
				m.registered = rep.returned
			}
		}
	} else {
		m.errors.reply(rep.status, rep.body)
	}
	if len(m.accepted) == len(m.plan.Legs) {
		// Stop sending, but keep draining: the reply of the send that
		// registered the order may still be on its way.
		r.stop(m)
		return
	}
	if !class.Accepted && !class.NotReady {
		// A verdict other than "not open yet": knocking again cannot help.
		r.stop(m)
	}
}

func (r *run) stop(m *member) {
	if m.stopped {
		return
	}
	m.stopped = true
	m.sending.Store(false)
	m.drainUntil = time.Now().Add(DrainTime)
}

func (r *run) result() Result {
	out := Result{Members: make([]MemberResult, 0, len(r.members))}
	for _, m := range r.members {
		result := MemberResult{
			Account:   m.plan.Account,
			Attempts:  m.attempts,
			HeldBack:  m.heldBack,
			Accepted:  []AcceptedOrder{},
			Errors:    m.errors.list,
			Ambiguous: m.ambiguous.list,
			GaveUp:    m.gaveUp,
		}
		for _, leg := range m.plan.Legs {
			if order, ok := m.accepted[leg.Outcome]; ok {
				result.Accepted = append(result.Accepted, order)
			}
		}
		if m.registered != 0 {
			registered := m.registered
			result.RegisteredMs = &registered
		}
		out.Members = append(out.Members, result)
	}
	return out
}

// items keeps each distinct reply or text once, in the order first seen.
type items struct {
	seen map[string]bool
	list []Item
}

func newItems() items { return items{seen: map[string]bool{}, list: []Item{}} }

func (l *items) reply(status int, body []byte) {
	key := strconv.Itoa(status) + "\x00" + string(body)
	if l.seen[key] {
		return
	}
	l.seen[key] = true
	text := string(body)
	l.list = append(l.list, Item{Status: &status, Body: &text})
}

func (l *items) text(text string) {
	key := "\x01" + text
	if l.seen[key] {
		return
	}
	l.seen[key] = true
	l.list = append(l.list, Item{Text: &text})
}
