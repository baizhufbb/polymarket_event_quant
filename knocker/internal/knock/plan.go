package knock

// Plan is one market's knock for every member of the fleet, as Python
// hands it over.
type Plan struct {
	Market     string  `json:"market"`
	IntervalMs float64 `json:"interval_ms"`
	// Wall-clock milliseconds: stop knocking at the first of the two.
	KnockUntilMs int64 `json:"knock_until_ms"`
	MarketEndMs  int64 `json:"market_end_ms"`
	// Where the orders go; empty means the exchange itself. CAFile, when
	// set, is the only certificate authority trusted.
	BaseURL string   `json:"base_url,omitempty"`
	CAFile  string   `json:"ca_file,omitempty"`
	Members []Member `json:"members"`
	// Preview, when set, holds every send until the market's record on
	// Gamma turns active, then times the sends from the startDate written
	// into it. Without it the knock starts at once on the cadence.
	Preview *Preview `json:"preview,omitempty"`
}

// Preview is where to watch for a market about to open, and how to send
// once it shows.
//
// Gamma's record of a market turns active, with a startDate to the
// microsecond, about a third of a second before the venue opens the book;
// before that the venue answers every order "not ready".
type Preview struct {
	// The market's record: gamma-api .../markets/slug/<slug>.
	URL string `json:"url"`
	// How often the record is asked for. Gamma's edge allows each IP 300
	// asks of its markets per 10 s and delays the ones over.
	PollMs float64 `json:"poll_ms"`
	// The bursts, timed from startDate; after the last one the members go
	// on at the plan's cadence.
	Bursts []Burst `json:"bursts"`
}

// Burst has every member send once per IntervalMs from FromMs to UntilMs
// after startDate, the members spread evenly across each interval.
type Burst struct {
	FromMs     float64 `json:"from_ms"`
	UntilMs    float64 `json:"until_ms"`
	IntervalMs float64 `json:"interval_ms"`
}

// Member is one account's part: its phase on the shared timetable, its
// credentials, and its signed orders.
type Member struct {
	Account    string  `json:"account"`
	PhaseMs    float64 `json:"phase_ms"`
	Address    string  `json:"address"`
	APIKey     string  `json:"api_key"`
	APISecret  string  `json:"api_secret"`
	Passphrase string  `json:"api_passphrase"`
	Legs       []Leg   `json:"legs"`
}

// Leg is one signed order: the exact request body, sent unchanged on
// every knock.
type Leg struct {
	Outcome string `json:"outcome"`
	Body    string `json:"body"`
}

// Result is every member's outcome, in plan order.
type Result struct {
	Members []MemberResult `json:"members"`
	// What the watch for the preview saw; absent without a Preview.
	Preview *PreviewSeen `json:"preview,omitempty"`
}

// PreviewSeen is the watch on the market's record, for the log.
type PreviewSeen struct {
	// The record's startDate, wall-clock ms with its microseconds; 0 if it
	// never turned active (SeenMs 0 too) or turned without a startDate it
	// could read (SeenMs set; the bursts were timed from SeenMs).
	StartDateMs float64 `json:"start_date_ms"`
	// When the ask that first found it active was sent, and when its answer
	// came back.
	AskedMs int64 `json:"asked_ms"`
	SeenMs  int64 `json:"seen_ms"`
	// When the bursts were timed from: startDate, or the moment it was seen
	// if the record put startDate later than that.
	AnchorMs float64 `json:"anchor_ms"`
	Asks     int     `json:"asks"`
	// Asks that failed or came back other than 200.
	Failed int `json:"failed"`
	// Asks not sent because previewAsksInFlight were still out.
	Skipped int `json:"skipped"`
}

// MemberResult is what one account's knocking came to.
type MemberResult struct {
	Account  string `json:"account"`
	Attempts int    `json:"attempts"`
	// Slots given up because the account already had InFlightCap requests
	// outstanding.
	HeldBack int `json:"held_back"`
	// The registered orders, one per leg at most, in leg order.
	Accepted []AcceptedOrder `json:"accepted"`
	// When the earliest reply carrying a registered order came back.
	RegisteredMs *int64 `json:"registered_ms"`
	// Replies and events that ended without an order, first seen first,
	// each once.
	Errors []Item `json:"errors"`
	// Sends that came back without a verdict (failed, 429, 5xx), and
	// replies that contradict each other.
	Ambiguous []Item `json:"ambiguous"`
	GaveUp    bool   `json:"gave_up"`
}

// AcceptedOrder is a registered order and the reply that carried it.
type AcceptedOrder struct {
	Outcome string `json:"outcome"`
	OrderID string `json:"order_id"`
	Status  int    `json:"status"`
	Body    string `json:"body"`
}

// Item is either an HTTP reply (status 0: nothing came back) or a text.
type Item struct {
	Status *int    `json:"status,omitempty"`
	Body   *string `json:"body,omitempty"`
	Text   *string `json:"text,omitempty"`
}

// Attempt is one send and its reply, for the attempt trace. It is emitted
// when the reply lands, also after the knock that sent it has returned.
type Attempt struct {
	Account    string   `json:"account"`
	Attempt    int      `json:"attempt"`
	Legs       []string `json:"legs"`
	SentMs     int64    `json:"sent_ts_ms"`
	ReturnedMs int64    `json:"returned_ts_ms"`
	Results    []string `json:"results"`
	// The raw reply, for the log: status 0 means the send failed and
	// Error says why.
	Status          int    `json:"status"`
	Body            string `json:"body,omitempty"`
	Error           string `json:"error,omitempty"`
	VersionMismatch bool   `json:"version_mismatch,omitempty"`
	// Where the time went, wall-clock microseconds: the timing thread woke
	// for the slot SlotLateUs after it was due, at WokeUs; the coordinator
	// handed it to a send at HandedUs; the send began at SentUs and had its
	// reply read at ReturnedUs.
	SlotLateUs int64 `json:"slot_late_us"`
	WokeUs     int64 `json:"woke_us"`
	HandedUs   int64 `json:"handed_us"`
	SentUs     int64 `json:"sent_us"`
	ReturnedUs int64 `json:"returned_us"`
}
