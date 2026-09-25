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
}
