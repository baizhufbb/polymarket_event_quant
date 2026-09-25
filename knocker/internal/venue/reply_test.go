package venue

import (
	"encoding/json"
	"os"
	"testing"
)

// replies.json was produced by the Python sender this package replaced
// (commit ec4d08b), reply by reply: what each HTTP reply turned into and
// how the knock loop judged it.
type replyCase struct {
	Status          int     `json:"status"`
	Body            string  `json:"body"`
	Trace           string  `json:"trace"`
	Transient       bool    `json:"transient"`
	ReplyJSON       *string `json:"reply_json"`
	ReplyText       *string `json:"reply_text"`
	Accepted        bool    `json:"accepted"`
	OrderID         *string `json:"order_id"`
	DuplicateID     *string `json:"duplicate_id"`
	NotReady        bool    `json:"not_ready"`
	VersionMismatch bool    `json:"version_mismatch"`
}

func loadReplies(t *testing.T) []replyCase {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/replies.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []replyCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 50 {
		t.Fatalf("only %d cases", len(cases))
	}
	return cases
}

func text(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func TestEveryReplyIsJudgedAsThePythonSenderJudgedIt(t *testing.T) {
	for _, c := range loadReplies(t) {
		got := FromHTTP(c.Status, []byte(c.Body))
		name := c.Body
		if got.Trace != c.Trace || got.Transient != c.Transient {
			t.Errorf("%d %q: trace %s transient %v, want %s %v", c.Status, name, got.Trace, got.Transient, c.Trace, c.Transient)
		}
		if c.Transient {
			continue
		}
		if got.Accepted != c.Accepted || got.OrderID != text(c.OrderID) ||
			got.DuplicateID != text(c.DuplicateID) || got.NotReady != c.NotReady ||
			got.VersionMismatch != c.VersionMismatch {
			t.Errorf("%d %q: got %+v, want accepted=%v id=%q dup=%q not_ready=%v version=%v",
				c.Status, name, got, c.Accepted, text(c.OrderID), text(c.DuplicateID), c.NotReady, c.VersionMismatch)
		}
	}
}

func TestAReplyValueIsJudgedLikeTheReplyItCameFrom(t *testing.T) {
	// What Python holds after the fold - a dict, a list, a string - reads
	// the same through FromValue as the HTTP reply did.
	for _, c := range loadReplies(t) {
		if c.Transient {
			continue
		}
		value := json.RawMessage(text(c.ReplyJSON))
		if c.ReplyText != nil {
			value, _ = json.Marshal(*c.ReplyText)
		}
		got := FromValue(value)
		if got.Trace != c.Trace || got.Accepted != c.Accepted || got.OrderID != text(c.OrderID) ||
			got.NotReady != c.NotReady {
			t.Errorf("%s: got %+v, want %s accepted=%v id=%q not_ready=%v",
				value, got, c.Trace, c.Accepted, text(c.OrderID), c.NotReady)
		}
	}
}

func TestARejectionFoldsIntoTheErrorMessageTheClientWouldShow(t *testing.T) {
	cases := map[string]string{
		`{"error":"invalid token id"}`: `{"errorMsg":"invalid token id","success":false}`,
		`Bad Request`:                  `{"errorMsg":"Bad Request","success":false}`,
		`{}`:                           `{"errorMsg":"PolyApiException[status_code=400, error_message={}]","success":false}`,
		``:                             `{"errorMsg":"PolyApiException[status_code=400, error_message=]","success":false}`,
	}
	for body, want := range cases {
		if got := string(foldedRejection(400, []byte(body))); got != want {
			t.Errorf("%q folded to %s, want %s", body, got, want)
		}
	}
}
