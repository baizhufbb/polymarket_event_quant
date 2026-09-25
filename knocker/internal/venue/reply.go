// Package venue is everything that talks to the exchange: the order
// request, its L2 authentication headers, the HTTP/2 connections that carry
// it, and what each reply means.
package venue

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// The bucket each reply is written under in the attempt trace.
const (
	Accepted       = "accepted"
	Duplicate      = "duplicate"
	NotReady       = "not_ready"
	Rejected       = "rejected"
	RateLimited    = "rate_limited"
	TransportError = "transport_error"
)

const (
	marketNotReady         = "the market is not yet ready to process new orders"
	orderbookMissingPrefix = "the orderbook "
	orderbookMissingSuffix = " does not exist"
	orderVersionMismatch   = "order_version_mismatch"
)

// A resend of an order the venue already holds is answered with this
// message, and it carries the id of the order that did register.
var duplicateOrder = regexp.MustCompile(
	`(?i)\border\s+(0x[0-9a-f]{64})\s+is invalid\.\s*duplicated\.`,
)

// Class is what one reply means for the knock.
type Class struct {
	Trace string `json:"trace"`
	// No verdict: the send failed, or the venue answered 429 or 5xx. The
	// same signed order is simply sent again on the next slot.
	Transient bool `json:"transient"`
	// The reply carries the id of an order the venue holds.
	Accepted    bool   `json:"accepted"`
	OrderID     string `json:"order_id"`
	DuplicateID string `json:"duplicate_id"`
	// The book is not open yet; knocking goes on.
	NotReady bool `json:"not_ready"`
	// The venue wants orders signed for another protocol version.
	VersionMismatch bool `json:"version_mismatch"`
}

// FromHTTP classifies one HTTP reply to an order. Status 0 is a send that
// failed before any reply came back.
//
// It keeps the semantics the Python sender had: 429 and 5xx carry no
// verdict; any other non-200 is folded into {"errorMsg": ..., "success":
// false} the way the official client folds a rejection; a 200 body that is
// not JSON is taken as text.
func FromHTTP(status int, body []byte) Class {
	switch {
	case status == 0:
		return Class{Trace: TransportError, Transient: true}
	case status == 429:
		return Class{Trace: RateLimited, Transient: true}
	case status >= 500 && status < 600:
		return Class{Trace: "error_" + strconv.Itoa(status), Transient: true}
	case status != 200:
		return FromValue(foldedRejection(status, body))
	}
	value := parseBody(body)
	class := FromValue(value)
	class.VersionMismatch = versionMismatch(value)
	return class
}

// FromValue classifies a reply the way it reads as a value: an object is
// judged on its order id and message, anything else is a rejection.
func FromValue(value json.RawMessage) Class {
	if kind(value) != '{' {
		return Class{Trace: Rejected}
	}
	fields := objectFields(value)
	message := errorMessage(fields)
	duplicateID := ""
	if match := duplicateOrder.FindStringSubmatch(message); match != nil {
		duplicateID = match[1]
	}
	orderID := ""
	for _, key := range []string{"orderID", "orderId", "order_id"} {
		if raw, ok := fields[key]; ok && truthy(raw) {
			orderID = pyStr(raw)
			break
		}
	}
	if orderID == "" {
		orderID = duplicateID
	}
	class := Class{OrderID: orderID, DuplicateID: duplicateID}
	class.Accepted = duplicateID != "" || (!isFalse(fields["success"]) && orderID != "")
	class.NotReady = orderID == "" && notReadyMessage(message)
	switch {
	case duplicateID != "":
		class.Trace = Duplicate
	case class.Accepted:
		class.Trace = Accepted
	case class.NotReady:
		class.Trace = NotReady
	default:
		class.Trace = Rejected
	}
	return class
}

func notReadyMessage(message string) bool {
	lower := strings.ToLower(message)
	return lower == marketNotReady ||
		lower == "invalid token id" ||
		lower == "market not found" ||
		(strings.HasPrefix(lower, orderbookMissingPrefix) &&
			strings.HasSuffix(lower, orderbookMissingSuffix))
}

func errorMessage(fields map[string]json.RawMessage) string {
	if raw, ok := fields["errorMsg"]; ok && truthy(raw) {
		return pyStr(raw)
	}
	return ""
}

func versionMismatch(value json.RawMessage) bool {
	if kind(value) != '{' {
		return false
	}
	raw, ok := objectFields(value)["error"]
	if !ok || !truthy(raw) {
		return false
	}
	message := string(compact(raw))
	if kind(raw) == '"' {
		message = pyStr(raw)
	}
	return strings.Contains(message, orderVersionMismatch)
}

// foldedRejection is the reply a non-transient rejection turns into:
// {"errorMsg": <the body's "error", else the body, else the exception
// text>, "success": false}.
func foldedRejection(status int, body []byte) json.RawMessage {
	parsed := parseBody(body)
	var message string
	switch {
	case kind(parsed) == '{':
		fields := objectFields(parsed)
		if raw, ok := fields["error"]; ok && truthy(raw) {
			message = pyStr(raw)
		} else if len(fields) > 0 {
			message = string(compact(parsed))
		} else {
			message = exceptionText(status, parsed)
		}
	case truthy(parsed):
		message = pyStr(parsed)
	default:
		message = exceptionText(status, parsed)
	}
	folded, _ := json.Marshal(struct {
		ErrorMsg string `json:"errorMsg"`
		Success  bool   `json:"success"`
	}{message, false})
	return folded
}

func exceptionText(status int, parsed json.RawMessage) string {
	return "PolyApiException[status_code=" + strconv.Itoa(status) +
		", error_message=" + pyStr(parsed) + "]"
}

// parseBody reads a body the way the reply is judged: JSON when it is
// JSON, otherwise the text itself as a string value.
func parseBody(body []byte) json.RawMessage {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && json.Valid(trimmed) {
		return json.RawMessage(trimmed)
	}
	text, _ := json.Marshal(string(body))
	return text
}

func kind(value json.RawMessage) byte {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 {
		return 0
	}
	switch c := trimmed[0]; c {
	case '{', '[', '"', 't', 'f', 'n':
		return c
	default:
		return '0'
	}
}

func objectFields(value json.RawMessage) map[string]json.RawMessage {
	fields := map[string]json.RawMessage{}
	_ = json.Unmarshal(value, &fields)
	return fields
}

func isFalse(value json.RawMessage) bool {
	return kind(value) == 'f'
}

// truthy is how Python reads a JSON value in a condition.
func truthy(value json.RawMessage) bool {
	switch kind(value) {
	case '{':
		return len(objectFields(value)) > 0
	case '[':
		var items []json.RawMessage
		_ = json.Unmarshal(value, &items)
		return len(items) > 0
	case '"':
		return pyStr(value) != ""
	case 't':
		return true
	case '0':
		number, err := strconv.ParseFloat(string(bytes.TrimSpace(value)), 64)
		return err != nil || number != 0
	default:
		return false
	}
}

// pyStr is str() of a JSON value for the cases that decide anything: a
// string is itself, a number its digits. Containers come back as compact
// JSON, which Python would print in its own notation instead - close
// enough for matching, and nothing matches against them.
func pyStr(value json.RawMessage) string {
	switch kind(value) {
	case '"':
		var text string
		_ = json.Unmarshal(value, &text)
		return text
	case 't':
		return "True"
	case 'f':
		return "False"
	case 'n':
		return "None"
	default:
		return string(compact(value))
	}
}

func compact(value json.RawMessage) []byte {
	var out bytes.Buffer
	if err := json.Compact(&out, value); err != nil {
		return bytes.TrimSpace(value)
	}
	return out.Bytes()
}
