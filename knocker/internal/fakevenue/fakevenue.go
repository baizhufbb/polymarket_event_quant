// Package fakevenue is a stand-in exchange for tests: HTTPS over HTTP/2,
// an order endpoint whose door opens at a set time, a market record (as
// Gamma keeps it) that turns active at another, and a record of every
// request it saw.
package fakevenue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RecordPath is where the stand-in keeps its one market record.
const RecordPath = "/markets/slug/test"

// Request is one order as the venue saw it.
type Request struct {
	Arrived time.Time
	Proto   int
	Header  http.Header
	Body    string
}

// Response is what the venue answers; ok false means "use the default".
type Response struct {
	Status   int
	Body     string
	Delay    time.Duration
	Location string
}

// Venue is a running stand-in exchange.
type Venue struct {
	URL    string
	CAFile string

	server *httptest.Server
	mu     sync.Mutex
	opens  time.Time
	orders map[string]string
	seen   []Request
	// The record turns active at activates, saying startDate; asks is
	// every ask for it.
	activates time.Time
	startDate time.Time
	asks      []Request
	// Respond, when set, answers instead of the door logic; returning
	// ok=false falls back to it.
	Respond func(n int, r Request) (Response, bool)
	// RespondRecord, when set, answers an ask for the record instead;
	// returning ok=false falls back to the record as it stands.
	RespondRecord func(n int, r Request) (Response, bool)
}

// Start runs a venue whose door is closed until Open is called. The
// certificate authority to trust is written to dir.
func Start(dir string) (*Venue, error) {
	far := time.Now().Add(24 * time.Hour)
	v := &Venue{orders: map[string]string{}, opens: far, activates: far}
	mux := http.NewServeMux()
	mux.HandleFunc("/order", v.order)
	mux.HandleFunc(RecordPath, v.record)
	mux.HandleFunc("/time", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("1790400000"))
	})
	v.server = httptest.NewUnstartedServer(mux)
	v.server.EnableHTTP2 = true
	v.server.StartTLS()
	v.URL = v.server.URL
	v.CAFile = filepath.Join(dir, "fakevenue-ca.pem")
	block := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: v.server.Certificate().Raw})
	if err := os.WriteFile(v.CAFile, block, 0o600); err != nil {
		v.server.Close()
		return nil, err
	}
	return v, nil
}

// OpenAt sets when the door opens.
func (v *Venue) OpenAt(t time.Time) {
	v.mu.Lock()
	v.opens = t
	v.mu.Unlock()
}

// ActivateAt sets when the market record turns active, and the startDate
// it then says.
func (v *Venue) ActivateAt(at, startDate time.Time) {
	v.mu.Lock()
	v.activates = at
	v.startDate = startDate
	v.mu.Unlock()
}

// Requests is every order seen so far, in arrival order.
func (v *Venue) Requests() []Request {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]Request(nil), v.seen...)
}

// Asks is every ask for the market record so far, in arrival order.
func (v *Venue) Asks() []Request {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]Request(nil), v.asks...)
}

// record answers as Gamma does for a market: inactive with no startDate
// until it turns, then active with it.
func (v *Venue) record(w http.ResponseWriter, r *http.Request) {
	request := Request{Arrived: time.Now(), Proto: r.ProtoMajor, Header: r.Header.Clone(), Body: r.URL.RawQuery}
	v.mu.Lock()
	n := len(v.asks)
	v.asks = append(v.asks, request)
	activates, startDate := v.activates, v.startDate
	respond := v.RespondRecord
	v.mu.Unlock()

	if respond != nil {
		if answer, ok := respond(n, request); ok {
			if answer.Delay > 0 {
				time.Sleep(answer.Delay)
			}
			w.WriteHeader(answer.Status)
			_, _ = w.Write([]byte(answer.Body))
			return
		}
	}
	fields := map[string]any{"slug": "test", "active": false, "createdAt": "2026-10-07T16:27:11.8433Z"}
	if !request.Arrived.Before(activates) {
		fields["active"] = true
		fields["acceptingOrders"] = true
		fields["startDate"] = startDate.UTC().Format("2006-01-02T15:04:05.000000Z")
	}
	body, _ := json.Marshal(fields)
	_, _ = w.Write(body)
}

// Close stops the venue.
func (v *Venue) Close() {
	v.server.CloseClientConnections()
	v.server.Close()
}

// OrderID is the id the venue gives a body: the same body, the same order.
func OrderID(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "0x" + hex.EncodeToString(sum[:])
}

func (v *Venue) order(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	request := Request{Arrived: time.Now(), Proto: r.ProtoMajor, Header: r.Header.Clone(), Body: string(body)}
	v.mu.Lock()
	n := len(v.seen)
	v.seen = append(v.seen, request)
	opens := v.opens
	respond := v.Respond
	v.mu.Unlock()

	if respond != nil {
		if answer, ok := respond(n, request); ok {
			if answer.Delay > 0 {
				time.Sleep(answer.Delay)
			}
			if answer.Location != "" {
				w.Header().Set("Location", answer.Location)
			}
			w.WriteHeader(answer.Status)
			_, _ = w.Write([]byte(answer.Body))
			return
		}
	}
	if request.Arrived.Before(opens) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid token id"}`))
		return
	}
	id := OrderID(request.Body)
	v.mu.Lock()
	_, known := v.orders[request.Body]
	v.orders[request.Body] = id
	v.mu.Unlock()
	if known {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"order ` + id + ` is invalid. duplicated."}`))
		return
	}
	_, _ = w.Write([]byte(`{"orderID":"` + id + `","status":"live","success":true}`))
}
