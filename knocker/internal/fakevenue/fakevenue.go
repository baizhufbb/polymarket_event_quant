// Package fakevenue is a stand-in exchange for tests: HTTPS over HTTP/2,
// an order endpoint whose door opens at a set time, and a record of every
// request it saw.
package fakevenue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"time"
)

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
	// Respond, when set, answers instead of the door logic; returning
	// ok=false falls back to it.
	Respond func(n int, r Request) (Response, bool)
}

// Start runs a venue whose door is closed until Open is called. The
// certificate authority to trust is written to dir.
func Start(dir string) (*Venue, error) {
	v := &Venue{orders: map[string]string{}, opens: time.Now().Add(24 * time.Hour)}
	mux := http.NewServeMux()
	mux.HandleFunc("/order", v.order)
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

// Requests is every order seen so far, in arrival order.
func (v *Venue) Requests() []Request {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]Request(nil), v.seen...)
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
