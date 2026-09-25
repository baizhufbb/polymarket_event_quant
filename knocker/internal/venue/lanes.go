package venue

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultBase is the exchange's order API.
	DefaultBase = "https://clob.polymarket.com"
	// Orders are spread over this many HTTP/2 connections, each its own
	// client, filled one at a time: a normal market stays on one busy
	// connection, the rest take the overflow of a slow spell. The venue
	// allows StreamsPerLane requests at once on a connection
	// (SETTINGS_MAX_CONCURRENT_STREAMS, measured 2026-09-01).
	Lanes          = 15
	StreamsPerLane = 100

	orderPath = "/order"
	timePath  = "/time"
	// A TLS handshake to the venue's edge takes about 35 ms; a second was
	// too short while the venue was slow (run38 logged ConnectTimeout four
	// times in the seven seconds before a door).
	connectTimeout = 10 * time.Second
	idleTimeout    = 300 * time.Second
	// A connection this long without a single frame is pinged, and dropped
	// if the ping is not answered: only a dead connection goes this quiet
	// while replies are owed. A slow reply is waited for, never cancelled -
	// every send for a market is the same signed order, so a late one earns
	// "not ready" or "duplicated", never a second order.
	silence       = 30 * time.Second
	pingTimeout   = 15 * time.Second
	warmInterval  = 60 * time.Second
	maxReplyBytes = 1 << 20
)

// Venue is the process-wide set of connections to one exchange endpoint.
type Venue struct {
	base  string
	lanes []*Lane

	warmMu   sync.Mutex
	lastWarm time.Time
}

// Lane is one HTTP/2 client and the requests it is carrying right now.
type Lane struct {
	client   *http.Client
	inFlight atomic.Int32
}

var (
	venuesMu sync.Mutex
	venues   = map[string]*Venue{}
)

// For returns the connections to base, built on first use. caFile, when
// set, is the only certificate authority trusted (the default is the
// system's).
func For(base, caFile string) (*Venue, error) {
	if base == "" {
		base = DefaultBase
	}
	key := base + "\x00" + caFile
	venuesMu.Lock()
	defer venuesMu.Unlock()
	if v, ok := venues[key]; ok {
		return v, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificate in %s", caFile)
		}
		config.RootCAs = pool
	}
	v := &Venue{base: base}
	for range Lanes {
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   connectTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSClientConfig:     config.Clone(),
			TLSHandshakeTimeout: connectTimeout,
			ForceAttemptHTTP2:   true,
			IdleConnTimeout:     idleTimeout,
			HTTP2: &http.HTTP2Config{
				SendPingTimeout: silence,
				PingTimeout:     pingTimeout,
			},
		}
		v.lanes = append(v.lanes, &Lane{client: &http.Client{
			Transport: transport,
			// A redirect is an answer, not somewhere to send the order and
			// its credentials again; the Python client never followed one.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}})
	}
	venues[key] = v
	return v, nil
}

// Pick takes a stream on the first connection that has one to spare, or
// on the least loaded one if none has. Release gives it back.
func (v *Venue) Pick() *Lane {
	chosen := v.lanes[0]
	for _, lane := range v.lanes {
		carrying := lane.inFlight.Load()
		if carrying < StreamsPerLane {
			chosen = lane
			break
		}
		if carrying < chosen.inFlight.Load() {
			chosen = lane
		}
	}
	chosen.inFlight.Add(1)
	return chosen
}

// InFlight is how many requests each connection is carrying.
func (v *Venue) InFlight() []int {
	counts := make([]int, len(v.lanes))
	for i, lane := range v.lanes {
		counts[i] = int(lane.inFlight.Load())
	}
	return counts
}

// SendOrder posts one signed order on lane and gives the lane's stream
// back when the reply is in. Status 0 means no reply came back; err then
// says why. ErrSecret is our own failure and is returned before anything
// is sent.
func (v *Venue) SendOrder(lane *Lane, creds Creds, body []byte) (status int, reply []byte, err error) {
	defer lane.inFlight.Add(-1)
	headers, err := Headers(creds, http.MethodPost, orderPath, body, time.Now().Unix())
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequest(http.MethodPost, v.base+orderPath, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	for name, value := range headers {
		request.Header[name] = []string{value}
	}
	request.Header.Set("User-Agent", "py_clob_client_v2")
	request.Header.Set("Accept", "*/*")
	request.Header.Set("Content-Type", "application/json")
	response, err := lane.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	reply, err = io.ReadAll(io.LimitReader(response.Body, maxReplyBytes))
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, reply, nil
}

// Warm dials every connection ahead of the burst so the first sends at a
// door do not pay a TLS handshake. It returns at once and does nothing if
// it ran in the last minute: a market handed back by signing asks again on
// every loop tick.
func (v *Venue) Warm() {
	v.warmMu.Lock()
	if !v.lastWarm.IsZero() && time.Since(v.lastWarm) < warmInterval {
		v.warmMu.Unlock()
		return
	}
	v.lastWarm = time.Now()
	v.warmMu.Unlock()
	for _, lane := range v.lanes {
		go func() {
			defer func() { _ = recover() }()
			response, err := lane.client.Get(v.base + timePath)
			if err == nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxReplyBytes))
				response.Body.Close()
			}
		}()
	}
}

// IsSecretError reports whether err is our own credentials failing.
func IsSecretError(err error) bool {
	return errors.Is(err, ErrSecret)
}
