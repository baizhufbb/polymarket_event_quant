package venue

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	// Gamma turns away some clients by their User-Agent (urllib's got 403
	// from the venue's edge); this is the one the bot's own lookups send.
	recordUserAgent = "polymarket-btc-bot/0.1"
	// An ask that has not come back by now is given up: a later one will
	// have the news sooner.
	recordTimeout = 3 * time.Second
)

var (
	recordMu      sync.Mutex
	recordClients = map[string]*http.Client{}
)

// RecordClient is the process-wide client for market records on Gamma,
// built on first use with the trust the order connections have.
func RecordClient(caFile string) (*http.Client, error) {
	recordMu.Lock()
	defer recordMu.Unlock()
	if client, ok := recordClients[caFile]; ok {
		return client, nil
	}
	config, err := tlsConfig(caFile)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   connectTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSClientConfig:     config,
			TLSHandshakeTimeout: connectTimeout,
			ForceAttemptHTTP2:   true,
			IdleConnTimeout:     idleTimeout,
			// The record is asked for only while a market is about to open,
			// a minute or two in every five; in between the connection sits
			// idle, and one dropped silently on the way would eat every ask
			// of the next market for the timeout. Pinged after 10 s without
			// a frame and dropped if the ping is not answered in 5 s, it is
			// dialled afresh before it is needed.
			HTTP2: &http.HTTP2Config{
				SendPingTimeout: 10 * time.Second,
				PingTimeout:     5 * time.Second,
			},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	recordClients[caFile] = client
	return client, nil
}

// AskRecord fetches a market's record. Each ask carries a parameter of its
// own and asks for no cache: Gamma's edge otherwise answers from a copy
// tens of seconds old, a 404 included.
func AskRecord(client *http.Client, record string, asked time.Time) (status int, body []byte, err error) {
	address, err := url.Parse(record)
	if err != nil {
		return 0, nil, err
	}
	query := address.Query()
	query.Set("_cb", strconv.FormatInt(asked.UnixNano(), 10))
	address.RawQuery = query.Encode()
	ctx, cancel := context.WithTimeout(context.Background(), recordTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address.String(), nil)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("User-Agent", recordUserAgent)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-cache")
	request.Header.Set("Pragma", "no-cache")
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err = io.ReadAll(io.LimitReader(response.Body, maxReplyBytes))
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, body, nil
}
