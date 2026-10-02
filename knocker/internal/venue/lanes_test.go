package venue_test

import (
	"sync"
	"testing"
	"time"

	"polymarket_event_quant/knocker/internal/fakevenue"
	"polymarket_event_quant/knocker/internal/venue"
)

var creds = venue.Creds{
	Address:    "0xSigner",
	APIKey:     "key-1",
	Secret:     "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
	Passphrase: "pass-1",
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

func TestAnOrderGoesOutOverHTTP2WithTheClientsHeaders(t *testing.T) {
	fake := start(t)
	fake.OpenAt(time.Now())
	v, err := venue.For(fake.URL, fake.CAFile)
	if err != nil {
		t.Fatal(err)
	}
	status, reply, err := v.SendOrder(v.Pick(), creds, []byte(`{"order":1}`))
	if err != nil || status != 200 {
		t.Fatalf("status %d, reply %s, err %v", status, reply, err)
	}
	seen := fake.Requests()
	if len(seen) != 1 || seen[0].Proto != 2 {
		t.Fatalf("seen %+v", seen)
	}
	header := seen[0].Header
	for _, name := range []string{"Poly_address", "Poly_signature", "Poly_timestamp", "Poly_api_key", "Poly_passphrase"} {
		if header.Get(name) == "" {
			t.Errorf("no %s header", name)
		}
	}
	if header.Get("User-Agent") != "py_clob_client_v2" || header.Get("Content-Type") != "application/json" {
		t.Errorf("headers %v", header)
	}
	if got := v.InFlight()[0]; got != 0 {
		t.Errorf("stream not given back: %d", got)
	}
}

func TestSendsFillOneConnectionBeforeSpillingOntoTheNext(t *testing.T) {
	fake := start(t)
	release := make(chan struct{})
	fake.Respond = func(int, fakevenue.Request) (fakevenue.Response, bool) {
		<-release
		return fakevenue.Response{Status: 400, Body: `{"error":"invalid token id"}`}, true
	}
	v, err := venue.For(fake.URL, fake.CAFile)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 150 {
		lane := v.Pick()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = v.SendOrder(lane, creds, []byte(`{}`))
		}()
	}
	counts := v.InFlight()
	if counts[0] != 100 || counts[1] != 50 || counts[2] != 0 {
		t.Fatalf("in flight per connection: %v", counts)
	}
	close(release)
	wg.Wait()
	for i, n := range v.InFlight() {
		if n != 0 {
			t.Errorf("connection %d still carries %d", i, n)
		}
	}
}

func TestAFailedSendGivesItsStreamBack(t *testing.T) {
	fake := start(t)
	url, ca := fake.URL, fake.CAFile
	fake.Close()
	v, err := venue.For(url, ca)
	if err != nil {
		t.Fatal(err)
	}
	status, _, err := v.SendOrder(v.Pick(), creds, []byte(`{}`))
	if err == nil || status != 0 {
		t.Fatalf("status %d err %v", status, err)
	}
	if got := v.InFlight()[0]; got != 0 {
		t.Errorf("stream not given back: %d", got)
	}
}
