package venue

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
)

// l2_headers.json holds headers built by the official client's own
// create_level_2_headers for fixed inputs.
func TestHeadersMatchTheOfficialClientByteForByte(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/l2_headers.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Address       string            `json:"address"`
		APIKey        string            `json:"api_key"`
		APISecret     string            `json:"api_secret"`
		APIPassphrase string            `json:"api_passphrase"`
		Timestamp     int64             `json:"timestamp"`
		Method        string            `json:"method"`
		Path          string            `json:"path"`
		Body          string            `json:"body"`
		Headers       map[string]string `json:"headers"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range cases {
		creds := Creds{Address: c.Address, APIKey: c.APIKey, Secret: c.APISecret, Passphrase: c.APIPassphrase}
		got, err := Headers(creds, c.Method, c.Path, []byte(c.Body), c.Timestamp)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(c.Headers) {
			t.Errorf("%q: %d headers, want %d", c.Body, len(got), len(c.Headers))
		}
		for name, want := range c.Headers {
			if got[name] != want {
				t.Errorf("%q: %s = %q, want %q", c.Body, name, got[name], want)
			}
		}
	}
}

func TestASecretThatIsNotBase64IsOurOwnFailure(t *testing.T) {
	_, err := Headers(Creds{Secret: "not base64 !!"}, "POST", "/order", nil, 1)
	if !errors.Is(err, ErrSecret) {
		t.Fatalf("got %v", err)
	}
}
