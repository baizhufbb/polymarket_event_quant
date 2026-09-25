package venue

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

// Creds are one account's L2 API credentials and the address its orders
// are signed by.
type Creds struct {
	Address    string
	APIKey     string
	Secret     string
	Passphrase string
}

// ErrSecret means an account's API secret is not base64: our own setup is
// wrong, not the venue.
var ErrSecret = errors.New("api secret is not base64")

// Headers are the L2 authentication headers of one request, built the way
// the official client builds them (py_clob_client_v2
// create_level_2_headers): an HMAC-SHA256 of timestamp + method + path +
// body, keyed by the base64url-decoded secret, with every ' in the body
// read as ", sent base64url-encoded.
func Headers(creds Creds, method, path string, body []byte, unix int64) (map[string]string, error) {
	key, err := decodeSecret(creds.Secret)
	if err != nil {
		return nil, err
	}
	timestamp := strconv.FormatInt(unix, 10)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(timestamp + method + path))
	mac.Write([]byte(strings.ReplaceAll(string(body), "'", `"`)))
	return map[string]string{
		"POLY_ADDRESS":    creds.Address,
		"POLY_SIGNATURE":  base64.URLEncoding.EncodeToString(mac.Sum(nil)),
		"POLY_TIMESTAMP":  timestamp,
		"POLY_API_KEY":    creds.APIKey,
		"POLY_PASSPHRASE": creds.Passphrase,
	}, nil
}

func decodeSecret(secret string) ([]byte, error) {
	normalized := strings.NewReplacer("+", "-", "/", "_").Replace(strings.TrimSpace(secret))
	if key, err := base64.URLEncoding.DecodeString(normalized); err == nil {
		return key, nil
	}
	if key, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(normalized, "=")); err == nil {
		return key, nil
	}
	return nil, ErrSecret
}
