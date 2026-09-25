// Command fakevenue runs the stand-in exchange for the Python integration
// test: it prints {"url": ..., "ca_file": ...} and serves until its stdin
// closes. The door opens -open-after the start.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"time"

	"polymarket_event_quant/knocker/internal/fakevenue"
)

func main() {
	dir := flag.String("dir", os.TempDir(), "where to write the certificate authority")
	openAfter := flag.Duration("open-after", 300*time.Millisecond, "when the door opens")
	flag.Parse()
	venue, err := fakevenue.Start(*dir)
	if err != nil {
		log.Fatal(err)
	}
	defer venue.Close()
	venue.OpenAt(time.Now().Add(*openAfter))
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{
		"url":     venue.URL,
		"ca_file": venue.CAFile,
	}); err != nil {
		log.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}
