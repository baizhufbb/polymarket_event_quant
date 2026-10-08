// Command fakevenue runs the stand-in exchange for the Python integration
// test: it prints {"url": ..., "ca_file": ..., "record_url": ...} and serves
// until its stdin closes. The door opens -open-after the start; the market
// record turns active -activate-after it, saying that moment as startDate.
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
	activateAfter := flag.Duration("activate-after", 24*time.Hour, "when the market record turns active")
	flag.Parse()
	venue, err := fakevenue.Start(*dir)
	if err != nil {
		log.Fatal(err)
	}
	defer venue.Close()
	began := time.Now()
	venue.OpenAt(began.Add(*openAfter))
	venue.ActivateAt(began.Add(*activateAfter), began.Add(*activateAfter))
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{
		"url":        venue.URL,
		"ca_file":    venue.CAFile,
		"record_url": venue.URL + fakevenue.RecordPath,
	}); err != nil {
		log.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}
