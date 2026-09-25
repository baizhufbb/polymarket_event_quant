// Package main is the knocking engine as a C library, loaded into the bot's
// Python process (polymarket_bot/knocker.py) and called through ctypes.
//
// Every function takes and returns JSON text. A returned string is the
// caller's to free with KnockerFree. Nothing here may crash the bot: every
// call answers a panic as {"error": ...}.
package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"runtime"
	"time"
	"unsafe"

	"polymarket_event_quant/knocker/internal/knock"
	"polymarket_event_quant/knocker/internal/venue"
)

// sourceHash is set at build time to the hash of the sources the library
// was built from, so a test can tell a library that is behind its sources.
var sourceHash = "unset"

// attempts carries every reply to Python's trace writer, which collects
// them with KnockerNextAttempts.
var attempts = make(chan knock.Attempt, 1<<16)

func main() {}

//export KnockerKnock
func KnockerKnock(plan *C.char) *C.char {
	return answer(func() (any, error) {
		var p knock.Plan
		if err := json.Unmarshal([]byte(C.GoString(plan)), &p); err != nil {
			return nil, err
		}
		return knock.Run(p, func(a knock.Attempt) { attempts <- a })
	})
}

// KnockerStop has every knock in progress stop sending; each still collects
// its replies in flight and returns what registered.
//
//export KnockerStop
func KnockerStop() *C.char {
	return answer(func() (any, error) {
		knock.StopAll()
		return true, nil
	})
}

// KnockerNextAttempts returns the replies that landed since the last call,
// waiting up to timeoutMs for the first one.
//
//export KnockerNextAttempts
func KnockerNextAttempts(timeoutMs C.int) *C.char {
	return answer(func() (any, error) {
		batch := []knock.Attempt{}
		select {
		case a := <-attempts:
			batch = append(batch, a)
		case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
			return batch, nil
		}
		for len(batch) < 4096 {
			select {
			case a := <-attempts:
				batch = append(batch, a)
			default:
				return batch, nil
			}
		}
		return batch, nil
	})
}

// KnockerClassify judges a reply the way the knock does.
//
//export KnockerClassify
func KnockerClassify(reply *C.char) *C.char {
	return answer(func() (any, error) {
		value := json.RawMessage(C.GoString(reply))
		if !json.Valid(value) {
			return nil, fmt.Errorf("reply is not JSON")
		}
		return venue.FromValue(value), nil
	})
}

//export KnockerVersion
func KnockerVersion() *C.char {
	return answer(func() (any, error) {
		return map[string]string{"source_hash": sourceHash, "go": runtime.Version()}, nil
	})
}

//export KnockerFree
func KnockerFree(text *C.char) {
	C.free(unsafe.Pointer(text))
}

func answer(call func() (any, error)) (out *C.char) {
	defer func() {
		if p := recover(); p != nil {
			out = encode(map[string]string{"error": fmt.Sprintf("panic: %v", p)})
		}
	}()
	value, err := call()
	if err != nil {
		return encode(map[string]string{"error": err.Error()})
	}
	return encode(map[string]any{"ok": value})
}

func encode(value any) *C.char {
	text, err := json.Marshal(value)
	if err != nil {
		text, _ = json.Marshal(map[string]string{"error": err.Error()})
	}
	return C.CString(string(text))
}
