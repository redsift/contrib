package dumper

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const discardedID = "discarded"

type Dumper interface {
	// Dump dumps slice b []byte to some destination and returns an id of the dump
	Dump([]byte) string
}

type discard struct{}

func (d *discard) Dump(_ []byte) string { return discardedID }

var Discard = &discard{}

type RollingDumper struct {
	sync.Mutex
	name     string
	capacity int
	cnt      int
}

func NewRollingDumper(name string, capacity int) *RollingDumper {
	return &RollingDumper{
		name:     name,
		capacity: capacity,
	}
}

// Dump dumps slice b []byte to the provided destination and returns an id of the dump.
// Dump locks the dumper while executing. Not designed for high load environments.
func (d *RollingDumper) Dump(b []byte) string {
	d.Lock()
	defer d.Unlock() // long lock is ok here as I do not expect much usage of that feature
	name := filepath.Join(os.TempDir(), fmt.Sprintf("%s%d", d.name, d.cnt%d.capacity))
	d.cnt++
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return discardedID
	}
	if n, err1 := f.Write(b); err1 == nil && n < len(b) {
		err = io.ErrShortWrite
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	if err != nil {
		os.Remove(f.Name())
		// better discard probably incomplete dump
		return discardedID
	}
	return f.Name()
}

// Dumper implements Dumper which could be used no more than N times in limited period after activation.
type LimitedDumper struct {
	sync.Mutex
	capacity uint
	exp      time.Time
	name     string
}

func NewLimitedDumper(name string) *LimitedDumper {
	return &LimitedDumper{name: name, exp: time.Now()}
}

func (d *LimitedDumper) reset(c uint, dur time.Duration) {
	d.capacity = c
	d.exp = time.Now().Add(dur)
}

// Dump dumps slice b []byte to the provided destination and returns an id of the dump.
// Dump locks the dumper while executing. Not designed for high load environments.
func (d *LimitedDumper) Dump(b []byte) string {
	d.Lock()
	defer d.Unlock()
	if time.Now().After(d.exp) || d.capacity == 0 {
		return discardedID
	}
	d.capacity--
	f, err := ioutil.TempFile("", d.name)
	if err != nil {
		return discardedID
	}
	if n, err1 := f.Write(b); err1 == nil && n < len(b) {
		err = io.ErrShortWrite
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	if err != nil {
		os.Remove(f.Name())
		// better discard probably incomplete dump
		return discardedID
	}
	return f.Name()
}

// ServeHTTP is a simple JSON endpoint that can report on or change the dumper settings.
//
// GET requests return a JSON description of the d.
// PUT requests change the b and expect a payload like:
//   {"capacity":5, "seconds":"86400"}
//
// It's perfectly safe to change the b while a program is running.
func (d *LimitedDumper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.Lock()
	defer d.Unlock()

	type errorResponse struct {
		Error string `json:"error"`
	}
	type payload struct {
		C uint  `json:"capacity"`
		D int64 `json:"seconds"`
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)

	switch r.Method {
	case "GET":
		enc.Encode(payload{
			C: d.capacity,
			D: int64(d.exp.Sub(time.Now()) / time.Second),
		})
	case "PUT":
		var req payload

		if errmess := func() string {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				return fmt.Sprintf("Request body must be well-formed JSON: %v", err)
			}
			if req.C == 0 {
				return "Must specify a capacity."
			}
			if req.D == 0 {
				return "Must specify a duration in seconds"
			}
			return ""
		}(); errmess != "" {
			w.WriteHeader(http.StatusBadRequest)
			enc.Encode(errorResponse{Error: errmess})
			return
		}

		d.reset(req.C, time.Duration(req.D)*time.Second)
		enc.Encode(req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		enc.Encode(errorResponse{
			Error: "Only GET and PUT are supported.",
		})
	}
}
