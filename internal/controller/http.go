package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dkrotx/shard-manager-demo/internal/workload"
	"github.com/dkrotx/shard-manager-demo/web"
)

func (c *Controller) startHTTP() error {
	mux := http.NewServeMux()
	static := http.FileServerFS(web.FS)
	// Embedded files carry no modification time, so browsers would otherwise keep a stale UI across rebuilds.
	mux.Handle("GET /", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		static.ServeHTTP(w, r)
	}))
	mux.HandleFunc("GET /api/demand", c.handleDemand)
	mux.HandleFunc("POST /api/demand", c.handleDemand)
	mux.HandleFunc("GET /api/stream", c.handleStream)
	mux.HandleFunc("POST /api/heat", c.action(func(r *http.Request) error { return c.heat(r.FormValue("shard")) }))
	mux.HandleFunc("POST /api/kill", c.action(func(r *http.Request) error {
		index, _ := strconv.Atoi(r.FormValue("worker"))
		return c.kill(index)
	}))
	mux.HandleFunc("POST /api/add", c.action(func(*http.Request) error { return c.add() }))
	mux.HandleFunc("POST /api/manager", c.action(func(r *http.Request) error {
		return c.setManager(r.FormValue("on") == "true")
	}))
	mux.HandleFunc("POST /api/reset", c.action(func(*http.Request) error { return c.reset("requested by a visitor") }))

	listener, err := net.Listen("tcp", c.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", c.cfg.Listen, err)
	}
	c.http = &http.Server{Handler: mux}
	go func() {
		if err := c.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
		}
	}()
	return nil
}

func (c *Controller) action(fn func(*http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(r); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleDemand is polled by workers: it is the workload generator's output.
// Workers post the load of the shards they run with every poll.
func (c *Controller) handleDemand(w http.ResponseWriter, r *http.Request) {
	var report workload.Report
	if r.Method == http.MethodPost {
		_ = json.NewDecoder(r.Body).Decode(&report)
	}
	c.mu.Lock()
	if wk := c.workers[report.Worker]; wk != nil && wk.proc.pid() == report.PID {
		wk.loads, wk.loadsAt = report.Loads, time.Now()
	}
	data, _ := json.Marshal(c.demand)
	c.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

// handleStream pushes every snapshot to the browser as Server-Sent Events.
func (c *Controller) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	updates := c.hub.subscribe()
	defer c.hub.unsubscribe(updates)
	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-updates:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// hub fans snapshots out to SSE subscribers; slow subscribers skip frames.
type hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func newHub() *hub { return &hub{subs: map[chan []byte]struct{}{}} }

func (h *hub) subscribe() chan []byte {
	ch := make(chan []byte, 1)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

func (h *hub) broadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- data:
		default:
		}
	}
}
