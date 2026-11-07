package worker

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	demandPollInterval  = 500 * time.Millisecond
	controllerLostAfter = 3 * time.Second
)

// Demand mirrors the controller's per-shard target RPS. The worker exits when
// the controller disappears so no orphan keeps burning CPU.
type Demand struct {
	url  string
	stop chan struct{}

	mu   sync.RWMutex
	rps  map[string]float64
	seen time.Time
}

func newDemand(controllerURL string) *Demand {
	return &Demand{url: controllerURL + "/api/demand", stop: make(chan struct{})}
}

func (d *Demand) Get(shardID string) float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rps[shardID]
}

func (d *Demand) Start(context.Context) error {
	if err := d.refresh(); err != nil {
		return err
	}
	go d.poll()
	return nil
}

func (d *Demand) Stop() { close(d.stop) }

func (d *Demand) refresh() error {
	client := http.Client{Timeout: time.Second}
	resp, err := client.Get(d.url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var rps map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&rps); err != nil {
		return err
	}
	d.mu.Lock()
	d.rps, d.seen = rps, time.Now()
	d.mu.Unlock()
	return nil
}

func (d *Demand) poll() {
	ticker := time.NewTicker(demandPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
		}
		if d.refresh() == nil {
			continue
		}
		d.mu.RLock()
		lost := time.Since(d.seen) > controllerLostAfter
		d.mu.RUnlock()
		if lost {
			log.Print("controller lost, exiting")
			os.Exit(1)
		}
	}
}
