package controller

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/dkrotx/shard-manager-demo/internal/workload"
)

var (
	errBusy      = errors.New("the playground is resetting, try again in a few seconds")
	errSwitching = errors.New("Shard Manager is switching, try again in a moment")
)

// touchLocked records visitor activity and rejects actions during a reset or
// while Shard Manager is switching.
func (c *Controller) touchLocked() error {
	if c.busy != "" {
		return errBusy
	}
	if c.turningOff || c.restarting {
		return errSwitching
	}
	c.dirty = true
	c.lastActivity = time.Now()
	return nil
}

// heat toggles a shard between its baseline and HeatFactor times baseline.
func (c *Controller) heat(shard string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.touchLocked(); err != nil {
		return err
	}
	base, ok := workload.Baseline()[shard]
	if !ok {
		return fmt.Errorf("unknown shard %q", shard)
	}
	current := c.demand[shard]
	if current > base {
		c.demand[shard] = base
		c.addEventLocked("action", fmt.Sprintf("Shard %s cooled down: %.0f → %.0f RPS", shard, current, base))
		return nil
	}

	hot := workload.Hot(base)
	total := 0.0
	for _, rps := range c.demand {
		total += rps
	}
	if total-current+hot > workload.MaxTotalRPS {
		return errors.New("the machine is hot enough already: cool a shard down first")
	}
	c.demand[shard] = hot
	c.addEventLocked("action", fmt.Sprintf("Shard %s heated up: %.0f → %.0f RPS", shard, current, hot))
	return nil
}

// kill sends SIGKILL: the worker gets no chance to drain, which is a real crash.
func (c *Controller) kill(index int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.touchLocked(); err != nil {
		return err
	}
	w := c.workers[index]
	if w == nil || !w.proc.alive() {
		return fmt.Errorf("worker %d is not running", index)
	}
	if c.aliveWorkersLocked() <= 1 {
		return errors.New("keep at least one worker alive")
	}
	w.killed = true
	w.proc.signal(syscall.SIGKILL)
	c.addEventLocked("action", fmt.Sprintf("Worker %d killed with SIGKILL (pid %d)", index, w.proc.pid()))
	return nil
}

func (c *Controller) add() error {
	c.mu.Lock()
	err := c.touchLocked()
	if err == nil && c.aliveWorkersLocked() >= maxWorkers {
		err = fmt.Errorf("%d workers is the maximum", maxWorkers)
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return c.spawnWorker()
}

// setManager switches between the two ways the playground runs the real
// shard-manager-server. On, it balances by load. Off, load balancing is
// disabled, so it only places shards by count: a new worker gets an equal
// share and a dead worker's shards are spread evenly, whatever their load.
// The UI shows "off" as Shard Manager not being there at all.
//
// Turning off also wipes the namespace, so Shard Manager forgets the balance
// it reached and places all shards by count again.
func (c *Controller) setManager(on bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.touchLocked(); err != nil {
		return err
	}
	if on == c.managerOn {
		return nil
	}
	c.restarting = true
	if on {
		c.managerOn, c.managerChangedAt = true, time.Now()
		c.addEventLocked("action", "Turning Shard Manager on: it restarts with load balancing enabled")
	} else {
		c.turningOff, c.managerChangedAt = true, time.Time{}
		c.addEventLocked("action", "Turning Shard Manager off: it restarts with load balancing disabled and forgets its placement")
	}
	go c.switchManager(on)
	return nil
}

// switchManager restarts shard-manager-server with the new config. Turning
// off then wipes the namespace; as the workers re-register, Shard Manager
// places the shards by count, and the end of that placement completes it.
func (c *Controller) switchManager(on bool) {
	err := c.restartServer(on)
	if err == nil && !on {
		err = c.resetNamespace()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.restarting = false
	switch {
	case err != nil:
		log.Printf("switch Shard Manager: %v", err)
		c.managerOn, c.turningOff = !on, false
		c.addEventLocked("system", "Switching Shard Manager failed: "+err.Error())
	case on:
		c.managerChangedAt = time.Now()
		c.addEventLocked("action", fmt.Sprintf("Shard Manager turned ON: it balances by load (pid %d)", c.server.pid()))
	default:
		c.placing, c.placingUntil, c.lastPlacingMove = true, time.Now().Add(10*time.Second), time.Time{}
	}
}

func (c *Controller) aliveWorkersLocked() int {
	n := 0
	for _, w := range c.workers {
		if w.proc.alive() {
			n++
		}
	}
	return n
}

// reset starts restoring the baseline in the background: fresh workers 1..N
// placed by count, default demand, Shard Manager off. The initial placement
// ends the reset.
func (c *Controller) reset(reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.busy != "" {
		return errBusy
	}
	if c.turningOff || c.restarting {
		return errSwitching
	}
	c.busy = "Resetting the playground…"
	c.demand = workload.Baseline()
	c.managerOn, c.managerChangedAt = false, time.Time{}
	workers := slices.Collect(maps.Values(c.workers))
	go func() {
		if err := c.finishReset(reason, workers); err != nil {
			log.Print(err)
		}
	}()
	return nil
}

func (c *Controller) finishReset(reason string, workers []*workerState) error {
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func() { defer wg.Done(); w.proc.stop(3 * time.Second) }()
	}
	wg.Wait()

	err := c.restartServer(false)
	if err == nil {
		err = c.resetNamespace()
	}
	if err != nil {
		c.mu.Lock()
		c.busy = ""
		c.mu.Unlock()
		return fmt.Errorf("reset: %w", err)
	}

	c.mu.Lock()
	c.workers = map[int]*workerState{}
	c.lastOwner = map[string]int{}
	c.moves = nil
	c.events = nil
	c.nextIndex = 1
	c.addEventLocked("system", "Reset: "+reason)
	c.mu.Unlock()

	c.spawnBaseline()

	c.mu.Lock()
	c.dirty = false
	c.lastActivity = time.Now()
	c.mu.Unlock()
	return nil
}
