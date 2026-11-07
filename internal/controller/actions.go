package controller

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/dkrotx/shard-manager-demo/internal/workload"
)

var errBusy = errors.New("the playground is resetting, try again in a few seconds")

// touchLocked records visitor activity and rejects actions during a reset.
func (c *Controller) touchLocked() error {
	if c.busy != "" {
		return errBusy
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

func (c *Controller) setBalancing(on bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.touchLocked(); err != nil {
		return err
	}
	if on == c.balancing {
		return nil
	}
	if err := writeDynamicConfig(c.dynamicConfigPath(), on); err != nil {
		return err
	}
	c.balancing, c.balancingChangedAt = on, time.Now()
	state := "OFF"
	if on {
		state = "ON"
	}
	c.addEventLocked("action", "Load balancing turned "+state+" (dynamic config moveBudgetProportion)")
	return nil
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

// reset restores the baseline: fresh workers 1..N, default demand, balancing
// OFF. It waits for Shard Manager to forget the old executors and for the
// balancing config to be applied, so the new workers start from a clean slate.
func (c *Controller) reset(reason string) error {
	c.mu.Lock()
	if c.busy != "" {
		c.mu.Unlock()
		return errBusy
	}
	c.busy = "Resetting the playground…"
	c.demand = workload.Baseline()
	if c.balancing {
		if err := writeDynamicConfig(c.dynamicConfigPath(), false); err != nil {
			c.busy = ""
			c.mu.Unlock()
			return err
		}
		c.balancing, c.balancingChangedAt = false, time.Now()
	}
	workers := slices.Collect(maps.Values(c.workers))
	configAppliedAt := c.balancingChangedAt.Add(dynamicConfigApplyDelay * time.Millisecond)
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func() { defer wg.Done(); w.proc.stop(3 * time.Second) }()
	}
	wg.Wait()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		empty := c.state != nil && len(c.state.Executors) == 0 && len(c.workers) == 0
		c.mu.Unlock()
		if empty {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(time.Until(configAppliedAt))

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
	c.busy = ""
	c.dirty = false
	c.lastActivity = time.Now()
	c.mu.Unlock()
	return nil
}
