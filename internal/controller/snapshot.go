package controller

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/cadence-workflow/shard-manager/common/types"

	"github.com/dkrotx/shard-manager-demo/internal/workload"
)

// Worker states shown in the UI.
const (
	stateStarting = "starting" // process is up, not yet registered with Shard Manager
	stateRunning  = "running"
	stateDead     = "dead"     // process exited, Shard Manager has not expired it yet
	stateDraining = "draining" // Shard Manager reports DRAINING
)

type snapshot struct {
	Workers     []workerView `json:"workers"`
	Shards      []shardView  `json:"shards"`
	Balancing   bool         `json:"balancing"`
	PendingMs   int64        `json:"balancingPendingMs"`
	Imbalance   float64      `json:"imbalance"`
	Moves       []Move       `json:"moves"`
	Events      []Event      `json:"events"`
	Narration   string       `json:"narration"`
	Busy        string       `json:"busy"`
	IdleResetIn int          `json:"idleResetIn"`
	SMOK        bool         `json:"smOK"`
	NowMs       int64        `json:"now"`
}

type workerView struct {
	Index   int       `json:"index"`
	Process string    `json:"process"`
	PID     int       `json:"pid"`
	State   string    `json:"state"`
	Shards  []string  `json:"shards"`
	Load    float64   `json:"load"`
	CPU     float64   `json:"cpu"`
	CPUHist []float64 `json:"cpuHistory"`
}

type shardView struct {
	ID     string  `json:"id"`
	Owner  int     `json:"owner"`
	Target float64 `json:"target"`
	Base   float64 `json:"base"`
	Load   float64 `json:"load"`
}

func (c *Controller) snapshotLocked() snapshot {
	now := time.Now()
	s := snapshot{
		Workers:     []workerView{},
		Balancing:   c.balancing,
		Moves:       append([]Move{}, c.moves...),
		Events:      append([]Event{}, c.events...),
		Busy:        c.busy,
		IdleResetIn: -1,
		SMOK:        c.smOK,
		NowMs:       now.UnixMilli(),
	}
	if pending := c.balancingChangedAt.Add(dynamicConfigApplyDelay * time.Millisecond).Sub(now); pending > 0 {
		s.PendingMs = pending.Milliseconds()
	}
	if c.dirty && c.busy == "" {
		s.IdleResetIn = max(0, int((c.cfg.IdleReset - now.Sub(c.lastActivity)).Seconds()))
	}

	owner := map[string]int{}
	byWorker := map[int][]string{}
	if c.state != nil {
		for executorID, assigned := range c.state.ShardAssignments {
			hb := c.state.Executors[executorID]
			w := c.workerForExecutorLocked(hb)
			if w == nil {
				continue
			}
			for shard := range assigned.GetAssignedShards() {
				owner[shard] = w.index
				byWorker[w.index] = append(byWorker[w.index], shard)
			}
		}
	}

	base := workload.Baseline()
	for i := range workload.ShardCount {
		id := strconv.Itoa(i)
		s.Shards = append(s.Shards, shardView{
			ID: id, Owner: owner[id], Target: c.demand[id], Base: base[id], Load: c.shardLoadLocked(id),
		})
	}

	for _, index := range slices.Sorted(maps.Keys(c.workers)) {
		w := c.workers[index]
		shards := append([]string{}, byWorker[index]...)
		slices.SortFunc(shards, func(a, b string) int { return shardNum(a) - shardNum(b) })
		load := 0.0
		for _, shard := range shards {
			load += c.shardLoadLocked(shard)
		}
		v := workerView{
			Index:   index,
			Process: w.proc.name,
			PID:     w.proc.pid(),
			State:   c.workerStateLocked(w),
			Shards:  shards,
			Load:    math.Round(load),
			CPUHist: append([]float64{}, w.cpu...),
		}
		if len(w.cpu) > 0 {
			v.CPU = w.cpu[len(w.cpu)-1]
		}
		s.Workers = append(s.Workers, v)
	}

	s.Imbalance = imbalance(s.Workers)
	s.Narration = c.narrationLocked(s, now)
	return s
}

func (c *Controller) workerStateLocked(w *workerState) string {
	switch {
	case !w.proc.alive():
		return stateDead
	case w.joinedAt.IsZero():
		return stateStarting
	}
	if c.state != nil {
		if hb := c.state.Executors[w.executorID]; hb != nil && hb.Status != types.ExecutorStatusACTIVE {
			return stateDraining
		}
	}
	return stateRunning
}

// imbalance is max/mean of reported load across running workers: 1.0 is perfect.
func imbalance(workers []workerView) float64 {
	var total, peak float64
	n := 0
	for _, w := range workers {
		if w.State != stateRunning {
			continue
		}
		total += w.Load
		peak = max(peak, w.Load)
		n++
	}
	if n < 2 || total == 0 {
		return 1
	}
	return math.Round(peak/(total/float64(n))*100) / 100
}

// narrationLocked explains the current situation in one sentence, using only
// observed moves and state.
func (c *Controller) narrationLocked(s snapshot, now time.Time) string {
	if s.Busy != "" {
		return s.Busy
	}
	if !s.SMOK {
		return "Waiting for Shard Manager…"
	}
	if c.placing {
		return "Workers are registering. Shard Manager is placing the shards…"
	}
	for _, w := range s.Workers {
		if w.State == stateDead && len(w.Shards) > 0 {
			return fmt.Sprintf("Worker %d died. Shard Manager will notice when its heartbeat expires (2s)…", w.Index)
		}
	}

	var recent []Move
	for _, m := range s.Moves {
		if now.UnixMilli()-m.AtMs < 6000 {
			recent = append(recent, m)
		}
	}
	if len(recent) > 0 {
		count := map[string]int{}
		newWorker := 0
		for _, m := range recent {
			count[m.Reason]++
			if m.Reason == reasonNewWorker {
				newWorker = m.To
			}
		}
		also := ""
		if n := count[reasonRebalance]; n > 0 {
			also = fmt.Sprintf(", then moved %s to even out the load", plural(n, "more shard"))
		}
		switch {
		case count[reasonRecovery] > 0:
			return fmt.Sprintf("Recovery: Shard Manager reassigned %s of the dead worker to the survivors%s",
				plural(count[reasonRecovery], "shard"), also)
		case count[reasonNewWorker] > 0:
			return fmt.Sprintf("Worker %d joined: Shard Manager handed it %s%s", newWorker, plural(count[reasonNewWorker], "shard"), also)
		default:
			return fmt.Sprintf("Shard Manager moved %s to even out the load", plural(count[reasonRebalance], "shard"))
		}
	}

	if s.PendingMs > 0 {
		state := "OFF"
		if s.Balancing {
			state = "ON"
		}
		return fmt.Sprintf("Turning load balancing %s. Shard Manager picks up the new config within ~5s…", state)
	}
	busiest := 0
	var peak float64
	var busiestShards []string
	for _, w := range s.Workers {
		if w.State == stateRunning && w.Load > peak {
			busiest, peak, busiestShards = w.Index, w.Load, w.Shards
		}
	}
	switch {
	case s.Balancing && s.Imbalance > 1.2 && len(busiestShards) == 1:
		return fmt.Sprintf("Worker %d now runs only Shard %s. A single shard can't be split, so Shard Manager moved everything else away from it.",
			busiest, busiestShards[0])
	case !s.Balancing && s.Imbalance >= 1.3:
		return fmt.Sprintf("Load balancing is OFF: Worker %d does %.1fx the average work. Turn on Shard Manager!", busiest, s.Imbalance)
	case !s.Balancing:
		return "Load balancing is OFF: shards only move when a worker joins or dies. Try heating up a shard."
	case s.Imbalance <= 1.2:
		return "Balanced: every worker is close to the average load. Now try to break it!"
	default:
		return fmt.Sprintf("Shard Manager sees Worker %d overloaded and is rebalancing (moves are paced on purpose)…", busiest)
	}
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return strconv.Itoa(n) + " " + word + "s"
}
