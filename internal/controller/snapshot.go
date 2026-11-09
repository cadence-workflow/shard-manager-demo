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
	Workers        []workerView `json:"workers"`
	Shards         []shardView  `json:"shards"`
	ManagerOn      bool         `json:"managerOn"`
	PendingMs      int64        `json:"managerPendingMs"`
	TurningOff     bool         `json:"managerTurningOff"`
	Imbalance      float64      `json:"imbalance"`
	Moves          []Move       `json:"moves"`
	Events         []Event      `json:"events"`
	Narration      string       `json:"narration"`
	Busy           string       `json:"busy"`
	IdleResetIn    int          `json:"idleResetIn"`
	SMOK           bool         `json:"smOK"`
	SMExecutors    int          `json:"smExecutors"`
	MaxWorkers     int          `json:"maxWorkers"`
	HeartbeatTTLMs int64        `json:"heartbeatTTLMs"`
	NowMs          int64        `json:"now"`
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
	// LastHeartbeatMs is when Shard Manager last heard from the worker, or 0 if
	// Shard Manager does not know it.
	LastHeartbeatMs int64 `json:"lastHeartbeat"`
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
		Workers:        []workerView{},
		ManagerOn:      c.managerOn,
		TurningOff:     c.turningOff,
		Moves:          append([]Move{}, c.moves...),
		Events:         append([]Event{}, c.events...),
		Busy:           c.busy,
		IdleResetIn:    -1,
		SMOK:           c.smOK,
		MaxWorkers:     maxWorkers,
		HeartbeatTTLMs: heartbeatTTL.Milliseconds(),
		NowMs:          now.UnixMilli(),
	}
	if c.state != nil {
		s.SMExecutors = len(c.state.Executors)
	}
	// A restart for switching is expected, not an outage.
	if c.restarting {
		s.SMOK = true
	}
	if c.restarting && c.managerOn {
		s.PendingMs = resumeDelay.Milliseconds()
	} else if pending := c.managerChangedAt.Add(resumeDelay).Sub(now); pending > 0 {
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
			ID: id, Owner: owner[id], Target: c.demand[id], Base: base[id], Load: c.shardLoadLocked(id, owner[id]),
		})
	}

	for _, index := range slices.Sorted(maps.Keys(c.workers)) {
		w := c.workers[index]
		shards := append([]string{}, byWorker[index]...)
		slices.SortFunc(shards, func(a, b string) int { return shardNum(a) - shardNum(b) })
		load := 0.0
		for _, shard := range shards {
			load += c.shardLoadLocked(shard, index)
		}
		v := workerView{
			Index:           index,
			Process:         w.proc.name,
			PID:             w.proc.pid(),
			State:           c.workerStateLocked(w),
			Shards:          shards,
			Load:            math.Round(load),
			CPUHist:         append([]float64{}, w.cpu...),
			LastHeartbeatMs: c.lastHeartbeatLocked(w),
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

// lastHeartbeatLocked returns when Shard Manager last heard from the worker, or 0
// if Shard Manager does not know it.
func (c *Controller) lastHeartbeatLocked(w *workerState) int64 {
	if c.state == nil || w.executorID == "" {
		return 0
	}
	hb := c.state.Executors[w.executorID]
	if hb == nil {
		return 0
	}
	return hb.GetLastHeartbeat().UnixMilli()
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

const (
	// maxImbalance caps the ratio when the quietest worker has (almost) no load.
	maxImbalance = 10
	// balancedImbalance is what load balancing reliably reaches with the baseline workload.
	balancedImbalance = 1.3
)

// imbalance is the busiest running worker's load divided by the quietest's:
// 1.0 is perfect.
func imbalance(workers []workerView) float64 {
	busiest, quietest := extremes(workers)
	if busiest == nil || busiest == quietest || busiest.Load == 0 {
		return 1
	}
	if quietest.Load*maxImbalance <= busiest.Load {
		return maxImbalance
	}
	return math.Round(busiest.Load/quietest.Load*100) / 100
}

// extremes returns the busiest and quietest running workers that own shards,
// or nil if there are none. A worker that just registered has no shards yet.
func extremes(workers []workerView) (busiest, quietest *workerView) {
	for i := range workers {
		w := &workers[i]
		if w.State != stateRunning || len(w.Shards) == 0 {
			continue
		}
		if busiest == nil || w.Load > busiest.Load {
			busiest = w
		}
		if quietest == nil || w.Load < quietest.Load {
			quietest = w
		}
	}
	return busiest, quietest
}

// narrationLocked explains the current situation in one sentence, using only
// observed moves and state.
func (c *Controller) narrationLocked(s snapshot, now time.Time) string {
	if s.Busy != "" {
		return s.Busy
	}
	if c.turningOff {
		return "Turning off: the shards go back to being placed by count, without looking at load…"
	}
	if c.restarting {
		return "Turning on Shard Manager…"
	}
	if !s.SMOK {
		return "Waiting for Shard Manager…"
	}
	if !s.ManagerOn {
		return offNarration(s, now)
	}
	if c.placing {
		return "Workers are registering. Shard Manager is placing the shards…"
	}
	if s.PendingMs > 0 {
		return "Shard Manager is on. It looks at each worker's load and starts moving shards…"
	}
	for _, w := range s.Workers {
		if w.State == stateDead && len(w.Shards) > 0 {
			return fmt.Sprintf("Worker %d was killed. Its heartbeats stopped, so Shard Manager moves its shards once the %s TTL passes.", w.Index, heartbeatTTL)
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
			return fmt.Sprintf("Shard Manager moved %s from the dead worker to the others%s.",
				plural(count[reasonRecovery], "shard"), also)
		case count[reasonNewWorker] > 0:
			return fmt.Sprintf("Worker %d joined, and Shard Manager gave it %s%s.", newWorker, plural(count[reasonNewWorker], "shard"), also)
		default:
			return fmt.Sprintf("Shard Manager moved %s to even out the load.", plural(count[reasonRebalance], "shard"))
		}
	}

	busiest, _ := extremes(s.Workers)
	if busiest == nil {
		return ""
	}
	switch {
	case s.Imbalance > balancedImbalance && len(busiest.Shards) == 1:
		return fmt.Sprintf("Worker %d now runs only shard %s. One shard can't be split, so Shard Manager moved the rest away from it.",
			busiest.Index, busiest.Shards[0])
	case s.Imbalance <= balancedImbalance:
		return "Balanced. Every worker does about the same work."
	default:
		return fmt.Sprintf("Worker %d is overloaded. Shard Manager moves shards a few at a time.", busiest.Index)
	}
}

// offNarration explains placement without Shard Manager: by count only.
func offNarration(s snapshot, now time.Time) string {
	for _, w := range s.Workers {
		if w.State == stateDead && len(w.Shards) > 0 {
			return fmt.Sprintf("Worker %d was killed. Once its %s heartbeat TTL passes, its shards are spread over the others by count.",
				w.Index, heartbeatTTL)
		}
	}
	for _, w := range s.Workers {
		if w.State == stateStarting {
			return fmt.Sprintf("Worker %d is starting. It gets an equal number of shards, whatever their load.", w.Index)
		}
	}
	recovered, joined, joiner := 0, 0, 0
	for _, m := range s.Moves {
		if now.UnixMilli()-m.AtMs >= 6000 {
			continue
		}
		switch m.Reason {
		case reasonRecovery:
			recovered++
		case reasonNewWorker:
			joined, joiner = joined+1, m.To
		}
	}
	switch {
	case recovered > 0:
		return fmt.Sprintf("The dead worker's %s were spread over the others by count, whatever their load.", plural(recovered, "shard"))
	case joined > 0:
		return fmt.Sprintf("Worker %d joined and got %s by count, whatever their load.", joiner, plural(joined, "shard"))
	}
	busiest, quietest := extremes(s.Workers)
	if busiest != nil && s.Imbalance >= 1.5 {
		ratio := fmt.Sprintf("%.1f×", s.Imbalance)
		if s.Imbalance >= maxImbalance {
			ratio = fmt.Sprintf("over %d×", maxImbalance)
		}
		return fmt.Sprintf("Shards are placed by count, not load: Worker %d does %s the work of Worker %d.", busiest.Index, ratio, quietest.Index)
	}
	return "Shard Manager is off. Shards are placed by count, not load."
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return strconv.Itoa(n) + " " + word + "s"
}
