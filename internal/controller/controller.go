// Package controller runs the playground: it owns process lifecycles and the
// workload demand table, and turns real Shard Manager state into UI snapshots.
// It never decides shard placement.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/fx"

	"github.com/cadence-workflow/shard-manager/client/sharddistributor"
	"github.com/cadence-workflow/shard-manager/common/types"

	"github.com/dkrotx/shard-manager-demo/internal/smclient"
	"github.com/dkrotx/shard-manager-demo/internal/workload"
)

const (
	// observeInterval is several times faster than the workers' 2s heartbeat,
	// so the UI sees every heartbeat arrive.
	observeInterval  = 250 * time.Millisecond
	cpuInterval      = time.Second
	cpuHistoryLen    = 60
	joinedRecentlyMs = 10_000
	maxMoves         = 30
	maxEvents        = 40
	maxWorkers       = 8

	// heartbeatTTL must match shardDistribution.process.heartbeatTTL in
	// config/development.yaml. It is only used to explain what Shard Manager does.
	heartbeatTTL = 4 * time.Second
)

// Config is set from command-line flags.
type Config struct {
	RunDir    string
	ServerBin string
	WorkerBin string
	EtcdBin   string
	EtcdAddr  string
	SMAddr    string
	Listen    string
	Workers   int
	IdleReset time.Duration
}

type workerState struct {
	index      int
	proc       *proc
	killed     bool
	executorID string
	joinedAt   time.Time
	// baseline workers got their shards in the initial placement, so later
	// moves to them are not "a new worker joined".
	baseline bool

	cpu        []float64
	lastCPU    time.Duration
	lastSample time.Time

	// loads is what the worker itself last reported running, per shard.
	loads   map[string]float64
	loadsAt time.Time
}

// Event is a user action or an observed lifecycle change.
type Event struct {
	AtMs int64  `json:"t"`
	Text string `json:"text"`
	Kind string `json:"kind"`
}

type Controller struct {
	cfg    Config
	procs  *procs
	sm     sharddistributor.Client
	hub    *hub
	cancel context.CancelFunc
	http   *http.Server

	// server is the shard-manager-server process. "Off" runs it with load
	// balancing disabled, so it places shards by count only; switching
	// restarts it, because a running server takes up to 5s to read config.
	server *proc

	mu               sync.Mutex
	demand           map[string]float64
	workers          map[int]*workerState
	nextIndex        int
	managerOn        bool
	managerChangedAt time.Time
	lastOwner        map[string]int
	moves            []Move
	events           []Event
	busy             string
	dirty            bool
	lastActivity     time.Time
	smOK             bool
	state            *types.GetFullNamespaceStateResponse

	// While the baseline workers register, Shard Manager hands shards from the
	// first worker to the next ones. Those moves are summarised as one
	// "initial placement" event instead of filling the moves list.
	// lastPlacingMove is zero until the placement moved a shard.
	placing         bool
	placingUntil    time.Time
	lastPlacingMove time.Time
	// turningOff lasts from the visitor turning Shard Manager off until the
	// count-based placement after the namespace reset is done.
	turningOff bool
	// restarting is set while shard-manager-server restarts with new config.
	restarting bool
}

// Module runs the controller inside an fx app next to smclient.Module.
var Module = fx.Module("controller",
	fx.Provide(New),
	fx.Invoke(func(*Controller) {}),
)

func New(lc fx.Lifecycle, cfg Config, sm sharddistributor.Client) *Controller {
	c := &Controller{
		cfg:          cfg,
		sm:           sm,
		hub:          newHub(),
		demand:       workload.Baseline(),
		workers:      map[int]*workerState{},
		nextIndex:    1,
		lastOwner:    map[string]int{},
		lastActivity: time.Now(),
	}
	lc.Append(fx.StartStopHook(c.start, c.stop))
	return c
}

// start brings up etcd, the server, the UI and the baseline workers.
func (c *Controller) start() error {
	var err error
	if c.procs, err = newProcs(c.cfg.RunDir); err != nil {
		return err
	}
	if err := c.startInfrastructure(); err != nil {
		c.stop()
		return err
	}
	if err := c.startHTTP(); err != nil {
		c.stop()
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.loop(ctx, observeInterval, c.observe)
	go c.loop(ctx, cpuInterval, c.sampleCPU)
	go c.loop(ctx, time.Second, c.checkIdle)

	c.addEvent("system", "Playground started")
	c.mu.Lock()
	c.busy = "Starting Shard Manager and workers…"
	c.mu.Unlock()
	c.spawnBaseline()
	log.Printf("playground ready: http://%s", c.cfg.Listen)
	return nil
}

func (c *Controller) startInfrastructure() error {
	if c.cfg.EtcdBin != "" {
		etcdBin, err := exec.LookPath(c.cfg.EtcdBin)
		if err != nil {
			return fmt.Errorf("etcd binary %q not found; install etcd or run `docker compose up -d etcd` and pass -etcd-bin=''", c.cfg.EtcdBin)
		}
		dataDir := filepath.Join(c.cfg.RunDir, "etcd")
		if err := os.RemoveAll(dataDir); err != nil {
			return err
		}
		url := "http://" + c.cfg.EtcdAddr
		if _, err := c.procs.start("playground-etcd", etcdBin, ".",
			"--data-dir", dataDir,
			"--listen-client-urls", url, "--advertise-client-urls", url,
			"--listen-peer-urls", "http://127.0.0.1:2380",
		); err != nil {
			return err
		}
	}
	if err := waitForTCP(c.cfg.EtcdAddr, 15*time.Second); err != nil {
		return fmt.Errorf("etcd: %w", err)
	}

	// The playground starts with Shard Manager "off": placement by count only.
	if err := writeDynamicConfig(c.dynamicConfigPath(), false); err != nil {
		return err
	}
	server, err := c.startServer()
	if err != nil {
		return err
	}
	c.server = server
	return c.waitForServer(30 * time.Second)
}

// restartServer restarts shard-manager-server with load balancing on or off.
// Assignments live in etcd, and workers keep their shards while it is down.
func (c *Controller) restartServer(balancing bool) error {
	if err := writeDynamicConfig(c.dynamicConfigPath(), balancing); err != nil {
		return err
	}
	c.server.stop(3 * time.Second)
	server, err := c.startServer()
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.server = server
	c.mu.Unlock()
	return c.waitForServer(15 * time.Second)
}

// resetNamespace makes Shard Manager forget every executor and assignment.
func (c *Controller) resetNamespace() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.sm.ForceResetNamespace(ctx, &types.ForceResetNamespaceRequest{Namespace: smclient.Namespace})
	return err
}

// startServer starts shard-manager-server. It reads the dynamic config at
// startup, unlike a running server, which polls it every few seconds.
func (c *Controller) startServer() (*proc, error) {
	return c.procs.start("shard-manager-server", c.cfg.ServerBin, ".",
		"--root", ".", "--config", "config", "--env", "development",
		"start", "--services", "shard-distributor",
	)
}

func (c *Controller) dynamicConfigPath() string {
	return filepath.Join(c.cfg.RunDir, "dynamicconfig.yaml")
}

func (c *Controller) waitForServer(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := c.sm.GetFullNamespaceState(ctx, &types.GetFullNamespaceStateRequest{Namespace: smclient.Namespace})
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("shard-manager-server not ready (see %s/logs/shard-manager-server.log): %w", c.cfg.RunDir, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// stop drains workers first, then stops the server and etcd, then the UI.
func (c *Controller) stop() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.procs == nil {
		return
	}
	c.mu.Lock()
	workers := slices.Collect(maps.Values(c.workers))
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func() { defer wg.Done(); w.proc.stop(3 * time.Second) }()
	}
	wg.Wait()
	c.procs.mu.Lock()
	rest := slices.Clone(c.procs.running)
	c.procs.mu.Unlock()
	for _, p := range slices.Backward(rest) {
		p.stop(3 * time.Second)
	}
	if c.http != nil {
		_ = c.http.Close()
	}
}

func (c *Controller) loop(ctx context.Context, every time.Duration, fn func()) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}

func (c *Controller) spawnBaseline() {
	c.mu.Lock()
	c.placing, c.placingUntil, c.lastPlacingMove = true, time.Now().Add(15*time.Second), time.Time{}
	c.mu.Unlock()
	for range c.cfg.Workers {
		if err := c.spawnWorker(); err != nil {
			log.Print(err)
		}
	}
}

func (c *Controller) spawnWorker() error {
	c.mu.Lock()
	index := c.nextIndex
	c.nextIndex++
	c.mu.Unlock()

	p, err := c.procs.startWorker(index, c.cfg.WorkerBin,
		"-index", strconv.Itoa(index),
		"-sm", c.cfg.SMAddr,
		"-controller", "http://"+c.cfg.Listen,
	)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.workers[index] = &workerState{index: index, proc: p, lastSample: time.Now()}
	c.mu.Unlock()
	c.addEvent("action", fmt.Sprintf("Worker %d started (pid %d)", index, p.pid()))
	return nil
}

func (c *Controller) addEvent(kind, text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addEventLocked(kind, text)
}

func (c *Controller) addEventLocked(kind, text string) {
	c.events = append([]Event{{AtMs: time.Now().UnixMilli(), Text: text, Kind: kind}}, c.events...)
	if len(c.events) > maxEvents {
		c.events = c.events[:maxEvents]
	}
}

// observe reads the real namespace state from Shard Manager and derives
// lifecycle events and shard moves from the difference with the previous read.
func (c *Controller) observe() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := c.sm.GetFullNamespaceState(ctx, &types.GetFullNamespaceStateRequest{Namespace: smclient.Namespace})
	cancel()

	c.mu.Lock()
	c.smOK = err == nil
	if err == nil {
		c.state = resp
		c.applyStateLocked(resp)
	}
	snapshot := c.snapshotLocked()
	c.mu.Unlock()

	data, _ := json.Marshal(snapshot)
	c.hub.broadcast(data)
}

// workerForExecutor matches an executor to a worker by the metadata the
// worker sets at startup.
func (c *Controller) workerForExecutorLocked(hb *types.HeartbeatState) *workerState {
	index, _ := strconv.Atoi(hb.GetMetadata()["worker"])
	w := c.workers[index]
	if w == nil || strconv.Itoa(w.proc.pid()) != hb.GetMetadata()["pid"] {
		return nil
	}
	return w
}

func (c *Controller) applyStateLocked(resp *types.GetFullNamespaceStateResponse) {
	now := time.Now()
	present := map[int]*types.HeartbeatState{}
	executorToWorker := map[string]int{}
	for executorID, hb := range resp.Executors {
		if w := c.workerForExecutorLocked(hb); w != nil {
			present[w.index] = hb
			executorToWorker[executorID] = w.index
			if w.joinedAt.IsZero() {
				w.joinedAt, w.executorID = now, executorID
				c.addEventLocked("system", fmt.Sprintf("Worker %d registered with Shard Manager", w.index))
			}
		}
	}

	for index, w := range c.workers {
		if _, ok := present[index]; ok || w.proc.alive() {
			continue
		}
		delete(c.workers, index)
		switch {
		case w.joinedAt.IsZero():
			c.addEventLocked("system", fmt.Sprintf("Worker %d exited before registering", index))
		case w.killed:
			c.addEventLocked("system", fmt.Sprintf("Shard Manager declared Worker %d dead (no heartbeat for %s)", index, heartbeatTTL))
		default:
			c.addEventLocked("system", fmt.Sprintf("Worker %d drained and left", index))
		}
	}

	owner := map[string]int{}
	for executorID, assigned := range resp.ShardAssignments {
		for shard := range assigned.GetAssignedShards() {
			owner[shard] = executorToWorker[executorID]
		}
	}

	facts := map[int]workerFacts{}
	for index, w := range c.workers {
		hb := present[index]
		facts[index] = workerFacts{
			dead:           !w.proc.alive() || (hb != nil && hb.Status != types.ExecutorStatusACTIVE),
			joinedRecently: !w.baseline && !w.joinedAt.IsZero() && now.Sub(w.joinedAt) < joinedRecentlyMs*time.Millisecond,
		}
	}
	for _, from := range c.lastOwner {
		if _, known := facts[from]; !known {
			facts[from] = workerFacts{dead: true}
		}
	}

	moves := diffOwnership(c.lastOwner, owner, facts)
	for shard, index := range owner {
		if index != 0 {
			c.lastOwner[shard] = index
		}
	}
	if c.placing {
		c.trackPlacementLocked(now, len(moves) > 0)
		return
	}
	for i := range moves {
		moves[i].AtMs = now.UnixMilli()
		moves[i].Load = c.shardLoadLocked(moves[i].Shard, moves[i].To)
		if moves[i].Reason == reasonRebalance {
			// A load-based move proves Shard Manager is back at work.
			c.managerChangedAt = time.Time{}
		}
	}
	slices.Reverse(moves)
	c.moves = append(moves, c.moves...)
	if len(c.moves) > maxMoves {
		c.moves = c.moves[:maxMoves]
	}
}

// trackPlacementLocked ends the placement once it moved shards, every worker
// has registered and owns shards, nothing moved for 500ms and every worker
// runs exactly the shards it was given (workers learn their shards from
// heartbeat responses, a moment after Shard Manager decides). Shard Manager
// only hands shards to empty workers here, so the shard counts can differ by
// more than one.
func (c *Controller) trackPlacementLocked(now time.Time, moved bool) {
	if moved {
		c.lastPlacingMove = now
	}
	counts := map[int]int{}
	for _, index := range c.lastOwner {
		counts[index]++
	}
	settled := len(c.lastOwner) == workload.ShardCount &&
		!c.lastPlacingMove.IsZero() && now.Sub(c.lastPlacingMove) > 500*time.Millisecond
	for index, w := range c.workers {
		if w.joinedAt.IsZero() || counts[index] == 0 || !c.runsAssignedLocked(w, counts[index]) {
			settled = false
		}
	}
	if !settled && now.Before(c.placingUntil) {
		return
	}
	c.placing = false
	for _, w := range c.workers {
		w.baseline = true
	}
	parts := make([]string, 0, len(counts))
	for _, index := range slices.Sorted(maps.Keys(counts)) {
		parts = append(parts, fmt.Sprintf("W%d: %d", index, counts[index]))
	}
	c.addEventLocked("system", "Shard Manager placed the shards by count ("+strings.Join(parts, ", ")+")")
	if c.turningOff {
		c.turningOff, c.managerOn = false, false
		c.addEventLocked("action", "Shard Manager turned OFF: shards are placed by count only")
	}
	c.busy = ""
}

// runsAssignedLocked reports whether the worker's latest load report covers
// exactly the assigned shards it owns.
func (c *Controller) runsAssignedLocked(w *workerState, assigned int) bool {
	if time.Since(w.loadsAt) > loadReportTTL || len(w.loads) != assigned {
		return false
	}
	for shard := range w.loads {
		if c.lastOwner[shard] != w.index {
			return false
		}
	}
	return true
}

// loadReportTTL covers a few of the workers' 500ms demand polls.
const loadReportTTL = 2 * time.Second

// shardLoadLocked is the load the shard's owner reports running. A shard whose
// owner is dead runs nowhere, so its load is 0.
func (c *Controller) shardLoadLocked(shard string, owner int) float64 {
	w := c.workers[owner]
	if w == nil || !w.proc.alive() || time.Since(w.loadsAt) > loadReportTTL {
		return 0
	}
	if load := w.loads[shard]; load > 0 {
		return load
	}
	// Right after a move the new owner has not measured the shard yet.
	if c.state == nil {
		return 0
	}
	return c.state.ShardStats[shard].GetSmoothedLoad()
}

func (c *Controller) sampleCPU() {
	c.mu.Lock()
	var pids []int
	for _, w := range c.workers {
		if w.proc.alive() {
			pids = append(pids, w.proc.pid())
		}
	}
	c.mu.Unlock()

	times := cpuTimes(pids)
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.workers {
		pct := 0.0
		if t, ok := times[w.proc.pid()]; ok && w.proc.alive() {
			if w.lastCPU > 0 {
				pct = 100 * float64(t-w.lastCPU) / float64(now.Sub(w.lastSample))
			}
			w.lastCPU = t
		}
		w.lastSample = now
		w.cpu = append(w.cpu, max(0, pct))
		if len(w.cpu) > cpuHistoryLen {
			w.cpu = w.cpu[len(w.cpu)-cpuHistoryLen:]
		}
	}
}

func (c *Controller) checkIdle() {
	c.mu.Lock()
	idle := c.dirty && c.busy == "" && time.Since(c.lastActivity) > c.cfg.IdleReset
	c.mu.Unlock()
	if idle {
		go func() {
			if err := c.reset("No one touched the playground for a while"); err != nil && !errors.Is(err, errBusy) && !errors.Is(err, errSwitching) {
				log.Print(err)
			}
		}()
	}
}
