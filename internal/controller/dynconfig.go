package controller

import (
	"fmt"
	"os"
	"time"
)

// Greedy knobs for the playground namespace. These are the real balancer's
// dynamic config keys, tuned so reactions are visible within seconds instead
// of the production defaults (1m smoothing, 1m cooldown, 1% move budget).
// The hysteresis bands are narrower than the defaults (1.15 / 0.90): greedy
// only moves single shards, and with a few heavy shards on 3 workers the
// default bands often stop it while one worker is still visibly busier.
const dynamicConfigTemplate = `# Written by demo-controller. Picked up by shard-manager-server within ~5s.
shardDistributor.loadBalancingMode:
  - value: greedy
    constraints: {namespace: playground}
shardDistributor.loadBalancingGreedy.moveBudgetProportion:
  # 0 disables load-based moves; failure recovery and new-worker placement still run.
  - value: %v
    constraints: {namespace: playground}
shardDistributor.loadBalancingGreedy.loadSmoothingTimeConstant:
  - value: 3s
    constraints: {namespace: playground}
shardDistributor.loadBalancingGreedy.perShardCooldown:
  - value: 5s
    constraints: {namespace: playground}
shardDistributor.loadBalancingGreedy.hysteresisUpperBand:
  - value: 1.05
    constraints: {namespace: playground}
shardDistributor.loadBalancingGreedy.hysteresisLowerBand:
  - value: 0.95
    constraints: {namespace: playground}
`

// moveBudgetOn allows ceil(32*0.2)=7 moves per rebalance pass.
const moveBudgetOn = 0.2

// resumeDelay is how long a restarted Shard Manager typically takes to run its
// first load-based rebalance pass.
const resumeDelay = 3 * time.Second

func writeDynamicConfig(path string, balancing bool) error {
	budget := 0.0
	if balancing {
		budget = moveBudgetOn
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf(dynamicConfigTemplate, budget)), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
