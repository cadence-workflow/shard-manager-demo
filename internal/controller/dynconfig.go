package controller

import (
	"fmt"
	"os"
)

// Greedy knobs for the playground namespace. These are the real balancer's
// dynamic config keys, tuned so reactions are visible within seconds instead
// of the production defaults (1m smoothing, 1m cooldown, 1% move budget).
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
`

// moveBudgetOn allows ceil(16*0.2)=4 moves per rebalance pass.
const moveBudgetOn = 0.2

// dynamicConfigApplyDelay covers the server's 5s minimum poll interval plus one rebalance period.
const dynamicConfigApplyDelay = 6500 // ms

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
