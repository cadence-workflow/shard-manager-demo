// Package workload defines the CPU cost of one request and the baseline demand per shard.
package workload

import "strconv"

// ShardCount must match shardNum of the playground namespace in config/development.yaml.
const ShardCount = 32

// RoundsPerRequest is fixed so every request costs the same work on every
// worker. Calibrating per process would make requests cheaper on workers that
// happened to calibrate on an efficiency core. ~2ms on an Apple M-series
// performance core.
const RoundsPerRequest = 6_500_000

// MaxShardRPS caps a heated shard. Greedy only moves one shard at a time, so a
// worker holding two shards each bigger than the gap to the quietest worker
// stays stuck; keeping heated shards around half a worker's share makes that rare.
const MaxShardRPS = 180

// MaxTotalRPS bounds the whole demo to roughly 4 cores of CPU.
const MaxTotalRPS = 2000

// HeatFactor is how much "Heat up" multiplies a shard's demand, with
// MinHotRPS as a floor so that light shards also become visibly hot.
const (
	HeatFactor = 5
	MinHotRPS  = 100
)

// baseline is deliberately skewed: eight heavy shards and a long tail. Placing
// shards across 3 workers by count leaves the busiest worker at >=1.3x the
// quietest in ~90% of placements, while greedy load balancing gets to ~1.1x.
// Heavy shards stay under half a worker's share so single moves can even it out.
// Sum is 1000 RPS (~2 cores).
var baseline = [ShardCount]float64{
	120, 15, 13, 100, 10, 10, 85, 8, 8, 5, 70, 5, 5, 23, 5, 20,
	120, 15, 12, 100, 10, 10, 85, 7, 7, 5, 70, 5, 5, 22, 5, 20,
}

// Report is what a worker sends with every demand poll: the measured load of
// each shard it is running.
type Report struct {
	Worker int                `json:"worker"`
	PID    int                `json:"pid"`
	Loads  map[string]float64 `json:"loads"`
}

// Hot returns the demand of a heated shard.
func Hot(base float64) float64 {
	return min(max(base*HeatFactor, MinHotRPS), MaxShardRPS)
}

// Baseline returns the default target RPS per shard ID.
func Baseline() map[string]float64 {
	demand := make(map[string]float64, ShardCount)
	for i, rps := range baseline {
		demand[strconv.Itoa(i)] = rps
	}
	return demand
}

// Do performs one request worth of deterministic integer math and returns a
// checksum so the compiler cannot elide the loop.
func Do() uint64 {
	x := uint64(0x9E3779B97F4A7C15)
	for range RoundsPerRequest {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		x *= 0xBF58476D1CE4E5B9
	}
	return x
}
