// Package workload defines the CPU cost of one request and the baseline demand per shard.
package workload

import "strconv"

// ShardCount must match shardNum of the playground namespace in config/development.yaml.
const ShardCount = 16

// RoundsPerRequest is fixed so every request costs the same work on every
// worker. Calibrating per process would make requests cheaper on workers that
// happened to calibrate on an efficiency core. ~2ms on an Apple M-series
// performance core.
const RoundsPerRequest = 6_500_000

// MaxShardRPS caps a heated shard. It stays below one core (each shard runs on
// one goroutine) and close to the average worker load, so that after moving
// the other shards away the balancer can still reach an even spread.
const MaxShardRPS = 360

// MaxTotalRPS bounds the whole demo to roughly 4 cores of CPU.
const MaxTotalRPS = 2000

// HeatFactor is how much "Heat up" multiplies a shard's demand, with
// MinHotRPS as a floor so that light shards also become visibly hot.
const (
	HeatFactor = 5
	MinHotRPS  = 200
)

// baseline is deliberately skewed: four heavy shards and a long tail. Placing
// 4 shards per worker by count leaves the busiest worker at >=1.3x the average
// in ~90% of placements, while a load-aware placement can reach ~1.02x.
// Sum is 1000 RPS (~2 cores).
var baseline = [ShardCount]float64{240, 30, 25, 200, 20, 20, 170, 15, 15, 10, 140, 10, 10, 45, 10, 40}

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
