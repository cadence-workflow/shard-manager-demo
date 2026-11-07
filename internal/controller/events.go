package controller

import (
	"sort"
	"strconv"
)

// Move reasons are inferred from observed state only.
const (
	reasonRebalance = "rebalance"
	reasonRecovery  = "recovery"
	reasonNewWorker = "new worker"
)

// Move is one observed change of a shard's owner in Shard Manager state.
type Move struct {
	AtMs   int64   `json:"t"`
	Shard  string  `json:"shard"`
	Load   float64 `json:"load"`
	From   int     `json:"from"`
	To     int     `json:"to"`
	Reason string  `json:"reason"`
}

// workerFacts tells the differ what the controller knows about a worker.
type workerFacts struct {
	dead           bool // process exited or Shard Manager stopped treating it as active
	joinedRecently bool
}

// diffOwnership compares the last known owner of each shard (worker index)
// with the current one. Shards that are momentarily unassigned (owner 0) keep
// their last owner, so a dead-worker -> survivor handover is one move.
func diffOwnership(last, current map[string]int, facts map[int]workerFacts) []Move {
	var moves []Move
	for shard, to := range current {
		from := last[shard]
		if to == 0 || from == 0 || from == to {
			continue
		}
		reason := reasonRebalance
		switch {
		case facts[from].dead:
			reason = reasonRecovery
		case facts[to].joinedRecently:
			reason = reasonNewWorker
		}
		moves = append(moves, Move{Shard: shard, From: from, To: to, Reason: reason})
	}
	sort.Slice(moves, func(i, j int) bool { return shardNum(moves[i].Shard) < shardNum(moves[j].Shard) })
	return moves
}

func shardNum(id string) int {
	n, _ := strconv.Atoi(id)
	return n
}
