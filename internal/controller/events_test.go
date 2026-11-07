package controller

import (
	"reflect"
	"testing"
)

func TestDiffOwnership(t *testing.T) {
	tests := []struct {
		name    string
		last    map[string]int
		current map[string]int
		facts   map[int]workerFacts
		want    []Move
	}{
		{
			name:    "no change",
			last:    map[string]int{"1": 1, "2": 2},
			current: map[string]int{"1": 1, "2": 2},
		},
		{
			name:    "first placement is not a move",
			last:    map[string]int{},
			current: map[string]int{"1": 1},
		},
		{
			name:    "momentarily unassigned is not a move",
			last:    map[string]int{"1": 1},
			current: map[string]int{"1": 0},
		},
		{
			name:    "load rebalance",
			last:    map[string]int{"3": 2},
			current: map[string]int{"3": 1},
			want:    []Move{{Shard: "3", From: 2, To: 1, Reason: reasonRebalance}},
		},
		{
			name:    "taken over from a dead worker",
			last:    map[string]int{"3": 2},
			current: map[string]int{"3": 1},
			facts:   map[int]workerFacts{2: {dead: true}},
			want:    []Move{{Shard: "3", From: 2, To: 1, Reason: reasonRecovery}},
		},
		{
			name:    "handed to a new worker",
			last:    map[string]int{"10": 1, "3": 2},
			current: map[string]int{"10": 5, "3": 5},
			facts:   map[int]workerFacts{5: {joinedRecently: true}},
			want: []Move{
				{Shard: "3", From: 2, To: 5, Reason: reasonNewWorker},
				{Shard: "10", From: 1, To: 5, Reason: reasonNewWorker},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := diffOwnership(tt.last, tt.current, tt.facts)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}
