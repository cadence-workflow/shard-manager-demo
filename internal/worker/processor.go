// Package worker implements the Shard Manager ShardProcessor that burns CPU
// for the shards this process owns.
package worker

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/client/executorclient"

	"github.com/dkrotx/shard-manager-demo/internal/workload"
)

const (
	tick          = 10 * time.Millisecond
	reportWindow  = 500 * time.Millisecond
	maxBacklog    = 100 * time.Millisecond
	loadEWMAAlpha = 0.5
)

// DemandFunc returns the current target RPS of a shard.
type DemandFunc func(shardID string) float64

// Processor executes requests for one shard at its target rate and reports the
// rate it actually completed as the shard load.
type Processor struct {
	shardID string
	demand  DemandFunc

	status   atomic.Int32
	loadBits atomic.Uint64
	stop     chan struct{}
	done     chan struct{}
}

var _ executorclient.ShardProcessor = (*Processor)(nil)

func (p *Processor) Start(context.Context) error {
	go p.run()
	return nil
}

func (p *Processor) Stop() {
	close(p.stop)
	<-p.done
}

func (p *Processor) GetShardReport() executorclient.ShardReport {
	return executorclient.ShardReport{
		ShardLoad: math.Float64frombits(p.loadBits.Load()),
		Status:    types.ShardStatus(p.status.Load()),
	}
}

func (p *Processor) SetShardStatus(status types.ShardStatus) {
	p.status.Store(int32(status))
}

func (p *Processor) run() {
	defer close(p.done)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var owed float64
	var completed int
	last := time.Now()
	windowStart := last
	load := 0.0

	for {
		select {
		case <-p.stop:
			return
		case now := <-ticker.C:
			rate := p.demand(p.shardID)
			owed = min(owed+rate*now.Sub(last).Seconds(), rate*maxBacklog.Seconds()+1)
			last = now
			for ; owed >= 1; owed-- {
				workload.Do()
				completed++
			}

			if elapsed := now.Sub(windowStart); elapsed >= reportWindow {
				measured := float64(completed) / elapsed.Seconds()
				load = loadEWMAAlpha*measured + (1-loadEWMAAlpha)*load
				p.loadBits.Store(math.Float64bits(math.Round(load*10) / 10))
				completed = 0
				windowStart = now
			}
		}
	}
}

// Factory creates processors for shards Shard Manager assigns to this executor.
type Factory struct {
	Demand DemandFunc
}

func (f *Factory) NewShardProcessor(shardID string) (*Processor, error) {
	p := &Processor{
		shardID: shardID,
		demand:  f.Demand,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	p.status.Store(int32(types.ShardStatusREADY))
	return p, nil
}
