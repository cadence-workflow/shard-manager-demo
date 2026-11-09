package worker

import (
	"os"
	"strconv"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/cadence-workflow/shard-manager/common/clock"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/client/clientcommon"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/client/executorclient"

	"github.com/dkrotx/shard-manager-demo/internal/smclient"
)

// Module provides everything executorclient.Params needs for this worker.
// The worker number and PID go into executor metadata so the controller can
// map Shard Manager executors back to OS processes.
func Module(index int, controllerURL string) fx.Option {
	return fx.Module("worker",
		fx.Supply(
			fx.Annotate(tally.NoopScope, fx.As(new(tally.Scope))),
			fx.Annotate(clock.NewRealTimeSource(), fx.As(new(clock.TimeSource))),
			clientcommon.Config{Namespaces: []clientcommon.NamespaceConfig{{
				Namespace:         smclient.Namespace,
				HeartBeatInterval: 2 * time.Second,
			}}},
			executorclient.ExecutorMetadata{
				"worker": strconv.Itoa(index),
				"pid":    strconv.Itoa(os.Getpid()),
			},
		),
		fx.Provide(
			func() (*zap.Logger, error) { return zap.NewProduction(zap.IncreaseLevel(zap.WarnLevel)) },
			NewActive,
			func(lc fx.Lifecycle, active *Active) *Demand {
				d := newDemand(controllerURL, index, active)
				lc.Append(fx.StartStopHook(d.Start, d.Stop))
				return d
			},
			func(d *Demand, active *Active) executorclient.ShardProcessorFactory[*Processor] {
				return &Factory{Demand: d.Get, Active: active}
			},
		),
	)
}
