// Package smclient provides YARPC gRPC clients to shard-manager-server as an fx
// module, the same way the upstream canary wires them.
package smclient

import (
	"time"

	"go.uber.org/fx"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"

	sharddistributorv1 "github.com/cadence-workflow/shard-manager/.gen/proto/sharddistributor/v1"
	"github.com/cadence-workflow/shard-manager/client/sharddistributor"
	grpcwrapper "github.com/cadence-workflow/shard-manager/client/wrappers/grpc"
	timeoutwrapper "github.com/cadence-workflow/shard-manager/client/wrappers/timeout"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/client/executorclient"
)

// Namespace is the fixed namespace configured in config/development.yaml.
const Namespace = "playground"

// serviceName is the outbound name the generated YARPC clients expect.
const serviceName = "shard-distributor"

// Module provides an executor client (for workers) and an API client (for the
// controller). The dispatcher starts before and stops after its users, so a
// worker can still send its final draining heartbeat on shutdown.
func Module(callerName, address string) fx.Option {
	return fx.Module("smclient",
		fx.Provide(
			func(lc fx.Lifecycle) *yarpc.Dispatcher {
				t := grpc.NewTransport()
				d := yarpc.NewDispatcher(yarpc.Config{
					Name:      callerName,
					Outbounds: yarpc.Outbounds{serviceName: {Unary: t.NewSingleOutbound(address)}},
				})
				lc.Append(fx.StartStopHook(d.Start, d.Stop))
				return d
			},
			func(d *yarpc.Dispatcher) executorclient.Client {
				c := grpcwrapper.NewShardDistributorExecutorClient(
					sharddistributorv1.NewShardDistributorExecutorAPIYARPCClient(d.ClientConfig(serviceName)))
				return timeoutwrapper.NewShardDistributorExecutorClient(c, 2*time.Second)
			},
			func(d *yarpc.Dispatcher) sharddistributor.Client {
				c := grpcwrapper.NewShardDistributorClient(
					sharddistributorv1.NewShardDistributorAPIYARPCClient(d.ClientConfig(serviceName)))
				return timeoutwrapper.NewShardDistributorClient(c, time.Second)
			},
		),
	)
}
