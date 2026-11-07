// demo-worker is one Shard Manager executor: it heartbeats through the real
// executorclient and burns CPU for the shards it is assigned.
package main

import (
	"flag"
	"fmt"

	"go.uber.org/fx"

	"github.com/cadence-workflow/shard-manager/service/sharddistributor/client/executorclient"

	"github.com/dkrotx/shard-manager-demo/internal/smclient"
	"github.com/dkrotx/shard-manager-demo/internal/worker"
)

func main() {
	index := flag.Int("index", 1, "worker number shown in the UI")
	smAddr := flag.String("sm", "127.0.0.1:7943", "shard-manager-server gRPC address")
	controllerURL := flag.String("controller", "http://127.0.0.1:8080", "demo controller URL")
	flag.Parse()

	fx.New(
		fx.NopLogger,
		smclient.Module(fmt.Sprintf("demo-worker-%d", *index), *smAddr),
		worker.Module(*index, *controllerURL),
		executorclient.ModuleWithNamespace[*worker.Processor](smclient.Namespace),
	).Run()
}
