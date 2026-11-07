// demo-controller runs the Shard Manager Playground: etcd, shard-manager-server,
// the demo workers and the web UI.
package main

import (
	"flag"
	"time"

	"go.uber.org/fx"

	"github.com/dkrotx/shard-manager-demo/internal/controller"
	"github.com/dkrotx/shard-manager-demo/internal/smclient"
)

func main() {
	var cfg controller.Config
	flag.StringVar(&cfg.Listen, "listen", "127.0.0.1:8080", "web UI address")
	flag.StringVar(&cfg.SMAddr, "sm", "127.0.0.1:7943", "shard-manager-server gRPC address")
	flag.StringVar(&cfg.ServerBin, "server-bin", "bin/shard-manager-server", "shard-manager-server binary")
	flag.StringVar(&cfg.WorkerBin, "worker-bin", "bin/demo-worker", "demo-worker binary")
	flag.StringVar(&cfg.EtcdBin, "etcd-bin", "etcd", "etcd binary to run; empty to use an already running etcd")
	flag.StringVar(&cfg.EtcdAddr, "etcd", "127.0.0.1:2379", "etcd client address")
	flag.StringVar(&cfg.RunDir, "run-dir", ".run", "logs, PIDs and generated config")
	flag.IntVar(&cfg.Workers, "workers", 3, "workers after start and reset")
	flag.DurationVar(&cfg.IdleReset, "idle-reset", 90*time.Second, "reset after this long without visitor actions")
	flag.Parse()

	fx.New(
		fx.NopLogger,
		fx.StartTimeout(time.Minute),
		fx.StopTimeout(30*time.Second),
		fx.Supply(cfg),
		smclient.Module("demo-controller", cfg.SMAddr),
		controller.Module,
	).Run()
}
