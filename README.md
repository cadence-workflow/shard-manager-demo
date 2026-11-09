# Shard Manager Playground

**Break the balance. Watch it recover.**

A small interactive demo of the real [Shard Manager](https://github.com/cadence-workflow/shard-manager).
Three real worker processes own 32 shards and burn real CPU for them. Visitors heat up shards,
kill workers, and add workers. The real Shard Manager decides where every shard goes.

Nothing is simulated: the UI only shows what Shard Manager reports, and the CPU numbers come
from the OS.

**Shard Manager OFF.** Shards are placed by count only, as without Shard Manager, so one worker
does far more than another:

![Shard Manager OFF](docs/1-balancing-off.png)

**Shard Manager ON.** Within a couple of seconds it moves a few shards, and the load is even:

![Shard Manager ON](docs/2-balancing-on.png)

**Kill a worker.** Shard Manager notices the missing heartbeat and moves its shards to the survivors:

![Recovery after killing a worker](docs/3-recovery.png)

**Press `?`** for a short guide to the screen:

![Help guide](docs/4-help.png)

## Run it

You need Go 1.26, `etcd` (`brew install etcd`), and a checkout of Shard Manager next to this repo
(`../shard-manager`, or set `SHARD_MANAGER_DIR`).

```
make demo
```

Open <http://localhost:8080>. Ctrl-C stops everything: workers, the server and etcd.

No native etcd? Use Docker for etcd only (the workers must stay native, so they are visible in `top`):

```
docker compose up -d etcd
make demo ETCD_BIN=
```

## What runs

```
demo-controller            web UI, process lifecycle, workload demand table
├── playground-etcd        fresh data dir on every start
├── shard-manager-server   real server, config in config/, namespace "playground" (fixed, 32 shards)
└── demo-worker-1..N       real executors (executorclient), one OS process each
```

- **Workers** register with Shard Manager through the real `executorclient` (heartbeat every 2s, with a 4s TTL).
  For each shard they are assigned, they run requests at the shard's target rate. One request is a
  fixed piece of integer math (~2ms on an M-series performance core). Each worker reports the rate it
  actually completed as the shard load (`ShardReport.ShardLoad`).
- **The workload generator** is the controller's demand table (`/api/demand`): the target RPS per
  shard. Workers only run demand for shards they own, so the work follows ownership. With every
  poll they also report the load of the shards they run, which the UI shows.
- **Shard Manager** decides placement with its greedy load balancer. The controller only reads
  state (`GetFullNamespaceState`) and turns differences between two reads into the moves list.
- **CPU** in the UI is per-process CPU time from `ps` (macOS) or `/proc` (Linux). It is
  independent of Shard Manager, so it is evidence that the work really moved.

## The four buttons

| Action | What really happens |
| --- | --- |
| Shard Manager ON/OFF | Restarts the real `shard-manager-server` with greedy load balancing enabled (ON, ~1s) or disabled (OFF). OFF also wipes the namespace (`ForceResetNamespace`), so the shards are placed by count again as the workers re-register (~5s). |
| Heat up a shard | Raises that shard's target RPS 5x (min 100, max 180). The owner burns more CPU and reports more load. |
| Kill a worker | `SIGKILL`, with no draining. After the 4s heartbeat TTL the shards go to the survivors: by load when ON, by count when OFF. |
| Add a worker | Starts a new process. It gets a share of shards by count; when ON, load balancing tunes it further. |
| Reset | Drains all workers, restarts the server with load balancing disabled, wipes the namespace and places 3 fresh workers by count. |

After 90s without any visitor action, the playground resets by itself (`-idle-reset`).

Press `?` (or the `?` button in the corner) to open a short guide that explains the screen to a visitor.

## Booth script (30-60 seconds)

1. "These are 3 real processes running 32 shards. Without Shard Manager they are spread by count,
   so look at the imbalance: one worker does several times the work of another. You can see it
   in the CPU numbers too."
2. Optionally, the visitor adds or kills a worker first: shards move by count, and the
   imbalance stays.
3. Visitor turns **Shard Manager ON**. Within a couple of seconds it moves a few shards, and the
   meter goes green.
4. Visitor clicks a big shard. It becomes HOT, and Shard Manager moves the other shards away from
   its worker.
5. Visitor kills a worker. Its wire counts down the 4s TTL, then the shards slide to the survivors.
6. Visitor adds a worker. It joins and gets shards.
7. **Reset** for the next visitor.

## Show the processes outside the browser

```
make htop   # htop --filter demo-worker (brew install htop)
make top    # plain top for the current workers
```

The workers appear as `demo-worker-1`, `demo-worker-2`, ... in `ps`, `top` and `htop`.

## Tradeoffs and tuning

- **Faster balancer knobs.** The greedy balancer is tuned through its real dynamic config keys, for
  this namespace only (`internal/controller/dynconfig.go`): 3s load smoothing instead of 1m, 5s
  per-shard cooldown instead of 1m, 20% move budget instead of 1%, and hysteresis bands of
  1.05/0.95 instead of 1.15/0.90. The algorithm is unchanged; it just reacts within seconds instead
  of minutes.
- **The baseline is skewed on purpose** (`internal/workload/work.go`): eight heavy shards and a long
  tail. With load balancing disabled Shard Manager places shards by count only, so while it is off
  the workers are visibly imbalanced most of the time. The exact numbers vary, because the
  placement is Shard Manager's.
- **Greedy moves one shard at a time.** If one worker holds two heavy shards and moving either
  would just overload another worker, greedy stops there. It does not swap shards. That is why
  the playground uses 32 smaller shards and caps a heated shard at 180 RPS: no shard is more than
  about half a worker's share, so there is almost always a shard small enough to close the gap.
- **A single hot shard cannot be split.** If one shard is hotter than an average worker,
  Shard Manager leaves it alone on its worker and moves everything else away. The narration
  explains this when it happens.
- **Pull model for demand.** Workers poll the demand table instead of receiving routed requests.
  This keeps the demo simple, and no request is lost while a shard moves. Workers exit if the
  controller is unreachable for 3s, so they never become orphans.
- **OFF is Shard Manager without load balancing.** The server keeps running so new and dead
  workers are still handled, by count, like a naive placement would. The UI shows it as off:
  no heartbeats travel and the box is dashed. Switching restarts the server, because a running
  server takes up to 5s to read a dynamic config change; workers keep their shards meanwhile.
- **Turning OFF re-places the shards by count.** Disabling load balancing alone would keep the
  balance it reached; wiping the namespace makes OFF always show what count-only placement
  looks like.

## Layout

```
cmd/demo-controller   fx app: smclient + controller
cmd/demo-worker       fx app: smclient + worker + upstream executorclient module
internal/controller   processes, state observer, move detection, actions, SSE
internal/worker       ShardProcessor, demand polling, fx wiring
internal/workload     CPU work unit, baseline demand
internal/smclient     YARPC gRPC clients for shard-manager-server
web/                  static UI (vanilla JS), embedded into the controller
config/               shard-manager-server config
```

`make test` runs `go vet` and the unit tests.
