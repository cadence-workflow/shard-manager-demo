SHELL := /bin/bash
SHARD_MANAGER_DIR ?= ../shard-manager
ETCD_BIN ?= etcd

.PHONY: demo build server top htop test clean

## demo: build everything and run the playground on http://127.0.0.1:8080
demo: build
	./bin/demo-controller -etcd-bin '$(ETCD_BIN)'

build: bin/shard-manager-server
	go build -o bin/ ./cmd/...

## server: shard-manager-server and smctl from the Shard Manager checkout
server: bin/shard-manager-server

bin/shard-manager-server:
	@test -d $(SHARD_MANAGER_DIR) || (echo "Shard Manager checkout not found at $(SHARD_MANAGER_DIR); set SHARD_MANAGER_DIR" && exit 1)
	cd $(SHARD_MANAGER_DIR) && go build -o $(CURDIR)/bin/shard-manager-server ./cmd/server
	cd $(SHARD_MANAGER_DIR) && go build -o $(CURDIR)/bin/smctl ./cmd/smctl

## top: watch the worker processes with plain top
top:
	@pids=$$(pgrep -d, -f '^demo-worker-'); test -n "$$pids" || (echo "no demo workers running" && exit 1); \
	if [ "$$(uname)" = Darwin ]; then top -stats pid,command,cpu,time -pid $${pids//,/ -pid }; else top -p $$pids; fi

## htop: watch the worker processes with htop
htop:
	htop --filter demo-worker

test:
	go vet ./...
	go test ./...

clean:
	rm -rf bin .run
