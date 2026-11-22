package main

import (
	"context"
	"log"
	"net"
	"os/signal"
	"syscall"
	"time"
)

// table hardcodes the demo topology: each node's id, its listen address, and
// the peers it knows about. Adding a node is one more row here plus one more
// entry in every other row's peer list.
var table = []struct {
	id    string
	addr  string
	peers []peer
}{
	{
		id:    "node-1",
		addr:  "127.0.0.1:50051",
		peers: []peer{{id: "node-2", addr: "127.0.0.1:50052"}, {id: "node-3", addr: "127.0.0.1:50053"}},
	},
	{
		id:    "node-2",
		addr:  "127.0.0.1:50052",
		peers: []peer{{id: "node-1", addr: "127.0.0.1:50051"}, {id: "node-3", addr: "127.0.0.1:50053"}},
	},
	{
		id:    "node-3",
		addr:  "127.0.0.1:50053",
		peers: []peer{{id: "node-1", addr: "127.0.0.1:50051"}, {id: "node-2", addr: "127.0.0.1:50052"}},
	},
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1) Build the nodes and get every server listening first, so the
	//    workloads never dial a port that is not up yet.
	nodes := make([]*Node, 0, len(table))
	for _, cfg := range table {
		n, err := New(cfg.id, cfg.peers)
		if err != nil {
			log.Fatalf("%s: %v", cfg.id, err)
		}
		lis, err := net.Listen("tcp", cfg.addr)
		if err != nil {
			log.Fatalf("%s: %v", cfg.id, err)
		}
		go n.serve(lis)
		nodes = append(nodes, n)
	}
	time.Sleep(300 * time.Millisecond) // let the servers finish binding

	<-ctx.Done()

	for _, n := range nodes {
		n.Stop()
	}
}
