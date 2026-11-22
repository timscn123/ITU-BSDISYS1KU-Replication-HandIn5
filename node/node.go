package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	auction "Auction/gRPC"
)

// peer is another node: the id it uses in messages, and where to reach it.
type peer struct {
	id   string
	addr string
}

// Node is one participant in the Ricart-Agrawala algorithm. It owns all
// per-node state and implements the generated gRPC server interface.
// One Node = one process in the textbook picture; running three Nodes in
// one binary over localhost simulates the distributed system.
type Node struct {
    auction.UnimplementedAuctionServiceServer

    id string

    clients map[string]auction.AuctionServiceClient
    server *grpc.Server

    mu sync.Mutex

    highestBid int
    highestBidder string

    auctionEnd time.Time

	bids map[string]int
}



// New builds a node and dials all of its peers.
func New(id string, peers []peer) (*Node, error) {
	n := &Node{id: id, clients: make(map[string]mutex.Ricart_ArgawalaClient, len(peers))}
	for _, p := range peers {
		conn, err := grpc.NewClient(p.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial %s (%s): %w", p.id, p.addr, err)
		}
		n.clients[p.id] = auction.NewAuctionServiceClient(conn)
	}

	n := &Node{
		id: id,
		clients: make(map[string]auction.AuctionServiceClient, len(peers)),
		bids: make(map[string]int),
		auctionEnd: time.Now().Add(100 * time.Second),
	}

	return n, nil
}

// serve runs this node's gRPC server until Stop is called.
func (n *Node) serve(lis net.Listener) {
	n.server = grpc.NewServer()
	mutex.RegisterRicart_ArgawalaServer(n.server, n)
	logger.Log(n.id, "LISTEN", lis.Addr().String())
	if err := n.server.Serve(lis); err != nil {
		logger.Log(n.id, "SERVE_ERROR", err.Error())
	}
}

// Stop gracefully shuts the node's server down.
func (n *Node) Stop() {
	if n.server != nil {
		n.server.GracefulStop()
	}
}

func (n *Node) Bid(
    ctx context.Context,
    req *auction.BidRequest,
) (*auction.BidResponse,error) {

    n.mu.Lock()
    defer n.mu.Unlock()

    if time.Now().After(n.auctionEnd) {
        return &auction.BidResponse{
            Status: "fail",
        }, nil
    }

    old, exists := n.bids[req.Bidder]

    if exists && int(req.Amount) <= old {
        return &auction.BidResponse{
            Status: "fail",
        }, nil
    }

    n.bids[req.Bidder] = int(req.Amount)

    if int(req.Amount) > n.highestBid {
        n.highestBid = int(req.Amount)
        n.highestBidder = req.Bidder
    }

    go n.replicateBid(req.Bidder, int(req.Amount))

    return &auction.BidResponse{
        Status:"success",
    }, nil
}

func (n *Node) replicateBid(
    bidder string,
    amount int,
) {

    for _, cli := range n.clients {

        ctx,cancel :=
            context.WithTimeout(
                context.Background(),
                2*time.Second,
            )

        cli.ReplicateBid(
            ctx,
            &auction.ReplicationMessage{
                Bidder: bidder,
                Amount: int32(amount),
            },
        )

        cancel()
    }
}

func (n *Node) ReplicateBid(
    ctx context.Context,
    req *auction.ReplicationMessage,
)(*emptypb.Empty,error){

    n.mu.Lock()
    defer n.mu.Unlock()

    n.bids[req.Bidder] =
        int(req.Amount)

    if int(req.Amount) > n.highestBid {

        n.highestBid =
           int(req.Amount)

        n.highestBidder =
           req.Bidder
    }

    return &emptypb.Empty{}, nil
}

func (n *Node) Result(
    ctx context.Context,
    req *auction.ResultRequest,
)(
    *auction.ResultResponse,
    error,
) {

    n.mu.Lock()
    defer n.mu.Unlock()

    return &auction.ResultResponse{
        Finished:
           time.Now().After(n.auctionEnd),

        HighestBid:
           int32(n.highestBid),

        HighestBidder:
           n.highestBidder,
    }, nil
}

auctionEnd: time.Now().Add(100 * time.Second)

