// auction.go
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type Empty struct{}

type State struct {
	Seq           int
	Bids          map[string]int // bidder -> their latest (highest) bid
	Highest       int
	HighestBidder string
}

type BidArgs struct {
	Bidder string
	Amount int
}
type BidReply struct{ Outcome string } // "success" | "fail"
type ResultReply struct{ Outcome string }

type Node struct {
	mu       sync.Mutex
	id       int
	addrs    []string
	leader   int
	deadline time.Time
	st       State
}

// call performs an RPC with timeouts (the network has a known time bound).
func call(addr, method string, args, reply any) error {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return err
	}
	c := rpc.NewClient(conn)
	defer c.Close()
	done := c.Go(method, args, reply, nil)
	select {
	case r := <-done.Done:
		return r.Error
	case <-time.After(2 * time.Second):
		return errors.New("timeout")
	}
}

// ---------- RPCs between nodes ----------

func (n *Node) Ping(_ Empty, _ *Empty) error { return nil }

// Replicate: backup installs the leader's state if it is newer.
func (n *Node) Replicate(s State, _ *Empty) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if s.Seq > n.st.Seq {
		n.st = s
	}
	return nil
}

// ---------- RPCs for clients ----------

func (n *Node) Bid(a BidArgs, r *BidReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leader != n.id {
		return errors.New("not leader")
	}
	if time.Now().After(n.deadline) {
		r.Outcome = "fail" // auction is over
		return nil
	}
	prev := n.st.Bids[a.Bidder]
	if prev == a.Amount {
		r.Outcome = "success" // client retry of an already-applied bid (idempotent)
		return nil
	}
	if a.Amount <= prev || a.Amount <= n.st.Highest {
		r.Outcome = "fail"
		return nil
	}
	n.st.Bids[a.Bidder] = a.Amount
	n.st.Highest, n.st.HighestBidder = a.Amount, a.Bidder
	n.st.Seq++
	n.replicate() // synchronous: all live backups have it before we ack
	r.Outcome = "success"
	return nil
}

func (n *Node) Result(_ Empty, r *ResultReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leader != n.id {
		return errors.New("not leader")
	}
	if time.Now().After(n.deadline) {
		r.Outcome = fmt.Sprintf("Auction over. Winner: %q with %d", n.st.HighestBidder, n.st.Highest)
	} else {
		r.Outcome = fmt.Sprintf("Auction running. Highest bid: %d by %q", n.st.Highest, n.st.HighestBidder)
	}
	return nil
}

// replicate must be called with n.mu held. Errors = crashed backup; ignore.
func (n *Node) replicate() {
	var wg sync.WaitGroup
	for i, addr := range n.addrs {
		if i == n.id {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			_ = call(addr, "Node.Replicate", n.st, &Empty{})
		}(addr)
	}
	wg.Wait()
}

// monitor: leader = lowest-ID node that responds (self always counts).
func (n *Node) monitor() {
	for {
		l := n.id
		for i := 0; i < n.id; i++ {
			if call(n.addrs[i], "Node.Ping", Empty{}, &Empty{}) == nil {
				l = i
				break
			}
		}
		n.mu.Lock()
		if l != n.leader {
			log.Printf("node %d: leader is now %d", n.id, l)
		}
		n.leader = l
		n.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

func runServer(id int, addrs []string, deadline int64) {
	n := &Node{
		id: id, addrs: addrs, leader: 0,
		deadline: time.Unix(deadline, 0),
		st:       State{Bids: map[string]int{}},
	}
	srv := rpc.NewServer()
	if err := srv.RegisterName("Node", n); err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", addrs[id])
	if err != nil {
		log.Fatal(err)
	}
	go n.monitor()
	log.Printf("node %d listening on %s", id, addrs[id])
	srv.Accept(ln)
}

// ---------- client: try every known node until the leader answers ----------

func runClient(addrs []string, bidder string, amount int, query bool) {
	for _, addr := range addrs {
		if query {
			var r ResultReply
			if err := call(addr, "Node.Result", Empty{}, &r); err == nil {
				fmt.Println(r.Outcome)
				return
			}
		} else {
			var r BidReply
			if err := call(addr, "Node.Bid", BidArgs{bidder, amount}, &r); err == nil {
				fmt.Println(r.Outcome)
				return
			}
		}
	}
	fmt.Println("exception: no node reachable")
}

func main() {
	id := flag.Int("id", 0, "node id (index into -addrs)")
	addrList := flag.String("addrs", "localhost:5000,localhost:5001,localhost:5002", "all node addresses")
	deadline := flag.Int64("deadline", 0, "unix time when the auction ends")
	client := flag.Bool("client", false, "run as client")
	bidder := flag.String("bidder", "", "bidder name")
	bid := flag.Int("bid", 0, "amount to bid")
	result := flag.Bool("result", false, "query the auction state")
	flag.Parse()

	addrs := strings.Split(*addrList, ",")
	if *client {
		runClient(addrs, *bidder, *bid, *result)
		return
	}
	runServer(*id, addrs, *deadline)
}
