package main

import (
	pb "auction/grpc"
	"log"
	"net"
	"os"
	"strconv"

	"google.golang.org/grpc"
)

func nodeID(port string) int32 {
	p, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal("invalid port: ", port)
	}
	return int32(p - 5000)
}

func main() {

	port := os.Args[1]

	id := nodeID(port)

	cluster := map[int32]string{
		1: "localhost:5001",
		2: "localhost:5002",
		3: "localhost:5003",
	}

	monitor := NewMonitor(id, cluster)
	monitor.Start()

	state := &AuctionState{}

	server := &Server{
		state:   state,
		myID:    id,
		peers:   cluster,
		monitor: monitor,
	}

	lis, err := net.Listen(
		"tcp",
		":"+port,
	)

	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterAuctionServiceServer(
		grpcServer,
		server,
	)

	log.Println(
		"Running on port",
		port,
	)

	grpcServer.Serve(lis)
}
