package main

import (
	pb "auction/grpc"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
)

func main() {

	port := os.Args[1]

	state := &AuctionState{
		EndTime: time.Now().Add(
			100 * time.Second,
		),
	}

	server := &Server{
		state: state,
	}

	if port == "5001" {

		server.isPrimary = true

		server.backups = []string{
			"localhost:5002",
			"localhost:5003",
		}
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
