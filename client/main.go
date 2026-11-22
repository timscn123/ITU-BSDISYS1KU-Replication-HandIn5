package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	pb "auction/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	conn, err := grpc.NewClient(
		"localhost:5002",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	client :=
		pb.NewAuctionServiceClient(conn)

	switch os.Args[1] {

	case "bid":

		amount, _ :=
			strconv.ParseInt(
				os.Args[2],
				10,
				64,
			)

		res, err := client.Bid(
			context.Background(),
			&pb.BidRequest{
				Bidder: "daniel",
				Amount: int32(amount),
			},
		)

		if err != nil {
			panic(err)
		}

		fmt.Println(res.Status)

	case "result":

		res, err :=
			client.Result(
				context.Background(),
				&pb.ResultRequest{},
			)

		if err != nil {
			panic(err)
		}

		fmt.Printf(
			"Highest bid: %d\nWinner: %s\nEnded: %v\n",
			res.HighestBid,
			res.HighestBidder,
			res.Finished,
		)
	}
}
