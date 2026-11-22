package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	pb "auction/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var nodes = []string{
	"localhost:5001",
	"localhost:5002",
	"localhost:5003",
}

const callTimeout = 3 * time.Second

func withAnyNode(
	fn func(ctx context.Context, c pb.AuctionServiceClient) error,
) error {

	var firstErr error

	for _, addr := range nodes {

		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)
		if err != nil {
			continue
		}

		ctx, cancel :=
			context.WithTimeout(
				context.Background(),
				callTimeout,
			)

		err = fn(ctx, pb.NewAuctionServiceClient(conn))

		cancel()
		conn.Close()

		if err == nil {
			return nil
		}

		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func main() {

	switch os.Args[1] {

	case "bid":

		amount, _ :=
			strconv.ParseInt(
				os.Args[2],
				10,
				64,
			)

		err := withAnyNode(
			func(ctx context.Context, c pb.AuctionServiceClient) error {
				res, err := c.Bid(
					ctx,
					&pb.BidRequest{
						Bidder: "daniel",
						Amount: int32(amount),
					},
				)
				if err != nil {
					return err
				}
				fmt.Println(res.Status)
				return nil
			},
		)

		if err != nil {
			fmt.Println("EXCEPTION")
		}

	case "result":

		err := withAnyNode(
			func(ctx context.Context, c pb.AuctionServiceClient) error {
				res, err := c.Result(
					ctx,
					&pb.ResultRequest{},
				)
				if err != nil {
					return err
				}
				fmt.Printf(
					"Highest bid: %d\nWinner: %s\nEnded: %v\n",
					res.HighestBid,
					res.HighestBidder,
					res.Finished,
				)
				return nil
			},
		)

		if err != nil {
			fmt.Println("EXCEPTION")
		}
	}
}
