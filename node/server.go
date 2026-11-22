package main

import (
	pb "auction/grpc"
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	pb.UnimplementedAuctionServiceServer

	state *AuctionState

	isPrimary bool

	backups []string

	myID int32

	monitor *Monitor
}

func (s *Server) Heartbeat(
	ctx context.Context,
	req *pb.HeartbeatRequest,
) (*pb.HeartbeatReply, error) {

	if s.monitor != nil {
		s.monitor.MarkAlive(req.NodeId)
	}

	return &pb.HeartbeatReply{
		NodeId: s.myID,
	}, nil
}

func (s *Server) Bid(
	ctx context.Context,
	req *pb.BidRequest,
) (*pb.BidResponse, error) {

	if !s.isPrimary {
		return s.forwardBid(req)
	}

	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	if time.Now().After(s.state.EndTime) {
		return &pb.BidResponse{
			Status: "FAIL",
		}, nil
	}

	if int64(req.Amount) <= s.state.HighestBid {
		return &pb.BidResponse{
			Status: "FAIL",
		}, nil
	}

	s.state.HighestBid = int64(req.Amount)
	s.state.Winner = req.Bidder

	acks := 0

	for _, backup := range s.backups {

		conn, err := grpc.NewClient(
			backup,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)

		if err != nil {
			continue
		}

		client := pb.NewAuctionServiceClient(conn)

		_, err = client.ReplicateBid(
			context.Background(),
			&pb.ReplicateRequest{
				Bidder: req.Bidder,
				Amount: req.Amount,
			},
		)

		if err == nil {
			acks++
		}

		conn.Close()
	}

	if acks >= 1 {
		return &pb.BidResponse{
			Status: "SUCCESS",
		}, nil
	}

	return &pb.BidResponse{
		Status: "EXCEPTION",
	}, nil
}

func (s *Server) Result(
	ctx context.Context,
	req *pb.ResultRequest,
) (*pb.ResultResponse, error) {

	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	ended :=
		time.Now().After(s.state.EndTime)

	return &pb.ResultResponse{
		Finished:      ended,
		HighestBidder: s.state.Winner,
		HighestBid:    int32(s.state.HighestBid),
	}, nil
}

func (s *Server) ReplicateBid(
	ctx context.Context,
	req *pb.ReplicateRequest,
) (*pb.ReplicateReply, error) {

	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	if int64(req.Amount) > s.state.HighestBid {

		s.state.HighestBid = int64(req.Amount)
		s.state.Winner = req.Bidder
	}

	return &pb.ReplicateReply{
		Success: true,
	}, nil
}

func (s *Server) forwardBid(
	req *pb.BidRequest,
) (*pb.BidResponse, error) {

	conn, err := grpc.NewClient(
		"localhost:5001",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		return &pb.BidResponse{
			Status: "EXCEPTION",
		}, nil
	}

	defer conn.Close()

	client :=
		pb.NewAuctionServiceClient(conn)

	return client.Bid(
		context.Background(),
		req,
	)
}
