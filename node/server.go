package main

import (
	pb "auction/grpc"
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const rpcTimeout = 1 * time.Second

type Server struct {
	pb.UnimplementedAuctionServiceServer

	state *AuctionState

	myID int32

	peers map[int32]string // every node incl. self: ID -> address

	monitor *Monitor
}

func (s *Server) isLeader() bool {
	return s.monitor.LeaderID() == s.myID
}

func (s *Server) leaderAddr() string {
	leader := s.monitor.LeaderID()
	if leader == 0 {
		return ""
	}
	return s.peers[leader]
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
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

	if !s.isLeader() {
		return s.forwardBid(s.leaderAddr(), req)
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

	prevBid := s.state.HighestBid

	s.state.HighestBid = int64(req.Amount)

	live := s.monitor.LiveNodes()
	required := len(live) / 2

	acks := 0

	for _, id := range live {
		if id == s.myID {
			continue
		}

		conn, err := dial(s.peers[id])
		if err != nil {
			continue
		}

		rctx, cancel :=
			context.WithTimeout(
				context.Background(),
				rpcTimeout,
			)

		_, err =
			pb.NewAuctionServiceClient(conn).
				ReplicateBid(
					rctx,
					&pb.ReplicateRequest{
						Amount: req.Amount,
					},
				)

		cancel()
		conn.Close()

		if err == nil {
			acks++
		}
	}

	if acks >= required {
		return &pb.BidResponse{
			Status: "SUCCESS",
		}, nil
	}

	s.state.HighestBid = prevBid

	return &pb.BidResponse{
		Status: "EXCEPTION",
	}, nil
}

func (s *Server) Result(
	ctx context.Context,
	req *pb.ResultRequest,
) (*pb.ResultResponse, error) {

	if !s.isLeader() {
		res, err := s.forwardResult(s.leaderAddr(), req)
		if err == nil {
			return res, nil
		}
	}

	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	ended :=
		time.Now().After(s.state.EndTime)

	return &pb.ResultResponse{
		Finished:   ended,
		HighestBid: int32(s.state.HighestBid),
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
	}

	return &pb.ReplicateReply{
		Success: true,
	}, nil
}

func (s *Server) forwardBid(
	addr string,
	req *pb.BidRequest,
) (*pb.BidResponse, error) {

	if addr == "" {
		return &pb.BidResponse{
			Status: "EXCEPTION",
		}, nil
	}

	conn, err := dial(addr)
	if err != nil {
		return &pb.BidResponse{
			Status: "EXCEPTION",
		}, nil
	}
	defer conn.Close()

	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			rpcTimeout,
		)
	defer cancel()

	return pb.NewAuctionServiceClient(conn).Bid(ctx, req)
}

func (s *Server) forwardResult(
	addr string,
	req *pb.ResultRequest,
) (*pb.ResultResponse, error) {

	if addr == "" {
		return nil, fmt.Errorf("no leader known")
	}

	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			rpcTimeout,
		)
	defer cancel()

	return pb.NewAuctionServiceClient(conn).Result(ctx, req)
}
