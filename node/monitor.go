package main

import (
	"context"
	"log"
	"sync"
	"time"

	pb "auction/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	heartbeatInterval = 1 * time.Second

	// how many failed beat before a node is declared "dead"
	failureTimeout = 3 * time.Second

	heartbeatTimeout = 1 * time.Second
)

type Monitor struct {
	mu       sync.Mutex
	myID     int32
	peers    map[int32]string
	lastSeen map[int32]time.Time
	failed   map[int32]bool
}

func NewMonitor(myID int32, peers map[int32]string) *Monitor {
	return &Monitor{
		myID:     myID,
		peers:    peers,
		lastSeen: make(map[int32]time.Time),
		failed:   make(map[int32]bool),
	}
}

func (m *Monitor) Start() {
	log.Printf(
		"node %d: heartbeats started (every %s, failed after %s of silence)",
		m.myID,
		heartbeatInterval,
		failureTimeout,
	)

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for range ticker.C {
			m.tick()
		}
	}()
}

func (m *Monitor) tick() {
	for id, addr := range m.peers {
		if id == m.myID {
			continue
		}
		m.probe(addr, id)
	}
	m.checkFailures()
}

func (m *Monitor) probe(addr string, id int32) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		heartbeatTimeout,
	)
	defer cancel()

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return
	}
	defer conn.Close()

	_, err = pb.NewAuctionServiceClient(conn).Heartbeat(
		ctx,
		&pb.HeartbeatRequest{NodeId: m.myID},
	)
	if err == nil {
		m.MarkAlive(id)
	}
}

func (m *Monitor) MarkAlive(id int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSeen[id] = time.Now()
}

func (m *Monitor) IsAlive(id int32) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.aliveLocked(id, time.Now())
}

func (m *Monitor) LiveNodes() []int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	live := make([]int32, 0, len(m.peers))
	for id := range m.peers {
		if m.aliveLocked(id, now) {
			live = append(live, id)
		}
	}
	return live
}

func (m *Monitor) LeaderID() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var best int32
	for id := range m.peers {
		if !m.aliveLocked(id, now) {
			continue
		}
		if best == 0 || id < best {
			best = id
		}
	}
	return best
}

func (m *Monitor) aliveLocked(id int32, now time.Time) bool {
	seen, ok := m.lastSeen[id]
	if !ok {
		return true
	}
	return now.Sub(seen) < failureTimeout
}

func (m *Monitor) checkFailures() {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	for id, addr := range m.peers {
		if id == m.myID {
			continue
		}

		seen, ok := m.lastSeen[id]
		if !ok {
			continue
		}

		dead := now.Sub(seen) >= failureTimeout
		switch {
		case dead && !m.failed[id]:
			m.failed[id] = true
			log.Printf(
				"node %d: node %d (%s) FAILED - no heartbeat for %s",
				m.myID,
				id,
				addr,
				now.Sub(seen),
			)
		case !dead && m.failed[id]:
			m.failed[id] = false
			log.Printf(
				"node %d: node %d (%s) is back",
				m.myID,
				id,
				addr,
			)
		}
	}
}
