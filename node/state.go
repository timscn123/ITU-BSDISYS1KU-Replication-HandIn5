package main

import (
	"sync"
	"time"
)

type AuctionState struct {
	mu sync.Mutex

	HighestBid int64
	Winner     string

	EndTime time.Time
}
