package main

import (
	"sync"
	"time"
)

type AuctionState struct {
	mu sync.Mutex

	HighestBid int64

	EndTime time.Time
}
