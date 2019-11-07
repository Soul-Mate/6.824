package raft

import (
	"log"
	"time"
	"math/rand"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// raft time
const (
	minElectionTimeout = 150
	maxElectionTimeout = 300

	minBroadcastTimeout = 1
	maxBroadcastTimeout = 20
)


func randElectionMillisecond() time.Duration {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(maxElectionTimeout-minElectionTimeout) + minElectionTimeout
	return time.Duration(n) * time.Millisecond
}

func randBroadcastMillisecond() time.Duration {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(maxBroadcastTimeout-minBroadcastTimeout) + minBroadcastTimeout
	return time.Duration(n) * time.Millisecond
}