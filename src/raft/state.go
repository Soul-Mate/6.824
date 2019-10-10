package raft

import (
	"sync/atomic"
)

type State int

const (
	_ State = iota
	Leader
	Candidate
	Follower
)

type raftState struct {
	data *atomic.Value
}

func newRaftState() raftState {
	return raftState{
		data: new(atomic.Value),
	}
}

func (rs raftState) load() State {
	return rs.data.Load().(State)
}

func (rs raftState) store(s State) {
	rs.data.Store(s)
}
