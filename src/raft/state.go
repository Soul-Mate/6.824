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

func (rf *Raft) stateTransition(state State) {
	switch state {
	case Follower:
		rf.state.store(state)
		rf.setVotes(-1)
	case Candidate:
		rf.state.store(state)
		rf.setVotes(0)
	case Leader:
		rf.state.store(state)

		// each follower
		rf.mu.Lock()
		for peerIndex := range rf.peers {
			rf.leaderState.nextIndex[peerIndex] = int(rf.lastLogIndex + 1)
			rf.leaderState.machIndex[peerIndex] = 0
		}
		rf.mu.Unlock()
	}
}
