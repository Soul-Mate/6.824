package raft

import "sync/atomic"

func (rf *Raft) getVotedFor() (votedFor int) {
	return int(atomic.LoadInt32(&rf.votedFor))
}

func (rf *Raft) setVotedFor(candidateID int) {
	atomic.StoreInt32(&rf.votedFor, int32(candidateID))
}

func (rf *Raft) getVotes() (votes int) {
	return int(atomic.LoadInt32(&rf.votes))
}

func (rf *Raft) setVotes(n int) {
	atomic.StoreInt32(&rf.votes, int32(n))
}

func (rf *Raft) incrVotes() {
	atomic.AddInt32(&rf.votes, 1)
}

func (rf *Raft) getLastApplied() (lastApplied int) {
	return int(atomic.LoadUint32(&rf.lastApplied))
}


func (rf *Raft) getCommitIndex() (committed int) {
	return int(atomic.LoadInt32(&rf.commitIndex))
}

func (rf *Raft) setCommitIndex(index int) {
	atomic.StoreInt32(&rf.commitIndex, int32(index))
}

func (rf *Raft) incrmenetCommitIndex() {
	atomic.AddInt32(&rf.commitIndex, 1)
}