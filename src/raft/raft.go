package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Log struct {
	Term    int
	Command interface{}
}

type LeaderState struct {
	nextIndex map[int]int
	machIndex map[int]int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state raftState
	timer raftTimer

	currentTerm     uint32
	votedFor        int32
	logs            []Log
	commitIndex     uint32
	lastApplied     uint32
	votes           int32
	voteChan        chan bool
	heartbeatChan   chan bool
	leaderState     LeaderState
	lastLogIndex    int32
	replicaLogCount int32
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	if rf.state.load() == Leader {
		isleader = true
		term = rf.getCurrentTerm()
	} else {
		isleader = false
		term = rf.getCurrentTerm()
	}

	return term, isleader
}

func (rf *Raft) getCurrentTerm() (term int) {
	return int(atomic.LoadUint32(&rf.currentTerm))
}

func (rf *Raft) setCurrentTerm(term int) {
	atomic.StoreUint32(&rf.currentTerm, uint32(term))
}

func (rf *Raft) incrCurrentTerm() {
	atomic.AddUint32(&rf.currentTerm, 1)
}

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

func (rf *Raft) getLastLogTerm() (term int) {
	index := rf.getLastApplied()
	if index <= 0 || index > len(rf.logs) {
		return 0
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.logs[index].Term
}

// getPrevLogIndex 获取Leader前一个日志条目的索引位置
func (rf *Raft) getPrevLogIndex() (prevLogIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.logs) == 0 || rf.lastLogIndex == 0 {
		return -1
	}

	return int(rf.lastLogIndex - 1)
}

// getPrevLogTerm 获取Leader前一个日志索引处的任期号
func (rf *Raft) getPrevLogTerm() (prevLogIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 日志为空 任期号返回-1表示为空
	if len(rf.logs) == 0 || rf.lastLogIndex == 0 {
		return -1
	}

	return rf.logs[rf.lastLogIndex-1].Term
}

// getSendLogEntries 获取AppendEntries RPC要发送的日志条目
// 每个raft peer都有自己需要发送的条目，由leader维护
func (rf *Raft) getSendLogEntries(peer int) (entries []Log) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 找不到raft peer 或者当前leader日志为空的时候不需要复制日志
	// 发送空的AppendEntries RPC
	nextIndex, ok := rf.leaderState.nextIndex[peer]
	if !ok || nextIndex == -1 {
		return
	}

	entries = make([]Log, nextIndex)
	copy(entries, rf.logs)
	return
}

func (rf *Raft) getLeaderCommit() (committed int) {
	return int(atomic.LoadUint32(&rf.commitIndex))
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate Term number
	CandidateID  int // candidate id
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// go func() { rf.voteChan <- true }()

	// 候选人任期小于投票者任期, 拒绝投票
	if args.Term < rf.getCurrentTerm() {
		DPrintf("Follower-%d refused to vote for Candidate-%d (Follower term: %d > Candidate term: %d), \n",
			rf.me, args.CandidateID, rf.getCurrentTerm(), args.Term)
		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = false

		return
	}

	// 如果 RPC 的请求或者响应中包含一个 term T 大于 currentTerm，
	// 则currentTerm赋值为 T，并切换状态为追随者（Follower）（5.1 节）
	// 同意投票请求
	if args.Term > rf.getCurrentTerm() {
		// 重置选举计时器
		go func() { rf.voteChan <- true }()
		DPrintf("Follower-%d vote for Candidate-%d (Follower term: %d < Candidate term: %d), \n",
			rf.me, args.CandidateID, rf.getCurrentTerm(), args.Term)
		rf.setCurrentTerm(args.Term)
		rf.stateTransition(Follower)
		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = true
		return
	}

	// 候选人与投票人任期号相等
	// 投票者没有投过票或者给自己投票
	if rf.getVotedFor() == -1 || args.CandidateID == rf.getVotedFor() {
		// 选举限制 （5.4）
		// candidate日志至少和过半服务器节点一样新
		// 通过比较最后一条日志条目的索引号和任期号来判断谁更新

		// 如果两份日志（候选人和投票人各自存储的日志条目）任期号不同，那么任期号大的更新
		if rf.getLastLogTerm() != args.LastLogTerm {
			// 投票人的最后一条日志条目的任期号大于候选人最新日志的任期号, 拒绝投票
			if rf.getLastLogTerm() > args.LastLogTerm {
				DPrintf("Follower-%d refused to vote for Candidate-%d (Follower term: %d == Candidate term: %d, Follower LastLogTerm: %d > Candidate LastLogTerm: %d), \n",
					rf.me, args.CandidateID, rf.getCurrentTerm(), args.Term, rf.getLastLogTerm(), args.LastLogTerm)
				reply.Term = rf.getCurrentTerm()
				reply.VoteGranted = false
				return
			}

			// 重置选举计时器
			go func() { rf.voteChan <- true }()
			// 同意投票
			DPrintf("Follower-%d vote for Candidate-%d (Follower term: %d == Candidate term: %d, Follower LastLogTerm: %d < Candidate LastLogTerm: %d), \n",
				rf.me, args.CandidateID, rf.getCurrentTerm(), args.Term, rf.getLastLogTerm(), args.LastLogTerm)
			reply.Term = rf.getCurrentTerm()
			reply.VoteGranted = true
			rf.setVotedFor(args.CandidateID)
			return
		}

		// 存储日志位置的任期号相同，比较日志长度（应用到状态机的日志索引，非commited index），长度大的日志较新
		// 投票人的日志长度大于候选人
		if rf.getLastApplied() > args.LastLogIndex {
			DPrintf("Follower-%d refused to vote for Candidate-%d (Follower term: %d == Candidate term: %d, Follower Apply Log Length: %d > Candidate Apply Log Length: %d), \n",
				rf.me, args.CandidateID, rf.getCurrentTerm(), args.Term, rf.getLastApplied(), args.LastLogIndex)
			reply.Term = rf.getCurrentTerm()
			reply.VoteGranted = false
			return
		}

		// 重置选举计时器
		go func() { rf.voteChan <- true }()
		// 同意投票
		DPrintf("Follower-%d vote for Candidate-%d (Follower term: %d == Candidate term: %d, Follower Apply Log Length: %d <= Candidate Apply Log Length: %d), \n",
			rf.me, args.CandidateID, rf.getCurrentTerm(), args.Term, rf.getLastApplied(), args.LastLogIndex)
		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateID)
	}

	DPrintf("Follower-%d (term: %d) alreay voted Candidate-%d, Refused to vote for Candidate-%d (term: %d), \n",
		rf.me, rf.getCurrentTerm(), rf.getVotedFor(), args.CandidateID, args.Term)
	// 投票人投过票了，拒绝再次投票
	reply.Term = rf.getCurrentTerm()
	reply.VoteGranted = false
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	// 向其他服务器发送requestVote RPC
	for i := range rf.peers {
		// 如果是自己不需要在投票了
		if i == rf.me {
			continue
		}

		go func(index int) {
			// 当前candidate在发起收集选票的过程中如果变为了candidate则不需要在进行选票收集了
			if rf.state.load() == Follower {
				return
			}

			args := &RequestVoteArgs{
				Term:         rf.getCurrentTerm(),
				CandidateID:  rf.me,
				LastLogIndex: rf.getLastApplied(),
				LastLogTerm:  rf.getLastLogTerm(),
			}

			reply := &RequestVoteReply{}

			if rf.sendRequestVote(index, args, reply) {
				// 回复者的term大于currentT
				if reply.Term > rf.getCurrentTerm() {
					DPrintf("Candidate-%d (term = %d) -> Follower (term = %d)\n", rf.me, reply.Term, rf.getCurrentTerm())
					// 更新当前服务器的任期
					rf.setCurrentTerm(reply.Term)
					// 切换为follower
					rf.stateTransition(Follower)
					return
				}

				if reply.VoteGranted == true {
					rf.incrVotes()
				}
			}
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	go func() { rf.heartbeatChan <- true }()

	currentTerm := rf.getCurrentTerm()
	// 如果 RPC 的请求或者响应中包含一个 term T 大于 currentTerm，
	// 则currentTerm赋值为 T，并切换状态为追随者（Follower）（5.1 节）
	if args.Term > currentTerm {
		DPrintf("Follower-%d 交换term %d -> %d\n", rf.me, currentTerm, args.Term)
		rf.setCurrentTerm(args.Term)
		rf.stateTransition(Follower)
		reply.Success = true
		reply.Term = currentTerm
		return
	}

	// leader Term < follower Term m,
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		return
	}

	reply.Success = true
	reply.Term = currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	for peerIndex := range rf.peers {
		if peerIndex == rf.me {
			continue
		}

		go func(peer int) {
			args := &AppendEntriesArgs{
				Term:         rf.getCurrentTerm(),
				LeaderId:     rf.me,
				PrevLogIndex: rf.getPrevLogIndex(),
				PrevLogTerm:  rf.getPrevLogTerm(),
				Entries:      rf.getSendLogEntries(peer),
				LeaderCommit: rf.getLeaderCommit(),
			}

			reply := &AppendEntriesReply{}

			for {
				if rf.sendAppendEntries(peer, args, reply) {
					if reply.Term > rf.getCurrentTerm() {
						rf.stateTransition(Follower)
						rf.setCurrentTerm(reply.Term)
						return
					}

					if reply.Success {
						// 更新follower nextIndex 和 machIndex
						rf.mu.Lock()
						rf.leaderState.nextIndex[peer] = rf.leaderState.nextIndex[peer] + 1
						rf.leaderState.machIndex[peer] = rf.leaderState.nextIndex[peer]
						rf.mu.Unlock()

						// 统计追加日志成功数
						atomic.AddInt32(&rf.replicaLogCount, 1)
					} else {

					}

					return
				}
			}

		}(peerIndex)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	// leader 向其他服务器发送 heartbeat RPC
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			args := &AppendEntriesArgs{
				Term: rf.getCurrentTerm(),
			}

			reply := &AppendEntriesReply{}

			if rf.sendAppendEntries(index, args, reply) {
				// 回复者的term大于currentT
				if reply.Term > rf.getCurrentTerm() {
					// 赋值term
					rf.setCurrentTerm(reply.Term)

					// 切换为follower
					rf.stateTransition(Follower)
					return
				}
			} else {
				DPrintf("Leader-%d 发送给 Server-%d heartbeat rpc 失败\n", rf.me, index)
			}
		}(i)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.state.load() != Leader {
		isLeader = false
		return index, term, isLeader
	}

	// 追加命令到日志中
	rf.mu.Lock()
	rf.logs = append(rf.logs, Log{
		Command: command,
		Term:    rf.getCurrentTerm(),
	})
	rf.lastLogIndex += 1
	rf.mu.Unlock()

	// 发起RPC
	rf.broadcastAppendEntries()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) startElection() {
	// 递增当前任期
	rf.incrCurrentTerm()

	// 给自己投票
	rf.setVotes(1)

	// 重置选举计时器
	rf.timer.reset()

	// 发起选举请求，收集选票
	rf.broadcastRequestVote()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = newRaftState()
	rf.state.store(Follower)

	// 初始化currentTerm
	atomic.StoreUint32(&rf.currentTerm, 0)

	// 初始化votedFor (用-1表示nil)
	atomic.StoreInt32(&rf.votedFor, -1)

	// 初始化votes
	atomic.StoreInt32(&rf.votes, 0)

	// 初始化commitIndex
	atomic.StoreUint32(&rf.commitIndex, 0)

	// 初始化lastApplied
	atomic.StoreUint32(&rf.lastApplied, 0)

	// 初始化logs
	rf.logs = make([]Log, 0, 0)

	rf.lastLogIndex = 0

	rf.voteChan = make(chan bool)
	rf.heartbeatChan = make(chan bool)

	go func() {
		rf.timer = newRaftTimer()
	L:
		for {
			switch rf.state.load() {
			case Follower: // 跟随者状态
				select {
				case <-rf.timer.C(): // 计时器到期
					// 转变状态为候选人
					rf.stateTransition(Candidate)
					DPrintf("Follower-%d (term = %d) -> Candidate, start election\n", rf.me, rf.getCurrentTerm())
					break
				case <-rf.voteChan: // 收到candidate投票rpc
					//DPrintf("server-%d 收到投票rpc, 重置选举计时器\n", rf.me)
					rf.timer.reset()
					break
				case <-rf.heartbeatChan: // 收到leader心跳rpc
					//DPrintf("server-%d 收到心跳rpc, 重置选举计时器\n", rf.me)
					rf.timer.reset()
					break
				default:
				}

			case Candidate: // 候选人状态
				// 开始选举
				rf.startElection()

				done := false
				for {
					// 如果在交换term的时候或者选举过程中收到leader的heartbeat，从而变为follower, 则不再进行选举
					if rf.state.load() == Follower {
						break
					}

					select {
					case <-rf.timer.C(): //选举超时
						DPrintf("Candidate-%d (term = %d) election timeout restart election\n", rf.me, rf.getCurrentTerm())
						// 重新开始新一轮选举
						rf.startElection()

					default: // 不断统计选票, 直到超过半数
						if rf.getVotes() > len(rf.peers)/2 {
							DPrintf("Candidate-%d (term = %d) -> Leader get votes = %d\n", rf.me, rf.getCurrentTerm(), rf.getVotes())
							// 选举成为leader
							rf.state.store(Leader)

							done = true
							break
						}
					}

					if done {
						break
					}
				}

			case Leader:
				for {
					// Leader在和其他server通信的过程中发现自己term落后，遂变成follower
					// 此时Leader不需要在发送heartbeat
					if rf.state.load() == Follower {
						DPrintf("Leader-%d (term = %d) -> Follower\n", rf.me, rf.getCurrentTerm())
						goto L
					}

					// 发送heartbeat
					DPrintf("Leader-%d (term = %d) broadcast heartbeat rpc\n", rf.me, rf.getCurrentTerm())
					rf.broadcastHeartbeat()
					time.Sleep(time.Duration(5) * time.Millisecond)
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
