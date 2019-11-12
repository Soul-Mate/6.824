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
	"labrpc"
	"sync"
	"sync/atomic"
	"time"
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

func (rs raftState) String(s State) string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unkown"
	}
}

func (rf *Raft) stateTransition(state State) {
	prevState := rf.state.load()

	switch state {
	case Follower:
		// 可能有的状态转换:
		//	Follower -> Follower
		//		不需要特殊处理
		//	Candidate -> Follower
		//		说明其他Candidate当选, // TODO 清空投票信息?
		// 	Leader -> Follower 
		//		说明Leader term变小了，此时需要唤醒electionLoop

		rf.state.store(state)
		rf.setVotes(-1) // 清空选票
			
		// Leader -> Follower
		if prevState == Leader {
			// 清空复制日志统计
			rf.setReplicated(0)

			DPrintf("[rf.stateTransition] Leader-%d convert Follower\n", rf.me)
			rf.leaderBecomeFollwerCh <- true // 唤醒electionLoop
		}

	case Candidate:
		// 可能有的状态转换:
		//	Follower -> Candidate
		//		说明Follower选举计时器超时，开始选举
		rf.state.store(state)
		// 不需要清空选票, Candidate在选举阶段会自动给自己投票，
		// 将votes = 1

	case Leader: 
		// 可能有的状态转换:
		// 	Candidate -> Leader
		//		说明Candidate当选，需要唤醒leaderLoop, 初始化[]nextIndex和[]matchIndex
		rf.state.store(state)
		// 初始化leader server上的nextIndex和matchIndex
		rf.mu.Lock()
		for index := range rf.peers {
			if len(rf.logs) == 0 {
				rf.leaderState.machIndex[index] = -1
				rf.leaderState.nextIndex[index] = 0
			} else {
				// leader将所有nextIndex的值都初始化为自己最后一个日志条目的index加1
				// 实际上的日志末尾值为 len(rf.logs) - 1， 这里之所以不写为 len(rf.logs)，
				// 是因为想用代码体现出字面表达的意思
				rf.leaderState.nextIndex[index] = len(rf.logs) - 1 + 1
				// matchIndex is used for safety. It is a conservative measurement of what prefix of the log the
				// leader shares with a given follower. matchIndex cannot ever be set to a value that is too high, as
				// this may cause the commitIndex to be moved too far forward. This is why matchIndex is initialized
				// to -1 (i.e., we agree on no prefix), and only updated when a follower positively acknowledges an
				// AppendEntries RPC.
				rf.leaderState.machIndex[index] = -1
			}
		}
		rf.mu.Unlock()

		if prevState == Candidate {
			DPrintf("[rf.stateTransition] Candidate-%d (term = %d) convert Leader (get votes = %d)\n",
						rf.me, rf.getCurrentTerm(), rf.getVotes())
			rf.candidateBecomeLeaderCh <- true
		}
	}
}

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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	// persisten state on all servers
	currentTerm uint32
	votedFor    int32
	logs        []Log

	// volatile state on all servers
	commitIndex int32
	lastApplied uint32

	// volatile state on leaders
	leaderState LeaderState

	// RequestVote RPC need
	votes int32 // 统计选票

	// election timeout need
	leaderStepDownCond     *sync.Cond // leader 退位condition
	startElectionCh        chan bool  // 开始发起选举channel
	resetElectionTimeoutCh chan bool  // 通知leader取消选举超时channel
	leaderTookOfficeCond   *sync.Cond // leader 上任condition
	startHeartbeatCh       chan bool  // 开始发送心跳channel
	commitCh               chan bool  // 提交日志channel

	leaderStepdownCh   chan bool
	leaderTookOfficeCh chan bool

	candidateBecomeLeaderCh chan bool
	leaderBecomeFollwerCh chan bool

	state raftState
	timer time.Timer

	replicated uint32
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

func (rf *Raft) incrReplicated() {
	atomic.AddUint32(&rf.replicated, 1)
}

func (rf *Raft) getReplicated() (replicated int) {
	return int(atomic.LoadUint32(&rf.replicated))
}

func (rf *Raft) setReplicated(replicated int) {
	atomic.StoreUint32(&rf.replicated, uint32(replicated))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	term = rf.getCurrentTerm()

	// Your code here (2A).
	if rf.state.load() == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
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

func (rf *Raft) incrLastApplied() {
	atomic.AddUint32(&rf.lastApplied, 1)
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
	if rf.getCurrentTerm() <= args.Term {
		// Rules for Servers
		// All Servers:
		//	If RPC request or respose contains term T > currentTerm:
		//	set currentTerm = T, convert to follower (#5.1)
		if rf.getCurrentTerm() < args.Term {
			rf.setCurrentTerm(args.Term)
			rf.stateTransition(Follower)
			rf.setVotedFor(-1)
		}
		
		// Check 2. If votedFor is null or candidatedId, and candidate's log is at
		// 			least as up-to-date as receiver's log, grant vote (#5.2, #5.4)

		if rf.getVotedFor() == -1 || args.CandidateID == rf.getVotedFor() {
			rf.mu.Lock()
			lastLogIndex := len(rf.logs) - 1
			lastLogTerm := 0 
			if lastLogIndex >= 0 {
				lastLogTerm = rf.logs[lastLogIndex].Term
				// rf.resetElectionTimeoutCh <- true
				// rf.setVotedFor(args.CandidateID)
				// rf.stateTransition(Follower)
				// reply.Term = rf.getCurrentTerm()
				// reply.VoteGranted = true
				// DPrintf("[rf.RequestVote] %s-%d (term = %d) vote to Candidate-%d (term = %d), when lastLogIndex < 0\n",
				// 	rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm(), args.CandidateID, args.Term)
			}
			rf.mu.Unlock()
			
			DPrintf("lastLogTerm = %d, args.LastLogTerm = %d\n", lastLogTerm, args.LastLogTerm)

			if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
				rf.resetElectionTimeoutCh <- true
				rf.stateTransition(Follower)
				rf.setVotedFor(args.CandidateID)
				reply.Term = rf.getCurrentTerm()
				reply.VoteGranted = true
				// DPrintf("[rf.RequestVote] %s-%d (term = %d) vote to Candidate-%d (term = %d)\n",
					// rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm(), args.CandidateID, args.Term)
				return
			}
		}

	} else {
		// Your code here (2A, 2B).
		// Check 1. Reply false if term < currentTerm (#5.1)
		DPrintf("[rf.RequestVote]%s-%d (term = %d) refused to vote for Candidate-%d (term = %d)\n",
			rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm(), args.CandidateID, args.Term)
		rf.setVotedFor(-1)
		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = false
	}
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
			// 当前candidate在发起收集选票的过程中如果变为了follower
			// 则不需要在进行选票收集了
			if rf.state.load() == Follower {
				return
			}

			rf.mu.Lock()
			lastLogTerm := 0
			lastLogIndex := len(rf.logs) - 1
			if lastLogIndex >= 0 {
				lastLogTerm = rf.logs[lastLogIndex].Term
			}
			rf.mu.Unlock()

			args := &RequestVoteArgs{
				Term:         rf.getCurrentTerm(),
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			reply := &RequestVoteReply{}

			if rf.sendRequestVote(index, args, reply) {
				// 回复者的term大于currentT
				if reply.Term > rf.getCurrentTerm() {
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
	// Rules for Servers

	if rf.getCurrentTerm() <= args.Term {
		// All Servers:
		// 	If RPC request or response contains term T > currentTerm:
		// 	set currentTerm = T, convert to follower (#5.1)
		if rf.getCurrentTerm() < args.Term {
			DPrintf("[rf.AppendEntries] %s-%d (term = %d) set term = %d\n",
				rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm(), args.Term)
			rf.setCurrentTerm(args.Term)
			rf.stateTransition(Follower)
		}
		// TODO heartbeat
		if len(args.Entries) == 0 && args.PrevLogIndex < 0 {
			rf.resetElectionTimeoutCh <- true
			rf.stateTransition(Follower)
			// DPrintf("%s-%d (term = %d) receive Leader-%d (term = %d) heartbeat\n",
			// 	rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm(), args.LeaderId, args.Term)

			reply.Success = true
			reply.Term = rf.getCurrentTerm()
			return
		}

		// Check 2. Reply false if log doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm (# 5.3)
		rf.mu.Lock()
		if len(rf.logs) < args.PrevLogIndex {
			rf.mu.Unlock()
			reply.Success = false
			reply.Term = rf.getCurrentTerm()
			return
		}
		rf.mu.Unlock()

		// Check 3. If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (# 5.3)

		// 要让follower的日志和自己一致，raft paper中对于日志复制这样描述：
		// 	1.leader必须找到两者达成一致的最大的日志条目（索引最大）
		// 	2. 删除follower日志中从那个点之后的所有日志
		//	3. 将自己从那个点之后的所有日志条目复制给follower (在这里就是要复制这些日志)
		rf.mu.Lock()
		if args.PrevLogIndex >= 0 {

		
		DPrintf("[rf.AppendEntires] args.PrevLogIndex = %d, len(rf.logs) = %d, rf.logs[args.PrevLogIndex].Term = %d, args.PrevLogTerm = %d\n", 
			args.PrevLogIndex, len(rf.logs),  rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		}
		if args.PrevLogIndex >= 0 && len(rf.logs) >= args.PrevLogIndex && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
			rf.resetElectionTimeoutCh <- true
			rf.stateTransition(Follower)

			DPrintf("[rf.AppendEntires] Follower-%d replica log...\n", rf.me)
			// Check 4. Append any new entries not already in the log
			// 删除follower日志中从那个点之后的所有日志
			rf.logs = rf.logs[:args.PrevLogIndex+1]

			// 将自己从那个点之后的所有日志条目复制给follower (在这里就是要复制这些日志)
			rf.logs = append(rf.logs, args.Entries...)

			// Check 5. If leaderCommit > commitIndex, set commitIndex =
			// min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.getCommitIndex() {
				if args.LeaderCommit > len(rf.logs) {
					rf.setCommitIndex(len(rf.logs))
				} else {
					rf.setCommitIndex(args.LeaderCommit)
				}
			}

			rf.mu.Unlock()

			rf.commitCh <- true

			// 接收者追加成功
			DPrintf("Follower-%d exchange term %d -> %d\n", rf.me, rf.getCurrentTerm(), args.Term)
			reply.Success = true
			reply.Term = rf.getCurrentTerm()
			return
		}

	} else {
		// Figure 2.
		// AppendEntries RPC Receiver implementation
		// Check 1. Reply false if term < currentTerm (# 5.1)
		reply.Success = false
		reply.Term = rf.getCurrentTerm()
		// DPrintf("[rf.AppendEntries] %s-%d (term = %d) refused Leader-%d (term = %d) request\n",
		// 	rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm(), args.LeaderId, args.Term)
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries(index int, term int, name string) {
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	majority := len(rf.peers) / 2
	majorityReplicaed := false
	for peerIndex := range rf.peers {
		// 自己不需要复制日志
		if peerIndex == rf.me {
			continue
		}

		go func(peer int) {
			// 发送过程中可能遇到follower运行缓慢，网络丢包延迟，需要不断重试AppendEntries RPC
			for {
				// 如果不是leader，则不再继续执行
				if _, isLeader := rf.GetState(); !isLeader {
					// DPrintf("[AppendEntries RPC] Leader-%d send to %d, but it is'n leader\n", rf.me, peer)
					return
				}

				entries := make([]Log, 0)

				rf.mu.Lock()
				// index指向日志末尾，index = len(rf.Logs) - 1
				// index + 1 指向日志末尾之外
				// nextIndex 指向index + 1所在的位置
				// Rule for Servers indicate：
				// Leaders: If last log index >= nextIndex for a follower: send
				// AppendEntries RPC with log entries starting at nextIndex
				nextIndex := rf.leaderState.nextIndex[peer]
				// leader的总体日志长度 >= 要发送给follower的下一个日志位置
				// 复制nextIndex ~ index+1 的位置发送给follower
				// 否则发送给follower一个空的entries
				if index+1 >= nextIndex {
					entries = rf.logs[nextIndex : index+1]
				}

				// index of log entry immediately preceding new ones
				prevLogIndex := nextIndex - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 {
					prevLogTerm = rf.logs[prevLogIndex].Term
				}

				// term of prevLogIndex entry
				rf.mu.Unlock()

				args := &AppendEntriesArgs{
					Term:         rf.getCurrentTerm(),
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.getCommitIndex(),
				}

				reply := &AppendEntriesReply{}

				// 防止提交不同任期的日志
				if rf.getCurrentTerm() != term {
					DPrintf("[rf.broadcastAppendEntries] Leader-%d send to Follower-%d, "+
						"the log append term when sending is different from the leader currentTerm \n", rf.me, peer)
					return
				}

				DPrintf("[rf.broadcastAppendEntries] %s-%d send to Follower-%d\n",
					rf.state.String(rf.state.load()), rf.me, peer)

				if rf.sendAppendEntries(peer, args, reply) {
					// Rules for Servers:
					// if RPC request or response contains term T > currentTerm:
					// set currentTerm = T, convert to follower (#5.1)
					if reply.Term > rf.getCurrentTerm() {
						rf.setCurrentTerm(reply.Term)
						rf.stateTransition(Follower)
						return
					}

					// Rules for Servers:
					// Leaders:
					// 		if successful: update nextIndex and matchIndex for follower (#5.3)
					//		if AppendEntries fails because of log inconsistency:
					//		decrement nextIndex and retry (#5.3)
					if reply.Success {
						rf.mu.Lock()
						if rf.leaderState.nextIndex[peer] < index+1 {
							// 当follower追加日志成功，更新 macthIndex
							rf.leaderState.machIndex[peer] = prevLogIndex + len(entries)
							rf.leaderState.nextIndex[peer] = rf.leaderState.machIndex[peer] + 1
						}

						// follower成功复制日志，增加复制数量
						rf.incrReplicated()

						if majorityReplicaed != true && rf.getReplicated() >= majority {
							majorityReplicaed = true
						}

						// Rules for Server
						// Leaders:
						//	If there exists an N such that N > commitIndex, a majority
						//	of matchIndex[i] >= N, and log[N].term == currentTerm:
						//	set commitIndex = N (#5.3, #5.4).
						if _, isLeader := rf.GetState(); isLeader && rf.getCurrentTerm() == term && majorityReplicaed {
							rf.setCommitIndex(index)
							rf.commitCh <- true
							// TODO 通知其他follower更新commiIndex?
						}
						rf.mu.Unlock()

						return
					}

					// TODO conflict
					rf.mu.Lock()
					if rf.leaderState.nextIndex[peer] > 0 {
						rf.leaderState.nextIndex[peer]--
					}
					rf.mu.Unlock()
				}
			}
		}(peerIndex)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	if _, isLeader := rf.GetState(); isLeader {
		rf.mu.Lock()
		index := len(rf.logs) - 1
		rf.mu.Unlock()
		rf.broadcastAppendEntries(index, rf.getCurrentTerm(), "heartbeat")
	}

	// leader 向其他服务器发送 heartbeat RPC
	// for i := range rf.peers {
	// 	if i == rf.me {
	// 		continue
	// 	}
	// 	go func(index int) {
	// 		args := &AppendEntriesArgs{
	// 			Term: rf.getCurrentTerm(),
	// 		}

	// 		reply := &AppendEntriesReply{}

	// 		if rf.sendAppendEntries(index, args, reply) {
	// 			// 回复者的term大于currentT
	// 			if reply.Term > rf.getCurrentTerm() {
	// 				// 赋值term
	// 				rf.setCurrentTerm(reply.Term)

	// 				// 切换为follower
	// 				rf.stateTransition(Follower)
	// 				return
	// 			}
	// 		} else {
	// 			DPrintf("Leader-%d 发送给 Server-%d heartbeat rpc 失败\n", rf.me, index)
	// 		}
	// 	}(i)
	// }
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

	if term, isLeader = rf.GetState(); isLeader {
		// 追加命令到日志中
		rf.mu.Lock()
		rf.logs = append(rf.logs, Log{
			Command: command,
			Term:    term,
		})

		// index 指向日志末尾, nextIndex指向末尾之外
		index = len(rf.logs) - 1
		rf.mu.Unlock()

		// 统计复制日志的服务器，自己也是其中之一
		rf.incrReplicated()
		// rf.mu.Lock()
		// replicated := 1
		// // 发起日志复制时需要把当前任期的term传递
		// go rf.broadcastAppendEntries(index, term, replicated, "replica log")
		// rf.mu.Unlock()
	}

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
	// Candidates(#5.2)
	// On conversion to candidate, start election:
	//	Increment currentTerm
	//  Vote for self
	// 	Reset election timer
	//	Send RequestVote RPCs to all other servers

	// Increment currentTerm
	rf.incrCurrentTerm()

	// Vote for self
	rf.setVotes(1)

	// Reset election timer
	rf.resetElectionTimeoutCh <- true

	// Send RequestVote RPCs to all other servers
	rf.broadcastRequestVote()
}

// electionLoop 周期性的登台follower选举计时器超时
func (rf *Raft) electionLoop() {
	timer := time.NewTimer(randElectionMillisecond())
L:
	for {
		// 在peer是leeader的情况下，不需要选举计时器
		// 进入condition等待，直到leader退任
		if _, isLeader := rf.GetState(); isLeader {
			<-rf.leaderBecomeFollwerCh
		} else {
			// 在peer不是leader的情况下
			// 等待心跳、投票事件，重置选举计时器
			// 选举计时器，超时要进行选举
			select {
			case <-timer.C: // 选举计时器到期, 进行选举
				if _, isLeader := rf.GetState(); isLeader {
					goto L
				}

		

				// 开始选举
				rf.stateTransition(Candidate)

				// Increment currentTerm
				rf.incrCurrentTerm()

				// Vote for self
				rf.setVotes(1)

				// Reset election timer
				timer.Reset(randElectionMillisecond())

				// Send RequestVote RPCs to all other servers
				rf.broadcastRequestVote()

				DPrintf("[rf.electionLoop] %s-%d election timeout, start election... (term = %d)\n",
					rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm())
				
				for {
					if rf.state.load() != Candidate {
						break
					}

					select {
					case <- timer.C:
							// Increment currentTerm
						rf.incrCurrentTerm()

						// Vote for self
						rf.setVotes(1)

						// Reset election timer
						timer.Reset(randElectionMillisecond())

						// Send RequestVote RPCs to all other servers
						rf.broadcastRequestVote()
		
						// DPrintf("[rf.electionLoop] %s-%d election timeout, relection (term = %d)\n",
						// 	rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm())
	
					default:
						if rf.getVotes() > len(rf.peers) / 2 {
							
							rf.stateTransition(Leader)
						}
					}
				}

			case <-rf.resetElectionTimeoutCh: // 收到了leader发送的AppendEntries 或 Candidate投票请求，重置选举计时器
				timer.Reset(randElectionMillisecond())
			}
		}
	}
}

func (rf *Raft) leaderLoop() {
	timer := time.NewTimer(randBroadcastMillisecond())
L:
	for {
		if _, isLeader := rf.GetState(); !isLeader {
			<-rf.candidateBecomeLeaderCh
		} else {
			<-timer.C
			// 在定时器到期过程中不在是leader了
			// 那么后续则不在处理了
			if _, isLeader := rf.GetState(); !isLeader {
				goto L
			}

			rf.broadcastHeartbeat()

			timer.Reset(randBroadcastMillisecond())
		}
	}
}

// periodHeartbeat 周期性的通知leader发送heartbeat rpc
func (rf *Raft) periodHeartbeat() {
	timer := time.NewTimer(randBroadcastMillisecond())
	for {
		// 不是leader的情况下不需要定期向其他peer发送心跳信息
		// 等待leader上任
		if _, isLeader := rf.GetState(); !isLeader {
			<-rf.leaderTookOfficeCh
			// rf.mu.Lock()
			// rf.leaderTookOfficeCond.Wait()
			// rf.mu.Unlock()
			// continue
		} else {
			select {
			case <-timer.C:
				<-timer.C
				rf.startHeartbeatCh <- true
				timer.Reset(randBroadcastMillisecond())
			}
		}
	}
}

func (rf *Raft) commit() {
	for {
		// Rules for Servers:
		// All Servers:
		// 	if commitIndex > lastApplied; increment lastApplied, apply
		//  log[lastApplied] to state machine (#5.3)
		select {
		case <-rf.commitCh:
			if rf.getCommitIndex() > rf.getLastApplied() {
				for {
					if rf.getCommitIndex() <= rf.getLastApplied() {
						break
					}

					rf.incrLastApplied()

					rf.mu.Lock()
					rf.applyCh <- ApplyMsg{
						Index:   rf.getLastApplied(),
						Command: rf.logs[rf.getLastApplied()].Command,
					}
					rf.mu.Unlock()
				}
			}
		}
	}
}

// loop raft事件循环
func (rf *Raft) loop() {
L:
	for {
		switch rf.state.load() {
		case Follower:
			select {
			case <-rf.startElectionCh:
				rf.stateTransition(Candidate)
				break
			default:
			}

		case Candidate:
			DPrintf("[rf.loop] %s-%d election timeout, start election... (term = %d)\n",
				rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm())

			// 开始选举
			rf.startElection()

			for {
				if rf.state.load() != Candidate {
					break
				}

				select {
				case <-rf.startElectionCh:
					rf.startElection()

				default:
					if rf.getVotes() > len(rf.peers)/2 {

						DPrintf("[rf.loop] %s-%d (term = %d) convert Leader (votes = %d)\n",
							rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm(), rf.getVotes())

						// 转换为leader
						rf.stateTransition(Leader)

						goto L
						// 立即发送心跳信息，防止选举超时
						// rf.broadcastHeartbeat()
					}
				}
			}
		case Leader:
			timer := time.NewTimer(randBroadcastMillisecond())
			for {
				if _, isLeader := rf.GetState(); !isLeader {
					timer.Stop()
					goto L
				}

				<-timer.C
				rf.broadcastHeartbeat()
				timer.Reset(randBroadcastMillisecond())
			}
		}
	}

	// L:
	// 	for {
	// 		select {
	// 		case <-rf.startElectionCh: // 收到选举channel
	// 			DPrintf("%s-%d start election\n", rf.state.String(rf.state.load()), rf.me)

	// 			// 转变状态
	// 			rf.stateTransition(Candidate)

	// 			// 开始选举
	// 			rf.startElection()

	// 			// 等待选举结果或选举超时重新开始新一轮的选举
	// 			for {
	// 				if rf.state.load() != Candidate {
	// 					break
	// 				}

	// 				select {
	// 				case <-rf.startElectionCh: // 选举超时
	// 					// 开始新一轮选举
	// 					rf.startElection()
	// 				default:
	// 					if rf.getVotes() > len(rf.peers)/2 {
	// 						DPrintf("Candidate-%d (term = %d) -> Leader get votes = %d\n", rf.me, rf.getCurrentTerm(), rf.getVotes())
	// 						// 转换为leader
	// 						rf.stateTransition(Leader)

	// 						// 立即发送心跳信息，防止选举超时
	// 						// rf.broadcastHeartbeat()

	// 						goto L
	// 					}
	// 				}
	// 			}
	// 		case <-rf.startHeartbeatCh: // 收到心跳发送channel
	// 			DPrintf("[rf.loop] %s-%d (term = %d) broadcast heartbeat\n", rf.state.String(rf.state.load()), rf.me, rf.getCurrentTerm())

	// 			rf.broadcastHeartbeat()
	// 		}
	// 	}
	// 	rf.timer = newRaftTimer()
	// L:
	// 	for {
	// 		switch rf.state.load() {
	// 		case Follower: // 跟随者状态
	// 			select {
	// 			case <-rf.timer.C(): // 计时器到期
	// 				// 转变状态为候选人
	// 				rf.stateTransition(Candidate)
	// 				DPrintf("Follower-%d (term = %d) -> Candidate, start election\n", rf.me, rf.getCurrentTerm())
	// 				break
	// 			case <-rf.voteChan: // 收到candidate投票rpc
	// 				//DPrintf("server-%d 收到投票rpc, 重置选举计时器\n", rf.me)
	// 				rf.timer.reset()
	// 				break
	// 			case <-rf.heartbeatChan: // 收到leader心跳rpc
	// 				//DPrintf("server-%d 收到心跳rpc, 重置选举计时器\n", rf.me)
	// 				rf.timer.reset()
	// 				break
	// 			default:
	// 			}

	// 		case Candidate: // 候选人状态
	// 			// 开始选举
	// 			rf.startElection()

	// 			done := false
	// 			for {
	// 				// 如果在交换term的时候或者选举过程中收到leader的heartbeat，从而变为follower, 则不再进行选举
	// 				if rf.state.load() == Follower {
	// 					break
	// 				}

	// 				select {
	// 				case <-rf.timer.C(): //选举超时
	// 					DPrintf("Candidate-%d (term = %d) election timeout restart election\n", rf.me, rf.getCurrentTerm())
	// 					// 重新开始新一轮选举
	// 					rf.startElection()

	// 				default: // 不断统计选票, 直到超过半数
	// 					if rf.getVotes() > len(rf.peers)/2 {
	// 						DPrintf("Candidate-%d (term = %d) -> Leader get votes = %d\n", rf.me, rf.getCurrentTerm(), rf.getVotes())
	// 						// 选举成为leader
	// 						rf.state.store(Leader)

	// 						done = true
	// 						break
	// 					}
	// 				}

	// 				if done {
	// 					break
	// 				}
	// 			}

	// 		case Leader:
	// 			for {
	// 				// Leader在和其他server通信的过程中发现自己term落后，遂变成follower
	// 				// 此时Leader不需要在发送heartbeat
	// 				if rf.state.load() == Follower {
	// 					DPrintf("Leader-%d (term = %d) -> Follower\n", rf.me, rf.getCurrentTerm())
	// 					goto L
	// 				}

	// 				// 发送heartbeat
	// 				DPrintf("Leader-%d (term = %d) broadcast heartbeat rpc\n", rf.me, rf.getCurrentTerm())
	// 				rf.broadcastHeartbeat()
	// 				time.Sleep(time.Duration(5) * time.Millisecond)
	// 			}
	// 		}
	// }
}

// Make the service or tester wants to create a Raft server. the ports
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
	// 初始化 apply channel
	rf.applyCh = make(chan ApplyMsg)

	// 初始化currentTerm，从0开始递增，使用原子操作
	atomic.StoreUint32(&rf.currentTerm, 0)

	// 初始化votedFor，用-1表示null，使用原子操作
	atomic.StoreInt32(&rf.votedFor, -1)

	// 初始化logs，默认为空
	rf.logs = make([]Log, 0)

	atomic.StoreInt32(&rf.commitIndex, 0)
	atomic.StoreUint32(&rf.lastApplied, 0)

	rf.leaderState.nextIndex = make(map[int]int)
	rf.leaderState.machIndex = make(map[int]int)

	atomic.StoreInt32(&rf.votes, 0)

	rf.leaderStepDownCond = sync.NewCond(&rf.mu)
	rf.leaderTookOfficeCond = sync.NewCond(&rf.mu)
	rf.startElectionCh = make(chan bool)
	rf.resetElectionTimeoutCh = make(chan bool)
	rf.startHeartbeatCh = make(chan bool)
	rf.leaderStepdownCh = make(chan bool)
	rf.leaderTookOfficeCh = make(chan bool)

	rf.leaderBecomeFollwerCh = make(chan bool)
	rf.candidateBecomeLeaderCh = make(chan bool)

	rf.commitCh = make(chan bool)

	atomic.StoreUint32(&rf.replicated, 0)

	rf.state = newRaftState()
	rf.state.store(Follower)
	
	// 启动 raft 事件主循环
	// go rf.loop()

	go rf.electionLoop()

	go rf.leaderLoop()

	// 启动 raft 提交日志循环
	go rf.commit()

	// go rf.electionLoop()

	// go rf.periodHeartbeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
