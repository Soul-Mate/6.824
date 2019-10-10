package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
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
	term    int
	command interface{}
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state       raftState
	timer       raftTimer

	currentTerm int
	votedFor    int
	logs        []Log
	commitIndex int
	lastApplied int

	electionTimeoutChan   chan bool
	electionTimerRestChan chan bool
	heartbeatChan         chan bool

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type LeaderPeer struct {
	nextIndex int
	machIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) GetLogTerm(index int) (term int) {
	if index <= 0 || index > len(rf.logs) {
		return 0
	}

	return rf.logs[index].term
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
	term         int // candidate term number
	candidateID  int // candidate id
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 更新在当前任期内投票给的候选人
	rf.votedFor = args.candidateID

	// 候选人任期小于投票者任期, 拒绝投票
	if args.term < rf.currentTerm {
		reply.term = rf.getCurrentTerm()
		reply.voteGranted = false
		return
	}

	// 投票者没有投过票或者给自己投票
	if rf.votedFor == -1 || args.candidateID == rf.votedFor {
		term := rf.GetLogTerm(rf.lastApplied)
		// 存储日志位置的任期号不同
		if term != args.lastLogTerm {
			// 投票者的任期号大于候选人任期号, 拒绝投票
			if term > args.lastLogTerm {
				reply.term = rf.getCurrentTerm()
				reply.voteGranted = false
			} else {
				reply.term = rf.getCurrentTerm()
				reply.voteGranted = true
			}
		}

		// 存储日志位置的任期号想通
		if term == args.lastLogTerm {
			// 投票者的日志长度大于候选人的日志长度, 拒绝投票
			if rf.lastApplied > args.lastLogIndex {
				reply.term = rf.getCurrentTerm()
				reply.voteGranted = false
			} else {
				reply.term = rf.getCurrentTerm()
				reply.voteGranted = true
			}
		}
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

func (rf *Raft) broadcastElection() <-chan int32{
	// 用于更新选票结果
	votesChan := make(chan int32)

	// 用于统计选票
	// 初始值为1，是为自己投票
	var votes int32
	atomic.StoreInt32(&votes, 1)

	// 向其他服务器发送requestVote RPC
	for i := range rf.peers {
		// 如果是自己不需要在投票了
		if i == rf.me {
			continue
		}

		go func(index int) {
			// follower状态不用统计
			if rf.state.load() == Follower {
				return
			}

			args := RequestVoteArgs{
				term:         rf.currentTerm,
				candidateID:  rf.me,
				lastLogIndex: rf.lastApplied,
				lastLogTerm:  rf.GetLogTerm(rf.lastApplied),
			}

			reply := RequestVoteReply{}

			if rf.sendRequestVote(index, &args, &reply) {
				// 回复者的term大于currentT
				if reply.term > rf.currentTerm {
					// 赋值term
					rf.mu.Lock()
					rf.currentTerm = reply.term
					rf.mu.Unlock()

					// 切换为follower
					rf.state.store(Follower)
					return
				}

				// 累计选票
				atomic.AddInt32(&votes, 1)

				// 发送选票值（是一个不断累加的过程）
				votesChan <- atomic.LoadInt32(&votes)
			}
		}(i)
	}

	return votesChan
}

type AppendEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []Log
	leaderCommit int
}

type AppendEntriesReply struct {
	term    int
	success bool
}

func (rf *Raft) AppendEntries() {

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	// 转变状态为候选人
	rf.state.store(Candidate)

	rf.mu.Lock()
	// 递增当前任期
	rf.currentTerm += 1
	// 投票给的候选人置空
	rf.votedFor = -1
	rf.mu.Unlock()
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
	rf.currentTerm = 0
	rf.votedFor = -1 // nil
	rf.commitIndex = 0
	rf.lastApplied = 0

	go func() {
		rf.timer = newRaftTimer()

		for {
			switch rf.state.load() {
			case Follower: // 跟随者状态
				select {
				case <-rf.timer.C(): // 计时器到期
					// 开始选举
					rf.startElection()
					break
				}

			case Candidate: // 候选人状态
				var (
					votes int32
					votesChan <-chan int32 = make(chan int32)
				)

				// 重置选举计时器
				rf.timer.reset()
				for {
					select {
					case <-rf.timer.C(): //选举超时
						// 重新开始新一轮选举
						rf.timer.reset()
						rf.startElection()
						votes = 0
						votesChan = rf.broadcastElection()
					case <-votesChan:
						votes += 1
					default:
						if int(votes) > len(rf.peers) / 2 {
							// 选举成为leader
							rf.state.store(Leader)
						}
					}
				}

			case Leader:

			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func randElectionTime(min, max int) int {
	rand.Seed(time.Now().UnixNano())

	return rand.Intn(max-min+1) + min
}
