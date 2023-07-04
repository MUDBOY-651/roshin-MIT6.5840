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
	"fmt"
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob
	"6.5840/labrpc"
)

const (
	Leader       = "Leader"
	Follower     = "Follower"
	Candidate    = "Candidate"
	HearBeatTime = 100 * time.Millisecond
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       string
	currentTerm int
	votedFor    int
	logs        []Log
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machine
	lastLogTerm int
	// leader fields
	nextIndex  []int
	matchIndex []int
	overtime   time.Duration
	timer      *time.Ticker
	leaderID   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool // true->received vote
}

// 1. Reply false if Term < currentTerm
// 2. If votedFor is null or candidateId, and candidate’s log is at
// 	  least as up-to-date as receiver’s log, grant vote

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.currentTerm || (args.LastLogTerm == rf.currentTerm && args.LastLogIndex >= rf.lastApplied)) {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	// AllServers 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log // empty for heartbeat
	LeaderCommit int   // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendLog(pLog *Log) bool {
	rf.logs = append(rf.logs, *pLog)
	rf.lastApplied++
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// timer
	DPrintf("[AppendEntries] req from S%d to S%d\n", args.LeaderId, rf.me)
	rf.timer.Reset(rf.overtime)
	reply.Success = true
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	if rf.lastApplied < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		if rf.lastApplied >= args.PrevLogIndex {
			rf.logs = rf.logs[1:args.PrevLogIndex]
			rf.lastApplied = args.PrevLogIndex - 1
			for _, log_ := range args.Entries {
				if !rf.AppendLog(&log_) {
					log.Printf("[AppendEntries] Append LOG: %+v Failed", log_)
				}
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastApplied)
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}
	for ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok; {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, HeartBeat bool) bool {
	if rf.killed() {
		return false
	}
	args.LeaderId = rf.me
	if !HeartBeat {
		args.PrevLogIndex = rf.nextIndex[server]
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		if args.PrevLogIndex >= rf.nextIndex[server] {
			args.Entries = rf.logs[rf.nextIndex[server]:]
		}
	}
	for ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok; {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	return true
}

func (rf *Raft) StartElection() {
	if rf.killed() {
		return
	}
	rf.currentTerm++
	cnt := 1
	voted := 1
	rf.votedFor = rf.me
	term := rf.currentTerm
	// 新一轮选举，重置计时器
	rf.overtime = RandTime()
	rf.timer.Reset(rf.overtime)
	var wg sync.WaitGroup
	var interLock sync.Mutex
	cond := sync.NewCond(&interLock)
	for serverID, _ := range rf.peers {
		if serverID == rf.me {
			continue
		}
		go func(server int) {
			wg.Add(1)
			defer wg.Done()
			args := &RequestVoteArgs{
				term,
				rf.me,
				rf.lastApplied,
				0,
			}
			if len(rf.logs) > 0 {
				args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if !ok {
				return
			}
			interLock.Lock()
			if reply.VoteGranted {
				DPrintf("Vote From S%d to S%d\n", server, rf.me)
				cnt += 1
			}
			voted++
			cond.Broadcast()
			interLock.Unlock()
		}(serverID)
	}
	interLock.Lock()
	for cnt*2 <= len(rf.peers) || voted < len(rf.peers) {
		cond.Wait()
	}
	DPrintf("S%d received %d Votes!", rf.me, cnt)
	if cnt*2 > len(rf.peers) {
		rf.state = Leader
		rf.leaderID = rf.me
		DPrintf("S%d is Leader", rf.me)
		rf.PrintState("Election", true)
	} else {
		rf.state = Follower
	}
	wg.Wait()
}

func (rf *Raft) LeaderEvent() {
	if rf.killed() {
		return
	}
	rf.overtime = HearBeatTime
	rf.timer.Reset(rf.overtime)
	var wg sync.WaitGroup
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			wg.Add(1)
			args := &AppendEntriesArgs{}
			reply := &AppendEntriesReply{}
			args.Term = term
			args.LeaderCommit = commitIndex
			rf.sendAppendEntries(server, args, reply, true)
			wg.Done()
		}(i)
	}
	wg.Wait()
	// TODO: leader身份变化就停止
	if rf.state != Leader {
		return
	}
	appendNums := 1
	var interLock sync.Mutex
	cond := sync.NewCond(&interLock)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &AppendEntriesArgs{}
			reply := &AppendEntriesReply{}
			args.Term = term
			args.LeaderCommit = commitIndex
			for reply.Success == false {
				rf.sendAppendEntries(server, args, reply, false)
				interLock.Lock()
				if reply.Success == false {
					rf.nextIndex[server]--
				} else {
					rf.nextIndex[server] += len(args.Entries)
					rf.matchIndex[server] += len(args.Entries)
					cond.Broadcast()
				}
				interLock.Unlock()
			}
		}(i)
	}
	interLock.Lock()
	for appendNums < len(rf.peers) {
		cond.Wait()
		for idx := rf.lastApplied; idx > rf.commitIndex; idx-- {
			cnt := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= idx && rf.logs[idx].Term == rf.currentTerm {
					cnt++
				}
			}
			if cnt*2 > len(rf.peers) {
				rf.commitIndex = idx
				break
			}
		}
		// TODO: 感觉有个限制能退出循环，不能一直在检测
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	rf.lastApplied++
	index = rf.lastApplied
	term = rf.currentTerm
	rf.lastLogTerm = term
	rf.logs = append(rf.logs, Log{term, command})
	return index, term, isLeader
}

func (rf *Raft) PrintState(pos string, ok bool) {
	if ok {
		fmt.Printf("[%s] Server#%d %s Term=%d comIndex=%d lastApp=%d lastTerm=%d Logs=%+v\n",
			pos, rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lastLogTerm, rf.logs)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("server#%d killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// TODO: 实现 election

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.timer.C:
			rf.PrintState("Ticker", true)
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.state {
			case Follower:
				rf.state = Candidate
				fallthrough
			case Candidate:
				rf.StartElection()
				fallthrough
			case Leader:
				rf.LeaderEvent()
			}
			rf.mu.Unlock()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]Log, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		overtime:    RandTime(),
		leaderID:    -1,
	}
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.logs = append(rf.logs, Log{0, void})
	rf.timer = time.NewTicker(rf.overtime)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.PrintState("Make", true)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func RandTime() time.Duration {
	ms := 100 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}
