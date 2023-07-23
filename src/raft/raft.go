package raft

// TODO:
// 持久化，幂等性检验

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
	"encoding/json"
	"errors"
	"fmt"

  "bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

  "6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Leader                          = "Leader"
	Follower                        = "Follower"
	Candidate                       = "Candidate"
	HearBeatTime                    = 90 * time.Millisecond
	ElectionTimeout                 = 250
	ShouldUpdateTerm                = "ShouldUpdateTerm"
  Inconsistency                   = "Inconsistency"
	NULL                            = 0
	ElectionTimeoutThresholdPercent = 0.8
)

type Retry struct {
  server int
}

type SendCommit struct {
  server int
  index int
  term int
}

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
  Index int
	Term    int
	Command interface{}
}

// 用于包装一个请求或响应
type ev struct {
	target      interface{} // 函数体
	returnValue interface{} // 返回值
	ch          chan error
}

// A Go object implementing a single Raft peer.
type Raft struct {
	lock      sync.RWMutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	killedCh  chan int32
	NewLogCh  chan *Log
	msgCh     chan ApplyMsg
	//
	ch chan *ev
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
  // --- Persistent state on all servers ---
	currentTerm int
	votedFor    int
	logs        []Log
  // ----------------------------------------
	state       string
	commitIndex int // index of the highest log entry known to be committed
  lastApplied int // index of the highest log entry applied to state machine
	lastLogIndex int 
	lastLogTerm int
	nextIndex  []int
	matchIndex []int
	overtime   time.Duration
	timer      *time.Ticker
	leaderID   int
  cond *sync.Cond
  lk sync.Mutex
  Wg sync.WaitGroup
}

func (rf *Raft) ToCandidate() {
	// TODO
  defer rf.persist()
  rf.lock.Lock()
  rf.SetRaftState(Candidate)
  rf.SetVotedFor(-1)
  rf.lock.Unlock()
}

func (rf *Raft) ToFollower(term int) {
  defer rf.persist()
  rf.lock.Lock()
	DPrintf("S%d State: %s -> Follower Term:%d -> %d\n", rf.me, rf.state, rf.currentTerm, term)
  rf.SetCurrentTerm(term)
  rf.SetRaftState(Follower)
  rf.SetVotedFor(-1)
  rf.lock.Unlock()
	//rf.ResetTime(RandTime())
}

func (rf *Raft) ToLeader() {
  defer rf.persist()
  rf.lock.Lock()
  Dprintf("---[Leader Changed]S%d -> Leader Term:%d lastapplied:%d\n", rf.me, rf.currentTerm, rf.lastLogIndex)
  rf.SetRaftState(Leader)
  rf.SetLeaderID(rf.me)
	for i := 0; i < len(rf.peers); i++ {
    // initialize nextIndex to the index to lastLogIndex + 1
		if i == rf.me {
			continue
		}
    rf.SetNextIndex(i, rf.lastLogIndex + 1)
	}
  rf.lock.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
  rf.lock.RLock()
  w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)
  e.Encode(rf.currentTerm)
  e.Encode(rf.votedFor)
  e.Encode(rf.logs)
  raftState := w.Bytes()
  rf.persister.Save(raftState, nil)
  rf.lock.RUnlock()
}

// lock
func (rf *Raft) Persist() {
  w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)
  e.Encode(rf.currentTerm)
  e.Encode(rf.votedFor)
  e.Encode(rf.logs)
  raftState := w.Bytes()
  rf.persister.Save(raftState, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
  r := bytes.NewBuffer(data)
  d := labgob.NewDecoder(r)
  var currentTerm, votedFor int
  var logs []Log
  if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
    Dprintf("---[ERROR] readPersist Failed!\n")
  } else {
    rf.lock.Lock()
    rf.currentTerm = currentTerm
    rf.votedFor = votedFor
    rf.logs = logs
    rf.lastLogIndex = rf.logs[len(logs) - 1].Index
    rf.lastLogTerm = rf.logs[len(logs) - 1].Term
    rf.lock.Unlock()
  }
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
	MsgState    string
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log // empty for heartbeat
	LeaderCommit int   // leader's commitIndex
}

// 修改请求字段要修改 Handler!
type AppendEntriesReply struct {
	Term    int
	Success bool
  Server int
  LastLogIndex int
  LogLength int
  ConflictTerm int
  ConflictIndex int
	State   string
  SendTerm int
}

func (rf *Raft) Commit(idx int) {
  log := rf.GetLog(idx)
  newMsg := ApplyMsg{
    CommandValid: true,
    Command: log.Command,
    CommandIndex: log.Index,
  }
  DPrintf("S%d Submit {Index: %d, Command %v}", rf.me, idx, log.Command)
  rf.PrintState("AfterSubmit", true)
  rf.lastApplied ++
  rf.msgCh <- newMsg
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {
	reply := &AppendEntriesReply{}
	args.LeaderId = rf.me

  rf.lock.RLock()
  if len(args.Entries) > 0 {
    Dprintf("---[INFO] to S%d index=%d lastIndex=%d\n", server,rf.nextIndex[server], rf.lastLogIndex)
  }
  if rf.nextIndex[server] <= rf.lastLogIndex + 1{
    args.PrevLogIndex = rf.nextIndex[server] - 1
    args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
    for i := rf.nextIndex[server]; i < len(rf.logs); i++ {
      args.Entries = append(args.Entries, rf.logs[i])
    }
  }
  rf.lock.RUnlock()

	args.LeaderCommit = rf.GetCommitIndex()
  ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
  if !ok {
    Dprintf("---[SEND FAIL] AppendEntries: S%d -> S%d FAILED\n", rf.me, server)
    return false
  }
  rf.ch <- &ev{target: reply, ch: make(chan error, 1)}
  return true
}

func (rf *Raft) sendHeartBeat(server int) {
	for rf.GetRaftState() == Leader {
		if rf.killed() {
			return
		}
		timer := time.NewTimer(HearBeatTime)
		go rf.sendAppendEntries(server, rf.InitArgs())
		<-timer.C
	}
}

// lock Required before invoke AppendLog
func (rf *Raft) AppendLog(pLog *Log) {
  defer rf.Persist()
  if rf.lastLogIndex < pLog.Index {
    rf.logs = append(rf.logs, *pLog)
    rf.SetLastInfo(rf.lastLogIndex + 1, pLog.Term)
  } else {
    if rf.logs[pLog.Index].Term != pLog.Term {
      DPrintf("LOG DIFF: last=%d rf.Log=%+v pLog=%+v\n", rf.lastLogIndex, rf.logs[pLog.Index], pLog)
      rf.logs = rf.logs[0: pLog.Index]
      rf.SetLastInfo(len(rf.logs) - 1, rf.logs[len(rf.logs) - 1].Term)
      rf.AppendLog(pLog)
    } 
  }
}


func (rf *Raft) ProcessAppendEntriesReply(reply *AppendEntriesReply) {
  defer rf.persist()
  Dprintf("---[ProcessAppendEntriesReply] S%d Processing reply from S%d\n", rf.me, reply.Server)
  if reply.SendTerm < rf.GetCurrnetTerm() {
    return 
  }
	if reply.State == ShouldUpdateTerm {
    Dprintf("---[TurnFollower] S%d ShouldUpdateTerm\n", rf.me)
		rf.ToFollower(reply.Term)
    return 
	}
  if reply.Success == false && reply.State == Inconsistency {

    rf.lock.Lock()
    server := reply.Server
    if reply.LogLength < rf.nextIndex[server] {
      rf.SetNextIndex(server, reply.LogLength)
    } else if rf.lastLogTerm >= reply.ConflictTerm {
      idx := rf.nextIndex[server] - 1
      for idx > 0 && rf.logs[idx].Term != reply.ConflictTerm {
        idx--
      } 
      if rf.logs[idx].Term != reply.ConflictTerm {
        idx = reply.ConflictIndex
      } else {
        idx++
      }
      rf.SetNextIndex(server, idx)
    }
    rf.lock.Unlock()

    rf.ch <- &ev{target: &Retry{reply.Server}, ch: make(chan error, 1)}
    return 
  }
  // 成功，更新 matchIndex, nextIndex
  if reply.Success {
    rf.lock.Lock()
    rf.SetNextIndex(reply.Server, reply.LastLogIndex + 1)
    rf.SetMatchIndex(reply.Server, reply.LastLogIndex)
    // CheckCommit
    for N := rf.matchIndex[reply.Server]; N > rf.commitIndex && rf.logs[N].Term == rf.currentTerm; N-- {
      cnt := 1
      for i := 0; i < len(rf.peers); i++ {
        if i != rf.me && rf.matchIndex[i] >= N {
          cnt++
        }
      }
      if cnt * 2 > len(rf.peers) {
        rf.lk.Lock()
        rf.SetCommitIndex(N)
        rf.cond.Broadcast()
        rf.lk.Unlock()
        break
      }
    }
    rf.lock.Unlock()
  }
}

// RPC Handler
func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	e := &ev{target: args, returnValue: reply, ch: make(chan error, 1)}
	// Process Request 之后才会返回 reply
	rf.ch <- e
  timer := RandTimer()
  select {
  case <-e.ch:
    temp, _ := json.Marshal(e.returnValue.(*AppendEntriesReply))
    json.Unmarshal(temp, reply)
  case <-timer:
    return 
  }
}

func (rf *Raft) ProcessAppendEntriesRequest(args *AppendEntriesArgs) (reply *AppendEntriesReply, update bool) {
	defer func() {
    rf.persist()
    DPrintf("AppendEntries Recevied S%d -> S%d\nargs:%+v\nreply:%+v\n", args.LeaderId, rf.me, args, reply)
	}()

  reply = &AppendEntriesReply{
    Server: rf.me,
    Success: true,
    Term: rf.GetCurrnetTerm(),
    SendTerm: args.Term,
  }
  currTerm := rf.GetCurrnetTerm()
	if args.Term < currTerm {
		reply.Success = false
		reply.State = ShouldUpdateTerm
		return reply, false
	} else {
		if rf.GetRaftState() != Follower {
      Dprintf("---[TurnFollower] S%d New Leader Elected\n", rf.me)
			rf.ToFollower(args.Term)
		} else if args.Term != currTerm {
			rf.LSetCurrentTerm(args.Term)
		}
    rf.LSetLeaderID(args.LeaderId)
  } 
  
  rf.PrintState("Recevie Entreis",true)
  DPrintf("senderlastIndx=%d prevLogindex= %d\n", rf.GetLastLogIndex(), args.PrevLogIndex)

  if rf.GetLastLogIndex() < args.PrevLogIndex {
    reply.Success = false
    reply.State = Inconsistency
    reply.LogLength = len(rf.logs)
    return reply, false
  }
  if rf.GetLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
    reply.Success = false
    reply.State = Inconsistency

    rf.lock.Lock()
    reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
    // 往前找ConflictTerm的第一个index
    for i := 0; i <= args.PrevLogIndex; i++ {
      if rf.logs[i].Term == reply.ConflictTerm {
        reply.ConflictIndex = i
        break
      }
    }
    reply.LastLogIndex = rf.lastLogIndex
    reply.LogLength = len(rf.logs)
    rf.lock.Unlock()

    return reply, false
  }

  rf.lock.Lock()
  for _, log_ := range args.Entries {
    rf.AppendLog(&log_) 
  }
  reply.LastLogIndex = rf.logs[len(rf.logs)-1].Index
  reply.LogLength = len(rf.logs)
  rf.PrintState(fmt.Sprintf("Recevied S%d AppendEntries", args.LeaderId), false)

  rf.lk.Lock()
  if args.LeaderCommit > rf.commitIndex {
    rf.SetCommitIndex(min(args.LeaderCommit, rf.lastLogIndex))
    rf.cond.Broadcast()
  }
  rf.lk.Unlock()

  rf.lock.Unlock()

	return reply, true
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

// 1. Reply false if Term < currentTerm
// 2. If votedFor is null or candidateId, and candidate’s log is at
// 	  least as up-to-date as receiver’s log, grant vote

// example RequestVote RPC handler.
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	e := &ev{target: args, returnValue: reply, ch: make(chan error, 1)}
	rf.ch <- e
  timer := RandTimer()
	select {
	case <-e.ch:
		temp, _ := json.Marshal(e.returnValue.(*RequestVoteReply))
    json.Unmarshal(temp, reply)
  case <-timer:
    return
	}
}

func (rf *Raft) ProcessRequestVoteRequest(args *RequestVoteArgs) (*RequestVoteReply, bool) {
  defer rf.persist()
  currTerm := rf.GetCurrnetTerm()
	reply := &RequestVoteReply{Term: currTerm, VoteGranted: true}

	if args.Term < currTerm {
		reply.VoteGranted = false
		reply.MsgState = ShouldUpdateTerm
		return reply, false
	}
	if args.Term > currTerm {
		if rf.GetRaftState() != Follower {
      Dprintf("---[TurnFollower] S%d meet higher Term\n", rf.me)
			rf.ToFollower(args.Term)
		} else {
			rf.LSetCurrentTerm(args.Term)
		}
	} else if rf.GetVotedFor() != -1 {
		DPrintf("RequstVote INJECTED args:%+v", args)
		reply.VoteGranted = false
		return reply, false
	}
  lastTerm := rf.GetLastLogTerm()
  lastIndex := rf.GetLastLogIndex()
  if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
    reply.VoteGranted = false
    return reply, false
  }
  
	reply.Term = rf.GetCurrnetTerm()
  rf.LSetVotedFor(args.CandidateId)
	return reply, true
}

// 1. 接受了投票，则 reply.Term == rf.term
// 2. 没接受投票，要么已经投了，要么 reply.Term > rf.term, convert to Follower
func (rf *Raft) ProcessRequestVoteReply(reply *RequestVoteReply) bool {
  defer rf.persist()
	if reply.VoteGranted && reply.Term == rf.GetCurrnetTerm() {
		DPrintf("S%d Got 1 Vote\n", rf.me)
		return true
	}
	if reply.Term > rf.GetCurrnetTerm() {
    rf.ToFollower(reply.Term)
	}
	return false
}



func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, respChan chan *RequestVoteReply) bool {
	reply := &RequestVoteReply{}
  ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
  if !ok {
    Dprintf("---[SEND WARN] RequstVote S%d -> S%d FAILED!\n", rf.me, server)
  }
	if rf.GetRaftState() == Candidate {
		respChan <- reply
	}

	return true
}


func (rf *Raft) FollowerLoop() {
	rf.ResetTime(RandTime())
	for rf.GetRaftState() == Follower {
		update := false
		var err error = nil
		select {
		case e := <-rf.ch:
			switch req := e.target.(type) {
			case *RequestVoteArgs:
				e.returnValue, update = rf.ProcessRequestVoteRequest(req)
			case *AppendEntriesArgs:
				e.returnValue, update = rf.ProcessAppendEntriesRequest(req)
			default:
				err = nil
			}
			e.ch <- err
		case <-rf.timer.C:
			rf.ToCandidate()
		case <-rf.killedCh:
			break
		}
		if update {
			rf.ResetTime(RandTime())
		}
	}
}

func (rf *Raft) CandidateLoop() {
	doVote := true
	respChan := make(chan *RequestVoteReply, len(rf.peers))
	voteCount := 0
	for rf.GetRaftState() == Candidate {
		if rf.killed() == true {
			return
		}
    // 第一次选举或选举超时
		if doVote {
      rf.LUpdateCurrentTerm(1)
      rf.LSetVotedFor(rf.me)
			rf.PrintState("Election", true)
			term := rf.GetCurrnetTerm()
      lastLogIndex, lastLogTerm := rf.GetLastInfo()
      for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(server int) {
					args := &RequestVoteArgs{
						Term:         term,
						CandidateId:  rf.me,
            LastLogIndex: lastLogIndex,
            LastLogTerm: lastLogTerm,
					}
					rf.sendRequestVote(server, args, respChan)
				}(i)
			}
			voteCount = 1
			doVote = false
			rf.ResetTime(RandTime())
		}
		if voteCount*2 > len(rf.peers) {
			rf.ToLeader()
			return
		}
		if rf.killed() {
			return
		}
		select {
		case resp := <-respChan:
			// TODO: 只考虑了成功投票
			if rf.ProcessRequestVoteReply(resp) {
				voteCount++
				DPrintf("S%d VoteCnt=%d\n", rf.me, voteCount)
			}
		case e := <-rf.ch:
			var err error = nil
			switch req := e.target.(type) {
			case *RequestVoteArgs:
				e.returnValue, _ = rf.ProcessRequestVoteRequest(req)
			case *AppendEntriesArgs:
				e.returnValue, _ = rf.ProcessAppendEntriesRequest(req)
			default:
				err = errors.New("unknown request")
			}
			e.ch <- err
		case <-rf.timer.C:
			doVote = true
		case <-rf.killedCh:
			break
		}
	}
}

func (rf *Raft) LeaderLoop() {
	if rf.killed() {
		return
	}
	// HeartBeat
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.sendHeartBeat(server)
		}(i)
	}
	for rf.GetRaftState() == Leader {
		var err error = nil
		select {
		case e := <-rf.ch:
			switch req := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, _ = rf.ProcessAppendEntriesRequest(req)
			case *RequestVoteArgs:
				e.returnValue, _ = rf.ProcessRequestVoteRequest(req)
			case *AppendEntriesReply: // 响应 AppendEntriesReply
				rf.ProcessAppendEntriesReply(req)
			}
			e.ch <- err
		case <-rf.killedCh:
			break
		}
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
  defer rf.persist()
  rf.lock.RLock()
  index := rf.lastLogIndex
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if !isLeader {
		isLeader = false
    rf.lock.RUnlock()
		return index, term, isLeader
	}
  rf.lock.RUnlock()

  rf.lock.Lock()
	newLog := &Log{rf.lastLogIndex + 1, term, command}
  dbg("S%d recevied NEWLOG: %v", rf.me, newLog)
	rf.AppendLog(newLog)
  index = rf.lastLogIndex
  rf.lock.Unlock()

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.killedCh <- rf.dead
	DPrintf("server#%d killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) CommitLoop() {
  rf.lk.Lock()
  for rf.killed() == false {
    for idx := rf.lastApplied + 1; idx <= rf.GetCommitIndex(); idx ++ {
      rf.Commit(idx)
    }
    //time.Sleep(10 * time.Millisecond)
    rf.cond.Wait()
  }
}

func (rf *Raft) Loop() {
  go rf.CommitLoop()
	for rf.killed() == false {
		if rf.killed() {
      rf.Wg.Wait()
			return
		}
		switch rf.GetRaftState() {
		case Follower:
			rf.FollowerLoop()
		case Candidate:
			rf.CandidateLoop()
		case Leader:
			rf.LeaderLoop()
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
		ch:          make(chan *ev, 1024),
		NewLogCh:    make(chan *Log, 1024),
		state:       Follower,
		killedCh:    make(chan int32, 1),
		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]Log, 0),
		commitIndex: 0,
		lastLogIndex: 0,
    lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		overtime:    RandTime(),
		leaderID:    -1,
	}
  rf.cond = sync.NewCond(&rf.lk)
	rf.logs = append(rf.logs, Log{0, 0, void})
  rf.lastLogIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
  rf.PrintState("Reboot", true)
  rf.ResetTime(RandTime())
	rf.msgCh = applyCh

	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	go rf.Loop()
	return rf
}

func (rf *Raft) InitArgs() *AppendEntriesArgs {
  rf.lock.RLock()
  args := &AppendEntriesArgs{
    Term:         rf.currentTerm,
    LeaderId:     rf.me,
    PrevLogIndex: NULL,
    PrevLogTerm:  NULL,
    Entries:      nil,
    LeaderCommit: rf.commitIndex,
  }
  rf.lock.RUnlock()
  return args
}

func RandTime() time.Duration {
	ms := ElectionTimeout + (rand.Int63() % ElectionTimeout)
	return time.Duration(ms) * time.Millisecond
}

func RandTimer() <-chan time.Time {
	timer := time.NewTimer(RandTime())
	return timer.C
}



func (rf *Raft) PrintState(pos string, ok bool) {
	if !Debug {
		ok = false
	}
	if ok {
    //fmt.Printf("[%s] S%d %s Term=%d lastLogIndex=%d lastApplied=%d commitIndex=%d\n----------Entries=%d\n",
    //pos, rf.me, rf.state, rf.currentTerm, rf.lastLogIndex, rf.lastApplied, rf.commitIndex, rf.logs[0:len(rf.logs)])
    fmt.Printf("[%s] S%d %s Term=%d lastLogIndex=%d lastApplied=%d commitIndex=%d len=%d\n\n",
    pos, rf.me, rf.state, rf.currentTerm, rf.lastLogIndex, rf.lastApplied, rf.commitIndex, len(rf.logs) - 1)
	}
}

// 可能有 data race， 但概率很小 
func (rf *Raft) ResetTime(duration time.Duration) {
	rf.overtime = duration
	rf.timer = time.NewTicker(rf.overtime)
}

func (rf *Raft) GetLog(idx int) Log {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.logs[idx]
}

func (rf *Raft) GetRaftState() string {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.state
}

func (rf *Raft) GetCurrnetTerm() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.currentTerm
}

func (rf *Raft) GetLastLogTerm() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.lastLogTerm
}

func (rf *Raft) GetLastLogIndex() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.lastLogIndex
}

func (rf *Raft) GetLastInfo() (int, int) {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.lastLogIndex, rf.lastLogTerm
}

func (rf *Raft) GetLogTerm(index int) int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.logs[index].Term
}
  

// return (nextIndex[server], matchIndex[server])
func (rf *Raft) GetIndexInfo(server int) (int, int) {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.nextIndex[server], rf.matchIndex[server]
}

func (rf *Raft) GetVotedFor() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.votedFor
}

func (rf *Raft) GetCommitIndex() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.commitIndex
}

func (rf *Raft) GetLeaderID() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.leaderID
}

func (rf *Raft) SetRaftState(new_state string) {
  rf.state = new_state
}

func (rf *Raft) LSetRaftState(new_state string) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetRaftState(new_state)
}

// Set rf.votedFor too
func (rf *Raft) SetCurrentTerm(term int) {
  if term < rf.currentTerm {
    return 
  }
  rf.currentTerm = term
  rf.votedFor = -1
}

func (rf *Raft) LSetCurrentTerm(term int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetCurrentTerm(term)
}

func (rf *Raft) UpdateCurrentTerm(val int) {
  rf.currentTerm += val
  rf.votedFor = -1
}

func (rf *Raft) LUpdateCurrentTerm(val int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.UpdateCurrentTerm(val)
}

func (rf *Raft) SetLastInfo(lastLogIndex, lastLogTerm int) {
  rf.lastLogIndex = lastLogIndex
  rf.lastLogTerm = lastLogTerm
}

func (rf *Raft) LSetLastInfo(lastLogIndex, lastLogTerm int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetLastInfo(lastLogIndex, lastLogTerm)
}

func (rf *Raft) SetMatchIndex(server, newMatchIndex int) {
  if newMatchIndex < rf.matchIndex[server] {
    fmt.Printf("wrong matchIndex: %d -> %d\n", rf.matchIndex[server], newMatchIndex)
    return
  }
  rf.matchIndex[server] = newMatchIndex
}

func (rf *Raft) LSetMatchIndex(server, newMatchIndex int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetMatchIndex(server, newMatchIndex)
}

func (rf *Raft) SetNextIndex(server, newNextIndex int) {
  //dbg("[INFO] Set S%d.nextIndex[S%d]=%d\n", rf.me, server, valIndex)
  if newNextIndex > rf.nextIndex[server] {
    return 
  }
  rf.nextIndex[server] = newNextIndex
}
func (rf *Raft) LSetNextIndex(server, newNextIndex int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetNextIndex(server, newNextIndex)
}
func (rf *Raft) UpdateIndexInfo(server, valIndex, valMatch int) {
  rf.nextIndex[server] += valIndex
  rf.matchIndex[server] += valMatch
}

func (rf *Raft) LUpdateIndexInfo(server, valIndex, valMatch int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.UpdateIndexInfo(server, valIndex, valMatch)
}

func (rf *Raft) SetVotedFor(newVotedFor int) {
  rf.votedFor = newVotedFor
}

func (rf *Raft) LSetVotedFor(newVotedFor int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.ResetTime(RandTime())
  rf.SetVotedFor(newVotedFor)
}

func (rf *Raft) SetCommitIndex(newCommitIndex int) {
  rf.commitIndex = newCommitIndex
}

func (rf *Raft) LSetCommitIndex(newCommitIndex int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetCommitIndex(newCommitIndex)
}

func (rf *Raft) SetLeaderID(newID int) {
  rf.leaderID = newID
}

func (rf *Raft) LSetLeaderID(newID int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetLeaderID(newID)
}


