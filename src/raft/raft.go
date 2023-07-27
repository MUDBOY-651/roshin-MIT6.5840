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
  "sort"

  "6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Leader                          = "Leader"
	Follower                        = "Follower"
	Candidate                       = "Candidate"
	HearBeatTime                    = 100 * time.Millisecond
	ElectionTimeout                 = 300
	ShouldUpdateTerm                = "ShouldUpdateTerm"
  Inconsistency                   = "Inconsistency"
	NULL                            = 0
	ElectionTimeoutThresholdPercent = 0.8
  MaxLog = 10 
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
  snapshot []byte
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
	nextIndex  []int
	matchIndex []int
	timer      *time.Ticker
	leaderID   int
  cond *sync.Cond
  lk sync.Mutex
  Wg sync.WaitGroup
  offset int
  offsetTerm int
  lastLogIndex int 
  lastLogTerm int
}

func (rf *Raft) ToCandidate() {
	// TODO
  defer rf.persistLocked()
  rf.lock.Lock()
  rf.SetRaftState(Candidate)
  rf.SetVotedFor(-1)
  rf.lock.Unlock()
}

func (rf *Raft) ToFollower(term int) {
  defer rf.persistLocked()
  rf.lock.Lock()
	DPrintf("S%d State: %s -> Follower Term:%d -> %d\n", rf.me, rf.state, rf.currentTerm, term)
  rf.leaderID = -1
  rf.SetCurrentTerm(term)
  rf.SetRaftState(Follower)
  rf.SetVotedFor(-1)
  rf.lock.Unlock()
  rf.ResetTime(RandTime())
}

func (rf *Raft) ToLeader() {
  defer func() {
    rf.persist()
    rf.lock.Unlock()
  }()
  rf.lock.Lock()
  Dprintf("---[Leader Changed]S%d -> Leader Term:%d\n", rf.me, rf.currentTerm)
  rf.SetRaftState(Leader)
  rf.SetLeaderID(rf.me)
	for i := 0; i < len(rf.peers); i++ {
    // initialize nextIndex to the index to lastLogIndex + 1
		if i == rf.me {
			continue
		}
    rf.SetNextIndex(i, rf.lastLogIndex + 1)  // already +1
	}
}

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
func (rf *Raft) persistLocked() {
  rf.lock.RLock()
  rf.persist()
  rf.lock.RUnlock()
}
// lock
func (rf *Raft) persist() {
  w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)
  e.Encode(rf.currentTerm)
  e.Encode(rf.votedFor)
  e.Encode(rf.logs)
  e.Encode(rf.offset)
  e.Encode(rf.offsetTerm)
  raftState := w.Bytes()
  rf.persister.Save(raftState, rf.snapshot)
}
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
  r := bytes.NewBuffer(data)
  d := labgob.NewDecoder(r)
  var currentTerm, votedFor, offset, offsetTerm int
  var logs []Log
  var snapshot []byte
  if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&snapshot) != nil || d.Decode(&offset) != nil || d.Decode(&offsetTerm) != nil {
    Dprintf("---[ERROR] readPersist Failed!\n")
  } else {
    rf.lock.Lock()
    rf.currentTerm = currentTerm
    rf.votedFor = votedFor
    rf.logs = logs
    rf.offset = offset
    rf.offsetTerm = offsetTerm
    rf.lastApplied = offset
    rf.commitIndex = offset
    rf.lock.Unlock()
  }
}

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

type InstallSnapshotArgs struct {
  Term int
  LeaderId int
  LastIncludedIndex int
  LastIncludedTerm int
  Data []byte
}

type InstallSnapshotReply struct {
  Term int
}

func (rf *Raft) sliceIndex(index int) int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return index - rf.offset
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
  rf.PrintState(fmt.Sprintf("Snapshot index=%d", index), true)
	// Your code here (2D).
  rf.lock.Lock()
  if index <= rf.offset {
    rf.lock.Unlock()
    return
  }
  temp := make([]Log, rf.lastLogIndex - index + 1)
  copy(temp[0:1], rf.logs[0:1])
  copy(temp[1:], rf.logs[index - rf.offset + 1:])
  rf.SetOffsetTerm(rf.GetLog(index).Term)
  rf.SetLogs(temp)
  rf.SetSnapshot(snapshot)
  rf.snapshot = snapshot
  rf.SetOffset(index)
  currTerm := rf.GetCurrentTerm()
  offsetTerm := rf.GetOffsetTerm()
  rf.lock.Unlock()

  if rf.state == Leader {
    for i := 0; i < len(rf.peers); i++ {
      if i == rf.me {
        continue
      }
      go func(server int) {
        // send InstallSnapshotRPC, if the logs send to follower discarded
        if rf.GetNextIndexLocked(server) <= index {
          args := &InstallSnapshotArgs{
            Term: currTerm,
            LeaderId: rf.me,
            LastIncludedIndex: index,
            LastIncludedTerm: offsetTerm,
            Data: snapshot,
          }
          rf.sendInstallSnapshot(server, args)
        }
      }(i)
    }
  }
  msg := ApplyMsg{
    SnapshotValid: true,
    Snapshot: snapshot,
    SnapshotTerm: offsetTerm,
    SnapshotIndex: index,
  }
  Dprintf("---[Snapshot] S%d apply index=%d %+v\n", rf.me, index, msg)
  rf.persist()
  rf.msgCh<-msg
}

func (rf *Raft) ProcessInstallSnapshotRequest(args *InstallSnapshotArgs) (*InstallSnapshotReply, bool) {
  reply := &InstallSnapshotReply{
    Term: rf.GetCurrnetTermLocked(),
  }
  // request expired
  if args.Term < reply.Term {
    return reply, false
  }
  if rf.GetRaftStateLocked() != Follower {
    rf.ToFollower(args.Term)
  }
  rf.SetLeaderID(args.LeaderId)
  // snapshot expired
  if args.LastIncludedIndex <= rf.GetOffsetLocked() {
    return reply, true
  }
  // a RPC includes the entire log entries, there's no chunks
  // there's a log entrie not exist in the server's log entries
  // discard all the log entries the server has
  if rf.GetLastLogIndexLocked() <= args.LastIncludedIndex || rf.GetLogTermLocked(args.LastIncludedIndex) != args.LastIncludedTerm {
    rf.lock.Lock()
    rf.logs = rf.logs[0:1]
    rf.offset = args.LastIncludedIndex
    defer rf.lock.Unlock()
  } else {
    // otherwise, discrad the prefix of the log entries
    rf.lock.Lock()
    temp := rf.logs[args.LastIncludedIndex - rf.offset + 1:]
    rf.logs = rf.logs[0:1]
    rf.logs = append(rf.logs, temp...)
    rf.offset = args.LastIncludedIndex
    defer rf.lock.Unlock()
  }
  rf.snapshot = args.Data
  msg := ApplyMsg{
    SnapshotValid: true,
    Snapshot: rf.snapshot,
    SnapshotTerm: rf.offsetTerm,
    SnapshotIndex: rf.offset,
  }
  rf.persist()
  rf.msgCh<-msg
  return reply, true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) bool {
  reply := &InstallSnapshotReply{}
  Dprintf("---[sendSnapShot] S%d -> S%d index=%d term=%d", rf.me,server,args.LastIncludedIndex,args.LastIncludedTerm)
  ok := rf.peers[server].Call("Raft.InstallSnapshotHandler", args, reply)
  if !ok {
    DPrintf("[WARN] send ERROR")
    return false
  }
  if reply.Term > rf.GetCurrnetTermLocked() {
    rf.ToFollower(reply.Term)
  }
  return true
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
  e := &ev{target: args, ch: make(chan error, 1)}
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
  LogLength int
  ConflictTerm int
  ConflictIndex int
	State   string
}

func (rf *Raft) sendHeartBeat(server int) {
	for rf.GetRaftStateLocked() == Leader {
		if rf.killed() {
			return
		}
		timer := time.NewTimer(HearBeatTime)
		go rf.sendAppendEntries(server, rf.InitArgs())
		<-timer.C
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {
	reply := &AppendEntriesReply{}
	args.LeaderId = rf.me

  Dprintf("---[INFO] AE: S%d to S%d index=%d lastIndex=%d\n",rf.me, server,rf.nextIndex[server], rf.lastLogIndex)
  if rf.GetNextIndexLocked(server) - 1 <= rf.GetLastLogIndexLocked() {
    rf.lock.RLock()
    args.PrevLogIndex = rf.nextIndex[server] - 1
    args.PrevLogTerm = rf.logs[args.PrevLogIndex - rf.offset].Term
    if args.PrevLogIndex == rf.offset {
      args.PrevLogTerm = rf.offsetTerm
    }
    args.Entries = make([]Log, rf.GetLastLogIndex() - rf.GetNextIndex(server) + 1)
    copy(args.Entries, rf.logs[rf.GetNextIndex(server) - rf.GetOffset() : ])
    rf.lock.RUnlock()
  }

	args.LeaderCommit = rf.GetCommitIndexLocked()
  ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
  if !ok {
    Dprintf("---[SEND FAIL] AppendEntries: S%d -> S%d FAILED\n", rf.me, server)
    return false
  }
  rf.ProcessAppendEntriesReply(args, reply)
  return true
}

func (rf *Raft) ProcessAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  defer rf.persistLocked()
  Dprintf("---[ProcessAppendEntriesReply] S%d Processing reply from S%d\nreply=%+v\n", rf.me, reply.Server, reply)
  if args.Term != rf.GetCurrnetTermLocked() {
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
    if reply.LogLength + 1 < rf.nextIndex[server] {  // follower 日志长度小
      rf.SetNextIndex(server, reply.LogLength)
    } else {  // 日志冲突
      idx := args.PrevLogIndex
      for idx > 0 && rf.GetLogTerm(idx) != reply.ConflictTerm {
        idx--
      } 
      // 不存在对应term的日志 
      if rf.GetLogTerm(idx) != reply.ConflictTerm {
        idx = reply.ConflictIndex
      } else {
        idx++
      }
      rf.SetNextIndex(server, idx)
    }
    rf.lock.Unlock()

    return 
  }
  // 成功，更新 matchIndex, nextIndex
  if reply.Success {
    rf.lock.Lock()
    rf.SetNextIndex(reply.Server, args.PrevLogIndex + len(args.Entries) + 1)
    rf.SetMatchIndex(reply.Server, args.PrevLogIndex + len(args.Entries))
    // CheckCommit
    indexs := make([]int, 0)
    indexs = append(indexs, rf.lastLogIndex)
    for i := 0; i < len(rf.peers); i++ {
      if i == rf.me {
        continue
      }
      indexs = append(indexs, rf.GetMatchIndex(i))
    }
    sort.Ints(indexs)
    newCommitIndex := indexs[len(rf.peers) / 2]
    if newCommitIndex > rf.GetCommitIndex() && rf.GetLogTerm(newCommitIndex) == rf.GetCurrentTerm() {
      rf.lk.Lock()
      rf.SetCommitIndex(newCommitIndex)
      rf.cond.Broadcast()
      rf.lk.Unlock()
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
    rf.persistLocked()
    DPrintf("AppendEntries Recevied S%d -> S%d\nargs:%+v\nreply:%+v\n", args.LeaderId, rf.me, args, reply)
	}()

  reply = &AppendEntriesReply{
    Server: rf.me,
    Success: false,
    Term: rf.GetCurrnetTermLocked(),
  }
  currTerm := rf.GetCurrnetTermLocked()
	if args.Term < currTerm {
		reply.State = ShouldUpdateTerm
		return reply, false
	} else {
		if rf.GetRaftStateLocked() != Follower {
      Dprintf("---[TurnFollower] S%d New Leader Elected\n", rf.me)
      rf.ToFollower(args.Term)
    } else if args.Term > currTerm {
      rf.ToFollower(args.Term)
    }
  } 
  rf.SetLeaderIDLocked(args.LeaderId)
  rf.PrintState("Recevie Entreis",true)
  DPrintf("lastIndex=%d prevLogindex= %d\n", rf.GetLastLogIndex(), args.PrevLogIndex)

  if rf.GetLastLogIndexLocked() < args.PrevLogIndex {
    reply.State = Inconsistency
    reply.LogLength = rf.GetLastLogIndexLocked()
    return reply, true
  }
  if rf.GetLogTermLocked(args.PrevLogIndex) != args.PrevLogTerm {
    reply.State = Inconsistency

    rf.lock.Lock()
    reply.ConflictTerm = rf.GetLogTerm(args.PrevLogIndex)
    // 往前找ConflictTerm的第一个index
    for i := rf.GetOffset() + 1; i <= args.PrevLogIndex; i++ {
      if rf.GetLogTerm(i) == reply.ConflictTerm {
        reply.ConflictIndex = i
        break
      }
    }
    reply.LogLength = rf.GetLastLogIndex()
    rf.lock.Unlock()

    return reply, true
  }

  rf.lock.Lock()
  for _, log_ := range args.Entries {
    rf.AppendLog(&log_)
  }
  reply.LogLength = rf.GetLastLogIndex()
  rf.PrintState(fmt.Sprintf("Recevied S%d AppendEntries", args.LeaderId), false)
  rf.lk.Lock()
  if args.LeaderCommit > rf.GetCommitIndex() {
    rf.SetCommitIndex(min(args.LeaderCommit, rf.GetLastLogIndex()))
    rf.cond.Broadcast()
  }
  reply.Success = true
  rf.lk.Unlock()
  rf.lock.Unlock()
	return reply, true
}
// lock Required before invoke AppendLog
func (rf *Raft) AppendLog(pLog *Log) {
  defer rf.persist()
  if rf.lastLogIndex < pLog.Index {
    rf.logs = append(rf.logs, *pLog)
    rf.SetLastLogIndex(pLog.Index)
    rf.SetLastLogTerm(pLog.Term)
  } else {
    if rf.GetLogTerm(pLog.Index) != pLog.Term {
      rf.logs = rf.logs[0: pLog.Index - rf.offset]
      rf.AppendLog(pLog)
    } 
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
  defer rf.persistLocked()
  currTerm := rf.GetCurrnetTermLocked()
	reply := &RequestVoteReply{Term: currTerm, VoteGranted: true}

	if args.Term < currTerm {
		reply.VoteGranted = false
		reply.MsgState = ShouldUpdateTerm
		return reply, false
	}
	if args.Term > currTerm {
		if rf.GetRaftStateLocked() != Follower {
      Dprintf("---[TurnFollower] S%d meet higher Term\n", rf.me)
			rf.ToFollower(args.Term)
		} else {
      rf.SetVotedForLocked(-1)
			rf.SetCurrentTermLocked(args.Term)
		}
	} 
  votedFor := rf.GetVotedForLocked()
  if votedFor != -1 && votedFor != args.CandidateId {
		DPrintf("RequstVote INJECTED args:%+v", args)
		reply.VoteGranted = false
		return reply, false
	}
  rf.lock.Lock()
  lastTerm := rf.GetLastLogTerm()
  lastIndex := rf.GetLastLogIndex()
	reply.Term = rf.GetCurrentTerm()
  rf.lock.Unlock()
  if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
    reply.VoteGranted = false
    return reply, false
  }
  
  rf.SetVotedForLocked(args.CandidateId)
	return reply, true
}

// 1. 接受了投票，则 reply.Term == rf.term
// 2. 没接受投票，要么已经投了，要么 reply.Term > rf.term, convert to Follower
func (rf *Raft) ProcessRequestVoteReply(reply *RequestVoteReply) bool {
  defer rf.persistLocked()
	if reply.VoteGranted && reply.Term == rf.GetCurrnetTermLocked() {
		DPrintf("S%d Got 1 Vote\n", rf.me)
		return true
	}
	if reply.Term > rf.GetCurrnetTermLocked() {
    rf.ToFollower(reply.Term)
	}
	return false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, respChan chan *RequestVoteReply) bool {
	reply := &RequestVoteReply{}
  ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
  if !ok {
    Dprintf("---[SEND WARN] RequstVote S%d %s -> S%d FAILED!\n", rf.me, rf.GetRaftStateLocked(), server)
  }
  timer := RandTimer()
  if rf.GetRaftStateLocked() != Candidate {
    return false
  }
  select {
  case <-timer:
    return false
  case respChan <- reply:
  }
	return true
}

func (rf *Raft) FollowerLoop() {
	for rf.GetRaftStateLocked() == Follower {
		update := false
		var err error = nil
		select {
		case e := <-rf.ch:
			switch req := e.target.(type) {
			case *RequestVoteArgs:
				e.returnValue, update = rf.ProcessRequestVoteRequest(req)
			case *AppendEntriesArgs:
				e.returnValue, update = rf.ProcessAppendEntriesRequest(req)
      case *InstallSnapshotArgs:
        e.returnValue, _ = rf.ProcessInstallSnapshotRequest(req)
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
	for rf.GetRaftStateLocked() == Candidate {
		if rf.killed() == true {
			return
		}
    // 第一次选举或选举超时
		if doVote {
      rf.UpdateCurrentTermLocked(1)
      rf.SetVotedForLocked(rf.me)
			rf.PrintState("Election", true)
			term := rf.GetCurrnetTermLocked()
      lastLogIndex, lastLogTerm := rf.GetLastInfoLocked()
			rf.ResetTime(RandTime())
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
      case *InstallSnapshotArgs:
        e.returnValue, _ = rf.ProcessInstallSnapshotRequest(req)
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
	for rf.GetRaftStateLocked() == Leader {
		var err error = nil
		select {
		case e := <-rf.ch:
			switch req := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, _ = rf.ProcessAppendEntriesRequest(req)
			case *RequestVoteArgs:
				e.returnValue, _ = rf.ProcessRequestVoteRequest(req)
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
  defer rf.persistLocked()
  rf.lock.RLock()
  index := rf.GetLastLogIndex()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if !isLeader {
		isLeader = false
    rf.lock.RUnlock()
		return index, term, isLeader
	}
  rf.lock.RUnlock()

  rf.lock.Lock()
	newLog := &Log{rf.GetLastLogIndex() + 1, term, command}
	rf.AppendLog(newLog)
  dbg("S%d recevied NEWLOG: %v", rf.me, newLog)
  index = rf.GetLastLogIndex()
  rf.lock.Unlock()

	return index, term, isLeader
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
  rf.msgCh<-newMsg
}

func (rf *Raft) CommitLoop() {
  //rf.lk.Lock()
  for rf.killed() == false {
    rf.lock.Lock()
    for idx := rf.GetLastApplied() + 1; idx <= rf.GetCommitIndex(); idx ++ {
      rf.Commit(idx)
    }
    rf.lock.Unlock()
    time.Sleep(10 * time.Millisecond)
    //rf.cond.Wait()
  }
}

func (rf *Raft) DebugLoop() {
  for rf.killed() == false {
    rf.PrintState("DEBUG", true)
    time.Sleep(100 * time.Millisecond)
  }
}

func (rf *Raft) SnapshotLoop() {
}

func (rf *Raft) Loop() {
  go rf.CommitLoop()
  //go rf.DebugLoop()
	for rf.killed() == false {
		if rf.killed() {
      rf.Wg.Wait()
			return
		}
		switch rf.GetRaftStateLocked() {
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
    lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		leaderID:    -1,
	}
  rf.timer = time.NewTicker(RandTime())
  rf.cond = sync.NewCond(&rf.lk)
	// initialize from state persisted before a crash
	rf.logs = append(rf.logs, Log{0, 0, void})
	rf.readPersist(persister.ReadRaftState())
  rf.lock.Lock()
  rf.state = Follower
  rf.offset = 0
  rf.PrintState("Reboot", true)
	rf.msgCh = applyCh
	for i := 0; i < len(peers); i++ {
    rf.SetNextIndex(i, rf.GetLastLogIndex() + 1)
    rf.SetMatchIndex(i, 0)
	}
  rf.lock.Unlock()

	go rf.Loop()
	return rf
}

func (rf *Raft) InitArgs() *AppendEntriesArgs {
  rf.lock.RLock()
  args := &AppendEntriesArgs{
    Term:         rf.GetCurrentTerm(),
    LeaderId:     rf.me,
    PrevLogIndex: NULL,
    PrevLogTerm:  NULL,
    LeaderCommit: rf.GetCommitIndex(),
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

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.killedCh <- rf.dead
	DPrintf("server#%d killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) PrintState(pos string, ok bool) {
	if !Debug {
		ok = false
	}
	if ok {
    //fmt.Printf("[%s] S%d %s Term=%d lastLogIndex=%d lastApplied=%d commitIndex=%d\n----------Entries=%d\n",
    //pos, rf.me, rf.state, rf.currentTerm, rf.lastLogIndex, rf.lastApplied, rf.commitIndex, rf.logs[0:len(rf.logs)])
    fmt.Printf("[%s] S%d %s Term=%d lastLogIndex=%d lastApplied=%d commitIndex=%d offset=%d\n\n",
    pos, rf.me, rf.state, rf.currentTerm, len(rf.logs) - 1, rf.lastApplied, rf.commitIndex, rf.offset)
	}
}
// 可能有 data race， 但概率很小 
func (rf *Raft) ResetTime(duration time.Duration) {
	rf.timer = time.NewTicker(duration)
}

func (rf *Raft) GetLog(idx int) Log {
  return rf.logs[idx - rf.offset]
}

func (rf *Raft) GetLogLocked(idx int) Log {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.logs[idx - rf.offset]
}

func (rf *Raft) GetRaftState() string {
  return rf.state
}

func (rf *Raft) GetRaftStateLocked() string {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.state
}

func (rf *Raft) GetCurrentTerm() int {
  return rf.currentTerm
}

func (rf *Raft) GetCurrnetTermLocked() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.currentTerm
}

func (rf *Raft) GetLastLogTerm() int {
  return rf.lastLogTerm
}

func (rf *Raft) GetLastLogTermLocked() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.lastLogTerm
}

func (rf *Raft) GetLastLogIndex() int {
  return rf.lastLogIndex
}

func (rf *Raft) GetLastLogIndexLocked() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.lastLogIndex
}

func (rf *Raft) GetLastInfoLocked() (int, int) {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.lastLogIndex, rf.lastLogTerm
}

func (rf *Raft) GetLogTerm(index int) int {
  return rf.logs[index - rf.offset].Term
}

func (rf *Raft) GetLogTermLocked(index int) int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.logs[index - rf.offset].Term
}

// return (nextIndex[server], matchIndex[server])
func (rf *Raft) GetIndexInfo(server int) (int, int) {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.nextIndex[server], rf.matchIndex[server]
}

func (rf *Raft) GetVotedFor() int {
  return rf.votedFor
}

func (rf *Raft) GetVotedForLocked() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.votedFor
}

func (rf *Raft) GetCommitIndex() int {
  return rf.commitIndex
}

func (rf *Raft) GetCommitIndexLocked() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.commitIndex
}

func (rf *Raft) GetLeaderID() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.leaderID
}

func (rf *Raft) GetLeaderIDLocked() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.leaderID
}

func (rf *Raft) SetRaftState(new_state string) {
  rf.state = new_state
}

func (rf *Raft) SetRaftStateLocked(new_state string) {
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
  rf.persist()
}

func (rf *Raft) SetCurrentTermLocked(term int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetCurrentTerm(term)
}

func (rf *Raft) UpdateCurrentTerm(val int) {
  rf.currentTerm += val
  rf.SetVotedFor(-1)
  // rf.SetVotedFor has persisted
}

func (rf *Raft) UpdateCurrentTermLocked(val int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.UpdateCurrentTerm(val)
}

func (rf *Raft) SetMatchIndex(server, newMatchIndex int) {
  dbg("[INFO] Set S%d.matchIndex[S%d]=%d -> %d", rf.me,server,rf.matchIndex[server], newMatchIndex)
  rf.matchIndex[server] = newMatchIndex
}

func (rf *Raft) SetMatchIndexLocked(server, newMatchIndex int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetMatchIndex(server, newMatchIndex)
}

func (rf *Raft) SetNextIndex(server, newNextIndex int) {
  dbg("[INFO] Set S%d.nextIndex[S%d]=%d -> %d", rf.me, server, rf.nextIndex[server], newNextIndex) 
  rf.nextIndex[server] = newNextIndex
}
func (rf *Raft) SetNextIndexLocked(server, newNextIndex int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetNextIndex(server, newNextIndex)
}
func (rf *Raft) UpdateIndexInfo(server, valIndex, valMatch int) {
  rf.nextIndex[server] += valIndex
  rf.matchIndex[server] += valMatch
}

func (rf *Raft) UpdateIndexInfoLocked(server, valIndex, valMatch int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.UpdateIndexInfo(server, valIndex, valMatch)
}

func (rf *Raft) SetVotedFor(newVotedFor int) {
  rf.votedFor = newVotedFor
  rf.persist()
}

func (rf *Raft) SetVotedForLocked(newVotedFor int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetVotedFor(newVotedFor)
}

func (rf *Raft) SetCommitIndex(newCommitIndex int) {
  rf.commitIndex = newCommitIndex
}

func (rf *Raft) SetCommitIndexLocked(newCommitIndex int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetCommitIndex(newCommitIndex)
}

func (rf *Raft) SetLeaderID(newID int) {
  rf.leaderID = newID
}

func (rf *Raft) SetLeaderIDLocked(newID int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetLeaderID(newID)
}

func (rf *Raft) GetOffsetTerm() int {
  return rf.offsetTerm
}

func (rf *Raft) GetOffsetTermLocked() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.offsetTerm
}

func (rf *Raft) GetOffset() int {
  return rf.offset
}

func (rf *Raft) GetOffsetLocked() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.offset
}

func (rf *Raft) SetOffset(newOffset int) {
  rf.offset = newOffset
}

func (rf *Raft) SetOffsetLocked(newOffset int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.SetOffset(newOffset)
}

func (rf *Raft) SetOffsetTerm(term int) {
  rf.offsetTerm = term
}

func (rf *Raft) SetOffsetTermLocked(term int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.offsetTerm = term
}

func (rf *Raft) GetNextIndex(server int) int {
  return rf.nextIndex[server]
}

func (rf *Raft) GetNextIndexLocked(server int) int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.GetNextIndex(server)
}

func (rf *Raft) SetLastLogIndex(index int) {
  rf.lastLogIndex = index
}

func (rf *Raft) SetLastLogIndexLocked(index int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.lastLogIndex = index
}

func (rf *Raft) SetLastLogTerm(term int) {
  rf.lastLogTerm = term
}

func (rf *Raft) SetLastLogTermLocked(term int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.lastLogTerm = term
}

func (rf *Raft) SetSnapshot(newSnapshot []byte) {
  copy(rf.snapshot, newSnapshot)
}

func (rf *Raft) SetSnapshotLocked(newSnapshot []byte) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  copy(rf.snapshot, newSnapshot)
}

func (rf *Raft) SetLogs(newLog []Log) {
  rf.logs = newLog
}

func (rf *Raft) SetLogsLocked(newLog []Log) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.logs = newLog
}

func (rf *Raft) GetLastApplied() int {
  return rf.lastApplied
}

func (rf *Raft) GetLastAppliedLocked() int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.lastApplied
}

func (rf *Raft) SetLastApplied(newApplied int) {
  rf.lastApplied = newApplied
}

func (rf *Raft) SetLastAppliedLocked(newApplied int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.lastApplied = newApplied
}

func (rf *Raft) GetMatchIndex(server int) int {
  return rf.matchIndex[server]
}

func (rf *Raft) GetMatchIndexLocked(server int) int {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  return rf.matchIndex[server]
}
