package raft

// TODO: 
// 

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
	"errors"
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
	Leader                          = "Leader"
	Follower                        = "Follower"
	Candidate                       = "Candidate"
	HearBeatTime                    = 100 * time.Millisecond
	ElectionTimeout                 = 500
	ShouldUpdateTerm                = "ShouldUpdateTerm"
  Inconsistency                   = "Inconsistency"
	NULL                            = 0
	ElectionTimeoutThresholdPercent = 0.8
)

type SendLog struct {
}

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
	state       string
	currentTerm int
	votedFor    int
	logs        []Log
	commitIndex int // index of the highest log entry known to be committed
  lastApplied int // index of the highest log entry applied to state machine
	lastLogIndex int 
	lastLogTerm int
	// leader fields
	nextIndex  []int
	matchIndex []int
	overtime   time.Duration
	timer      *time.Ticker
	leaderID   int
  cond *sync.Cond
  lk sync.Mutex
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

func (rf *Raft) GetLastInfo() (int, int) {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.lastLogIndex, rf.lastLogTerm
}

// return (nextIndex[server], matchIndex[server])
func (rf *Raft) GetIndexInfo(server int) (int, int) {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  return rf.nextIndex[server], rf.matchIndex[server]
}

func (rf *Raft) GetVotedFor(server int) int {
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

func (rf *Raft) Setstate(new_state string) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.state = new_state
}

// Set rf.votedFor too
func (rf *Raft) SetCurrentTerm(term int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  if term < rf.currentTerm {
    return 
  }
  rf.currentTerm = term
  rf.votedFor = -1
}

func (rf *Raft) UpdateCurrentTerm(val int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.currentTerm += val
  rf.votedFor = -1
}

func (rf *Raft) SetLastInfo(lastLogIndex, lastLogTerm int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.lastLogIndex = lastLogIndex
  rf.lastLogTerm = lastLogTerm
}

func (rf *Raft) SetIndexInfo(server, valIndex, valMatch int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  //dbg("[INFO] Set S%d.nextIndex[S%d]=%d\n", rf.me, server, valIndex)
  rf.nextIndex[server] = valIndex
  rf.matchIndex[server] = valMatch
}

func (rf *Raft) UpdateIndexInfo(server, valIndex, valMatch int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  //dbg("[INFO] Update S%d.nextIndex[S%d]=%d\n", rf.me, server, rf.nextIndex[server] + valIndex)
  rf.nextIndex[server] += valIndex
  rf.matchIndex[server] += valMatch
}

func (rf *Raft) SetVotedFor(newVotedFor int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.votedFor = newVotedFor
}

func (rf *Raft) SetCommitIndex(newCommitIndex int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.commitIndex = newCommitIndex
}

func (rf *Raft) SetLeaderID(newID int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.leaderID = newID
}

func (rf *Raft) ToCandidate() {
	// TODO
  rf.lock.Lock()
  defer rf.lock.Unlock()
	rf.state = Candidate
	rf.votedFor = -1
}

func (rf *Raft) ToFollower(term int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
	DPrintf("S%d State: %s -> Follower Term:%d -> %d\n", rf.me, rf.state, rf.currentTerm, term)
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.ResetTime(RandTime())
}

func (rf *Raft) ToLeader() {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  Dprintf("[Leader Changed]S%d -> Leader Term:%d lastapplied:%d\n", rf.me, rf.currentTerm, rf.lastLogIndex)
	rf.state = Leader
	rf.leaderID = rf.me
	for i := 0; i < len(rf.peers); i++ {
    // initialize nextIndex to the index to lastLogIndex + 1
		if i == rf.me {
			continue
		}
    rf.nextIndex[i] = rf.lastLogIndex + 1
    /*
		go func(server int) {
			rf.sendAppendEntries(server, rf.InitArgs(), true)
		}(i)
    */
	}
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
	// Your code here (2A).
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
	MsgState    string
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log // empty for heartbeat
	LeaderCommit int   // leader's commitIndex
  HeartBeat bool
}

type AppendEntriesReply struct {
	Term    int
	Success bool
  Server int
  LastLogIndex int
	State   string
  HeartBeat bool
}

func (rf *Raft) Commit(idx int) {
  newMsg := ApplyMsg{
    CommandValid: true,
    Command: rf.logs[idx].Command,
    CommandIndex: idx,
  }
  DPrintf("S%d Submit {Index: %d, Command %v}", rf.me, idx, rf.logs[idx].Command)
  rf.PrintState("AfterSubmit", true)
  rf.lastApplied ++
  rf.msgCh <- newMsg
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, HeartBeat bool) bool {
	reply := &AppendEntriesReply{}
  args.HeartBeat = HeartBeat
 /* if rf.GetCurrnetTerm() != args.Term {*/
		/*return false*/
	/*}*/
	args.LeaderId = rf.me
    //PrintLockInfo(NULL)
  rf.lock.RLock()
  if !HeartBeat {
    Dprintf("[INFO] to S%d index=%d lastIndex=%d\n", server,rf.nextIndex[server], rf.lastLogIndex)
  }
  if rf.nextIndex[server] <= rf.lastLogIndex + 1{
    /*
    args.PrevLogIndex = rf.nextIndex[server] - 1
    args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
    for i := args.PrevLogIndex; i < len(rf.logs); i++ {
      args.Entries = append(args.Entries, rf.logs[i])
    }
    */
    args.PrevLogIndex = rf.nextIndex[server] - 1
    args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
    if !HeartBeat {
      for i := rf.nextIndex[server]; i < len(rf.logs); i++ {
        args.Entries = append(args.Entries, rf.logs[i])
      }
    }
  }
  rf.lock.RUnlock()
	args.LeaderCommit = rf.GetCommitIndex()
  ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
  if !ok {
    Dprintf("[SEND FAIL] AppendEntries: S%d -> S%d FAILED\n", rf.me, server)
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
		go rf.sendAppendEntries(server, rf.InitArgs(), true)
		<-timer.C
	}
}

// lock Required
func (rf *Raft) AppendLog(pLog *Log) bool {
  if rf.lastLogIndex == pLog.Index - 1 {
    rf.lastLogIndex++
    rf.logs = append(rf.logs, *pLog)
    rf.lastLogTerm = pLog.Term
  } else if rf.lastLogIndex >= pLog.Index {
    if rf.logs[pLog.Index].Term != pLog.Term {
      DPrintf("LOG DIFF: last=%d rf.Log=%+v pLog=%+v\n", rf.lastLogIndex, rf.logs[pLog.Index], pLog)
      rf.logs = rf.logs[0: pLog.Index]
      rf.lastLogIndex = len(rf.logs) - 1
      rf.AppendLog(pLog)
      //log.Fatal("Log DIFF")
    } else {
      DPrintf("EXISTED Log:%+v\n", pLog)
    }
  } else {
    fmt.Printf("last=%d pLog=%v\n", rf.lastLogIndex, pLog)
    log.Fatal("Log DIFF")
  }
	return true
}

// ProcessNewLog另启go routine
// 接收客户端命令追加日志->发送请求到管道 rf.ch-> 从rf.ch接收事件, 向不同server调用sendAppendEntries
func (rf *Raft) ProcessNewLog() {
	term := rf.GetCurrnetTerm()
  leaderCommit := rf.GetCommitIndex()
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
    wg.Add(1)
		go func(server int) {
			rf.sendAppendEntries(server, &AppendEntriesArgs{
        LeaderId: rf.me,
        Term: term,
        LeaderCommit: leaderCommit,
        },
        false)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) ProcessAppendEntriesReply(reply *AppendEntriesReply) {
  if !reply.HeartBeat {
    Dprintf("[ProcessAppendEntriesReply] S%d Processing reply from S%d\n", rf.me, reply.Server)
  }
	if reply.State == ShouldUpdateTerm {
    Dprintf("[TurnFollower] S%d ShouldUpdateTerm\n", rf.me)
		rf.ToFollower(reply.Term)
    return 
	}
  if reply.Success == false && reply.State == Inconsistency {
    rf.UpdateIndexInfo(reply.Server, -1, 0)
    rf.ch <- &ev{target: &Retry{reply.Server}, ch: make(chan error, 1)}
    return 
  }
  // 成功，matchIndex++ 和 nextIndex++
  // 否则，nextIndex--
  if reply.Success && !reply.HeartBeat {
    rf.lock.Lock()
    rf.nextIndex[reply.Server] = reply.LastLogIndex + 1
    rf.matchIndex[reply.Server] = reply.LastLogIndex
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
        rf.commitIndex = N
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
	rf.ch <- e
	// Process Request 之后才会返回 reply
  if len(args.Entries) > 0 {
    //dbg("AP: S%d -> S%d\n", args.LeaderId, rf.me)
  }
	select {
	case <-e.ch:
		temp := e.returnValue.(*AppendEntriesReply)
    reply.Server = temp.Server
		reply.Term = temp.Term
		reply.State = temp.State
		reply.Success = temp.Success
    reply.HeartBeat = temp.HeartBeat
    reply.LastLogIndex = temp.LastLogIndex
	}
  if len(args.Entries) > 0 {
    //dbg("AP: S%d -> S%d done\n",args.LeaderId, rf.me)
  }
}

func (rf *Raft) ProcessAppendEntriesRequest(args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{
    Server: rf.me, 
		Success: true,
		Term:    rf.currentTerm,
    HeartBeat: args.HeartBeat,
	}
	defer func() {
    //if !args.HeartBeat {
      DPrintf("AppendEntries Recevied S%d -> S%d\nargs:%+v\nreply:%+v\n", args.LeaderId, rf.me, args, reply)
    //}
    if reply.State == Inconsistency && rf.lastLogIndex >= args.PrevLogIndex {
      rf.lock.Lock()
      rf.logs = rf.logs[0: args.PrevLogIndex]
      rf.lastLogIndex = len(rf.logs) - 1
      rf.lock.Unlock()
    }
    if reply.Success == false {
      return
    }
    rf.lock.Lock()
    rf.lk.Lock()
    if args.LeaderCommit > rf.commitIndex {
      rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex)
      rf.cond.Broadcast()
      /*
      newCommitIndex := min(args.LeaderCommit, rf.lastLogIndex)
      for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
        DPrintf("S%d Submit after recevied S%d appendentries\n", rf.me, args.LeaderId)
        rf.Commit(i)
      }
      rf.commitIndex = newCommitIndex
      */
    }
    rf.lk.Unlock()
    rf.lock.Unlock()
	}()
	if args.Term < rf.GetCurrnetTerm() {
		reply.Success = false
		reply.State = ShouldUpdateTerm
		return reply, false
	} else if args.Term >= rf.GetCurrnetTerm() {
		if rf.GetRaftState() != Follower {
      Dprintf("[TurnFollower] S%d New Leader Elected\n", rf.me)
			rf.ToFollower(args.Term)
		} else if args.Term != rf.GetCurrnetTerm() {
			rf.SetCurrentTerm(args.Term)
		}
    rf.SetLeaderID(args.LeaderId)
  } 
  
  rf.PrintState("Recevie Entreis",true)
  DPrintf("last=%d pindex= %d\n", rf.lastLogIndex, args.PrevLogIndex)
  if rf.lastLogIndex < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
    reply.Success = false
    reply.State = Inconsistency
    return reply, false
  }
  rf.lock.Lock()
  for _, log_ := range args.Entries {
    rf.AppendLog(&log_)
  }
  reply.LastLogIndex = rf.lastLogIndex

  rf.PrintState(fmt.Sprintf("Recevied S%d AppendEntries", args.LeaderId), false)
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
	select {
	case <-e.ch:
		temp := e.returnValue.(*RequestVoteReply)
		reply.VoteGranted = temp.VoteGranted
		reply.MsgState = temp.MsgState
		reply.Term = temp.Term
		//DPrintf("Final Reply: %+v", reply)
	}
}

func (rf *Raft) ProcessRequestVoteRequest(args *RequestVoteArgs) (*RequestVoteReply, bool) {
	// Your code here (2A, 2B).
  curTerm := rf.GetCurrnetTerm()
	reply := &RequestVoteReply{Term: curTerm, VoteGranted: true}
	rf.PrintState(fmt.Sprintf("ProcessRequestVoteRequest S%d term=%d", args.CandidateId, args.Term), false)
	if args.Term < curTerm {
		reply.VoteGranted = false
		reply.MsgState = ShouldUpdateTerm
		return reply, false
	}
	if args.Term > curTerm {
		//DPrintf("[RequestVote] S%d Term: %d -> %d\n", rf.me, rf.currentTerm, args.Term)
		if rf.state != Follower {
      Dprintf("[TurnFollower] S%d Higher Term\n", rf.me)
			rf.ToFollower(args.Term)
		} else {
			rf.SetCurrentTerm(args.Term)
		}
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("RequstVote INJECTED args:%+v", args)
		reply.VoteGranted = false
		return reply, false
	}
	// TODO
  if args.LastLogTerm < rf.lastLogTerm || (args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex < rf.lastLogIndex) {
    reply.VoteGranted = false
    // ???
    return reply, false
  }
  
	reply.Term = rf.GetCurrnetTerm()
	//DPrintf("Vote From S%d to S%d Original Reply=%+v\n", rf.me, args.CandidateId, reply)
	rf.votedFor = args.CandidateId
	return reply, true
}

// 1. 接受了投票，则 reply.Term == rf.term
// 2. 没接受投票，要么已经投了，要么 reply.Term > rf.term, convert to Follower
func (rf *Raft) ProcessRequestVoteReply(reply *RequestVoteReply) bool {
	if reply.VoteGranted && reply.Term == rf.currentTerm {
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
    Dprintf("[SEND WARN] RequstVote S%d -> S%d FAILED!\n", rf.me, server)
  }
	//DPrintf("S%d Got RequestVote Reply: %+v", rf.me, reply)
	if rf.GetRaftState() == Candidate {
		respChan <- reply
	}

	return true
}


func (rf *Raft) FollowerLoop() {
	//since := time.Now()
	rf.ResetTime(RandTime())
	for rf.state == Follower {
		update := false
		var err error = nil
		select {
		case e := <-rf.ch:
			switch req := e.target.(type) {
			case *RequestVoteArgs:
				e.returnValue, update = rf.ProcessRequestVoteRequest(req)
			case *AppendEntriesArgs:
				//elapsedTime := time.Now().Sub(since)
				//if elapsedTime < time.Duration(float64(rf.overtime) * ElectionTimeoutThresholdPercent) {
				//}
				e.returnValue, update = rf.ProcessAppendEntriesRequest(req)
			default:
				err = nil
      //case *CommitArgs:
        //e.returnValue, update = rf.ProcessCommitRequest(req)
			}
			e.ch <- err
		case <-rf.timer.C:
			rf.ToCandidate()
		case <-rf.killedCh:
			break
		}
		if update {
			//since = time.Now()
			rf.ResetTime(RandTime())
			//rf.PrintState("FollowerUpdate", true)
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
		if doVote {
      rf.UpdateCurrentTerm(1)
      rf.SetVotedFor(rf.me)
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
	for rf.state == Leader {
		var err error = nil
		select {
		// TODO: 处理 killed
		case e := <-rf.ch:
			switch req := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, _ = rf.ProcessAppendEntriesRequest(req)
			case *RequestVoteArgs:
				e.returnValue, _ = rf.ProcessRequestVoteRequest(req)
			case *AppendEntriesReply: // 响应 AppendEntriesReply
				go rf.ProcessAppendEntriesReply(req)
			case *SendLog:
				// 能不能 go ?
				go rf.ProcessNewLog()
      case *Retry:
				// 能不能 go ?
        DPrintf("Retry S%d -> S%d\n", rf.me, req.server)
        go rf.sendAppendEntries(req.server, rf.InitArgs(), false)
      /*
      case *CommitReply:
        rf.ProcessCommitReply(req)
      case *SendCommit:
        rf.sendCommit(req.server, &CommitArgs{rf.me, req.index, req.term})
      */
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
  index, _ := rf.GetLastInfo()
	term := rf.GetCurrnetTerm()
	isLeader := rf.GetRaftState() == Leader
	if !isLeader {
		isLeader = false
		return index, term, isLeader
	}
  rf.lock.Lock()
	newLog := &Log{rf.lastLogIndex + 1, term, command}
  dbg("S%d recevied NEWLOG: %v\n", rf.me, newLog)
	rf.AppendLog(newLog)
  index = rf.lastLogIndex
  rf.lock.Unlock()
	rf.ch <- &ev{target: &SendLog{}, ch: make(chan error, 1)}
	return index, term, isLeader
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
	rf.killedCh <- rf.dead
	DPrintf("server#%d killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) CommitLoop() {
  //rf.lk.Lock()
  for rf.killed() == false {
    for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx ++ {
      rf.Commit(idx)
    }
    time.Sleep(10 * time.Millisecond)
    //rf.cond.Wait()
  }
}

func (rf *Raft) Loop() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.killed() {
			return
		}
		switch rf.state {
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
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.logs = append(rf.logs, Log{0, 0, void})
  rf.lastLogIndex = 0
	rf.timer = time.NewTicker(rf.overtime)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
  //rf.PrintState("Make", true)
	rf.msgCh = applyCh
	go rf.Loop()
  go rf.CommitLoop()

	return rf
}

func (rf *Raft) InitArgs() *AppendEntriesArgs {
  args := &AppendEntriesArgs{
    Term:         rf.GetCurrnetTerm(),
    LeaderId:     rf.me,
    PrevLogIndex: NULL,
    PrevLogTerm:  NULL,
    Entries:      nil,
    LeaderCommit: rf.GetCommitIndex(),
  }
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

func (rf *Raft) ResetTime(duration time.Duration) {
	rf.overtime = duration
	rf.timer = time.NewTicker(rf.overtime)
}

func (rf *Raft) PrintState(pos string, ok bool) {
	if !Debug {
		ok = false
	}
	if ok {
		//fmt.Printf("[%s] Server#%d %s Term=%d comIndex=%d lastApp=%d lastTerm=%d Logs=%+v\n",
		//	pos, rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lastLogTerm, rf.logs)
		fmt.Printf("[%s] S%d %s Term=%d lastLogIndex=%d lastApplied=%d commitIndex=%d\n----------Entries=%d\n",
    pos, rf.me, rf.state, rf.currentTerm, rf.lastLogIndex, rf.lastApplied, rf.commitIndex, rf.logs[0:len(rf.logs)])
	}
}


