package raft

// TODO:

import (
	"encoding/json"
	"errors"
	"fmt"

	"bytes"
	"math/rand"
	"sort"
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
	HeartBeatTime                   = 100 * time.Millisecond
	ElectionTimeout                 = 400
	ShouldUpdateTerm                = "ShouldUpdateTerm"
	Inconsistency                   = "Inconsistency"
	NULL                            = 0
	ElectionTimeoutThresholdPercent = 0.8
	MaxLog                          = 30
)

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
	applyBuf  chan ApplyMsg
	snapshot  []byte
	ch        chan *ev
	// --- Persistent state on all servers ---
	currentTerm int
	votedFor    int
	logs        []Log
	// ----------------------------------------
	state        string
	commitIndex  int // index of the highest log entry known to be committed
	lastApplied  int // index of the highest log entry applied to state machine
	nextIndex    []int
	matchIndex   []int
	timer        *time.Ticker
	leaderID     int
	cond         *sync.Cond
	lk           sync.Mutex
	offset       int
	offsetTerm   int
	lastLogIndex int
	lastLogTerm  int
	Wg           sync.WaitGroup
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
	rf.ResetTime(RandTime())
	rf.lock.Unlock()
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
		rf.SetNextIndex(i, rf.lastLogIndex+1) // already +1
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
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.lastLogTerm)
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
	var currentTerm, votedFor, lastLogIndex, lastLogTerm, offset, offsetTerm int
	var logs []Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastLogIndex) != nil {
		Dprintf("---[ERROR] readPersist Failed!\n")
		return
	}
	if d.Decode(&lastLogTerm) != nil || d.Decode(&offset) != nil || d.Decode(&offsetTerm) != nil {
		Dprintf("---[ERROR] readPersist Failed!\n")
		return
	} else {
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.logs = logs
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastLogIndex = lastLogIndex
		rf.lastLogTerm = lastLogTerm
		rf.offset = offset
		rf.offsetTerm = offsetTerm
		rf.lastApplied = offset
		//rf.SetLastApplied(lastApplied)
		//rf.persist()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.PrintState(fmt.Sprintf("Snapshot index=%d", index), true)

	rf.lock.Lock()
	if index <= rf.offset {
		rf.lock.Unlock()
		return
	}
	rf.PrintState("BeforeSnapShot", true)
	// 保留 (index, ) 内的日志
	temp := make([]Log, rf.lastLogIndex-index+1)
	copy(temp[0:1], rf.logs[0:1])
	copy(temp[1:], rf.logs[index-rf.offset+1:])
	rf.SetOffsetTerm(rf.GetLog(index).Term)
	rf.SetLogs(temp)
	rf.SetSnapshot(snapshot)
	rf.snapshot = snapshot
	rf.SetOffset(index)
	rf.persist()

	rf.lock.Unlock()
	if rf.state == Leader {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				// send InstallSnapshotRPC, if the logs send to follower discarded
				if rf.GetNextIndexLocked(server) <= index {
					rf.sendInstallSnapshot(server)
				}
			}(i)
		}
	}
	//rf.applyCond.Broadcast()
	//rf.applylk.Unlock()

	rf.PrintState("AfterSnapshot", true)
}

func (rf *Raft) ProcessInstallSnapshotRequest(args *InstallSnapshotArgs) (*InstallSnapshotReply, bool) {
	reply := &InstallSnapshotReply{
		Term: rf.GetCurrnetTermLocked(),
	}
	// request expired
	if args.Term < reply.Term {
		return reply, false
	}
	if args.Term > reply.Term || rf.GetRaftStateLocked() != Follower {
		// 遇到 term 大于自己 or leader，退回到 follower 或者更新状态
		rf.ToFollower(args.Term)
	}
	rf.SetLeaderID(args.LeaderId)
	// snapshot expired
	if args.LastIncludedIndex <= rf.GetOffsetLocked() {
		return reply, true
	}
	rf.PrintState("BeforeInstallSnapshot", true)
	// a RPC includes the entire log entries, there's no chunks.
	// if there's a log entrie not exist in the server's log entries, discard all the log entries the server has
	if rf.GetLastLogIndexLocked() <= args.LastIncludedIndex || rf.GetLogTermLocked(args.LastIncludedIndex) != args.LastIncludedTerm {
		rf.lock.Lock()
		temp := make([]Log, 1)
		temp[0] = rf.logs[0]
		rf.logs = temp
	} else {
		// otherwise, discrad the prefix of the log entries
		rf.lock.Lock()
		var temp []Log
		temp = append(temp, rf.logs[0])
		for i := args.LastIncludedIndex - rf.offset + 1; i < len(rf.logs); i++ {
			temp = append(temp, rf.logs[i])
		}
		copy(rf.logs, temp)
	}
	rf.SetOffset(args.LastIncludedIndex)
	rf.SetOffsetTerm(args.LastIncludedTerm)
	rf.SetSnapshot(args.Data)
	rf.SetCommitIndex(max(rf.GetCommitIndex(), args.LastIncludedIndex))
	rf.SetLastApplied(max(rf.GetLastApplied(), args.LastIncludedIndex))
	rf.SetLastLogIndex(max(rf.GetLastLogIndex(), args.LastIncludedIndex))
	rf.SetLastApplied(max(rf.GetLastApplied(), args.LastIncludedIndex))
	rf.SetLastLogTerm(rf.GetLogTerm(rf.GetLastLogIndex()))
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.GetOffsetTerm(),
		SnapshotIndex: rf.GetOffset(),
	}
	rf.persist()

	rf.lock.Unlock()
	//rf.applylk.Lock()

	rf.msgCh <- msg
	//rf.applyCond.Broadcast()
	//rf.applylk.Unlock()

	rf.PrintState("AfterInstallSnapshot", true)
	return reply, true
}

func (rf *Raft) sendInstallSnapshot(server int) bool {
	rf.lock.RLock()
	args := &InstallSnapshotArgs{
		Term:              rf.GetCurrentTerm(),
		LeaderId:          rf.me,
		LastIncludedIndex: rf.GetOffset(),
		LastIncludedTerm:  rf.GetOffsetTerm(),
		Data:              rf.snapshot,
	}
	rf.lock.RUnlock()
	reply := &InstallSnapshotReply{}
	Dprintf("---[sendSnapShot] S%d -> S%d index=%d term=%d\n", rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm)
	ok := rf.peers[server].Call("Raft.InstallSnapshotHandler", args, reply)
	if !ok {
		DPrintf("[WARN] send FAILED")
		return false
	}
	if reply.Term > rf.GetCurrnetTermLocked() {
		rf.ToFollower(reply.Term)
		return true
	}
	rf.lock.Lock()
	rf.SetNextIndex(server, args.LastIncludedIndex+1)
	rf.SetMatchIndex(server, max(rf.GetMatchIndex(server), args.LastIncludedIndex))
	rf.lock.Unlock()
	return true
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	e := &ev{target: args, ch: make(chan error, 1)}
	rf.ch <- e
	timer := RandTimer()
	select {
	case <-e.ch:
		temp, _ := json.Marshal(e.returnValue.(*InstallSnapshotReply))
		json.Unmarshal(temp, reply)
	case <-timer:
		return
	}
}

func (rf *Raft) sendHeartBeat(server int) {
	for rf.GetRaftStateLocked() == Leader {
		if rf.killed() {
			return
		}
		timer := time.NewTimer(HeartBeatTime)
		go rf.sendAppendEntries(server, rf.InitArgs())
		<-timer.C
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {
	reply := &AppendEntriesReply{}
	args.LeaderId = rf.me
	nextIndex := rf.GetNextIndexLocked(server)
	// peer's nextIndex 小于 offset，直接发送一个 InstallSnapshot 给他安排一个快照
	if nextIndex <= rf.GetOffsetLocked() {
		rf.sendInstallSnapshot(server)
		return false
	}
	if rf.GetNextIndexLocked(server)-1 <= rf.GetLastLogIndexLocked() {
		rf.lock.RLock()
		// PrevLogIndex 是找到匹配的位置，nextIndex 是要发送的日志序列里的第一个日志下标
		args.PrevLogIndex = rf.GetNextIndex(server) - 1
		args.PrevLogTerm = rf.GetLogTerm(args.PrevLogIndex)
		if args.PrevLogIndex == rf.GetOffset() {
			args.PrevLogTerm = rf.GetOffsetTerm()
		}
		args.Entries = make([]Log, rf.GetLastLogIndex()-rf.GetNextIndex(server)+1)
		copy(args.Entries, rf.logs[rf.GetNextIndex(server)-rf.GetOffset():])
		rf.lock.RUnlock()
	}

	args.LeaderCommit = rf.GetCommitIndexLocked()
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)

	if !ok {
		Dprintf("---[SEND FAIL] AppendEntries: S%d -> S%d FAILED\n", rf.me, server)
		return false
	}
	Dprintf("---[INFO] AE: S%d to S%d index=%d lastIndex=%d reply=%+v\n", rf.me, server, rf.nextIndex[server], rf.lastLogIndex, reply)

	rf.ProcessAppendEntriesReply(args, reply)
	return true
}

func (rf *Raft) ProcessAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.persistLocked()
	Dprintf("---[ProcessAppendEntriesReply] S%d Processing reply from S%d\nreply=%+v\n", rf.me, reply.Server, reply)

	if args.Term != rf.GetCurrnetTermLocked() {
		return
	}
	// term < follower's term，更新自己的 term，并退回到 follower
	if reply.State == ShouldUpdateTerm {
		Dprintf("---[TurnFollower] S%d ShouldUpdateTerm\n", rf.me)
		rf.ToFollower(reply.Term)
		return
	}
	// 日志存在不一致，进行回退，优化核心思想：每次跳一个 term 而不是一个 log
	if reply.Success == false && reply.State == Inconsistency {

		rf.lock.Lock()
		server := reply.Server
		if reply.LogLength+1 < rf.nextIndex[server] { // follower 日志长度小
			rf.SetNextIndex(server, reply.LogLength+1)
		} else { // 日志冲突
			idx := args.PrevLogIndex
			// 找到 leader 对应冲突 term 的最后一个日志
			for idx > rf.GetOffset() && rf.GetLogTerm(idx) != reply.ConflictTerm {
				idx--
			}
			if rf.GetLogTerm(idx) != reply.ConflictTerm {
				// 不存在对应term的日志，nextIndex 直接退回到 follower 这个 term 的第一个位置
				idx = reply.ConflictIndex
			} else {
				// 存在则将 nextIndex 设置为这个 term 最后index的 下一个日志
				// 这样下次发送时，如果匹配成功，在 follower上可以直接覆盖冲突 Term 之后的所有日志。
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
		rf.SetNextIndex(reply.Server, args.PrevLogIndex+len(args.Entries)+1)
		rf.SetMatchIndex(reply.Server, args.PrevLogIndex+len(args.Entries))
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
		newCommitIndex := indexs[len(rf.peers)/2]
		commitIndex := rf.GetCommitIndex()
		newCommitIndexTerm := rf.GetLogTerm(newCommitIndex)
		currTerm := rf.GetCurrentTerm()
		rf.lock.Unlock()
		// 超过半数节点的信息可以更新，当 logTerm 和 currentTerm 相同时更新 commitIndex。
		if newCommitIndex > commitIndex && newCommitIndexTerm == currTerm {
			rf.lk.Lock()
			rf.SetCommitIndexLocked(newCommitIndex)
			rf.lk.Unlock()
			rf.cond.Broadcast()
		}
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
		// 涉及状态修改，需要持久化
		rf.persistLocked()
		DPrintf("AppendEntries Recevied S%d -> S%d\nargs:%+v\nreply:%+v\n", args.LeaderId, rf.me, args, reply)
	}()

	currTerm := rf.GetCurrnetTermLocked()
	reply = &AppendEntriesReply{
		Server:  rf.me,
		Success: false,
		Term:    currTerm,
	}
	// term 小于自己，直接返回 false，并设置 reply.State 的状态是 leader 应该更新自己的 term
	if args.Term < currTerm {
		reply.State = ShouldUpdateTerm
		// 不重置选举时间
		return reply, false
	} else {
		// term 大于自己，更新 term
		// 如果不是 follower 需要退回到 follower。
		if rf.GetRaftStateLocked() != Follower {
			Dprintf("---[TurnFollower] S%d is not follower -> follower", rf.me)
			rf.ToFollower(args.Term)
		} else if args.Term > rf.GetCurrnetTermLocked() {
			rf.ToFollower(args.Term)
		}
	}
	rf.SetLeaderIDLocked(args.LeaderId)

	rf.PrintState("Recevie Entreis", true)
	DPrintf("S%d.lastIndex=%d args.prevLogindex= %d\n", rf.me, rf.GetLastLogIndex(), args.PrevLogIndex)

	// 日志长度小于 leader，返回自己的日志长度
	if rf.GetLastLogIndexLocked() < args.PrevLogIndex {
		reply.State = Inconsistency
		reply.LogLength = rf.GetLastLogIndexLocked()
		// follower 需要重置自己的选举时间，类似收到了心跳
		return reply, true
	}
	if args.PrevLogIndex <= rf.offset && args.PrevLogIndex+len(args.Entries) <= rf.offset {
		reply.Success = false
		return reply, true
	}
	// 有对应的下标，但日志不统一
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
	lastLogIndex := rf.GetLastLogIndex()
	reply.LogLength = lastLogIndex
	rf.PrintState(fmt.Sprintf("Recevied S%d AppendEntries", args.LeaderId), false)
	commitIndex := rf.GetCommitIndex()
	rf.lock.Unlock()

	// leader commitIndex > follower commitIndex，更新 follower commitIndex
	if args.LeaderCommit > commitIndex {
		rf.lk.Lock()
		rf.SetCommitIndexLocked(min(args.LeaderCommit, lastLogIndex))
		rf.lk.Unlock()
		rf.cond.Broadcast()
	}
	reply.Success = true
	return reply, true
}

// lock Required before invoke AppendLog
func (rf *Raft) AppendLog(pLog *Log) {
	// 是新的 log 就添加到末尾，如果存在不一致，用 leader 的覆盖 follower 的日志
	defer rf.persist()
	if rf.GetLastLogIndex() < pLog.Index {
		rf.logs = append(rf.logs, *pLog)
		rf.SetLastLogIndex(pLog.Index)
		rf.SetLastLogTerm(pLog.Term)
	} else {
		if rf.GetLogTerm(pLog.Index) != pLog.Term {
			rf.logs = rf.logs[0 : pLog.Index-rf.offset]
			rf.SetLastLogIndex(pLog.Index - 1)
			rf.SetLastLogTerm(rf.GetLogTerm(pLog.Index - 1))
			rf.AppendLog(pLog)
		}
	}
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

	// Candidate Term 小于自己，不给投票，并且不重置自己的选举时间
	if args.Term < currTerm {
		reply.VoteGranted = false
		reply.MsgState = ShouldUpdateTerm
		return reply, false
	}
	// 收到 term 大的
	if args.Term > currTerm {
		// 如果不是 follower，需要回退到 follower
		if rf.GetRaftStateLocked() != Follower {
			Dprintf("---[TurnFollower] S%d meet higher Term\n", rf.me)
			rf.ToFollower(args.Term)
		} else {
			// 更新为新的 term，重置投票对象
			rf.SetVotedForLocked(-1)
			rf.SetCurrentTermLocked(args.Term)
		}
	}
	votedFor := rf.GetVotedForLocked()
	// 已投票返回 false，并且不重置自己的选举时间
	if votedFor != -1 {
		DPrintf("RequstVote INJECTED args:%+v", args)
		reply.VoteGranted = false
		return reply, false
	}
	// 需要进行投票，对比自己的 lastLogTerm 和 lastLogIndex
	rf.lock.Lock()
	defer rf.lock.Unlock()
	lastTerm := rf.GetLastLogTerm()
	lastIndex := rf.GetLastLogIndex()
	reply.Term = rf.GetCurrentTerm()
	// lastLogTerm 为第一优先级，lastLogIndex 为第二优先级。
	if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
		reply.VoteGranted = false
		return reply, false
	}
	// 投票成功，并重置自己的选举时间
	rf.SetVotedFor(args.CandidateId)
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
			// 要发起选举，++term，请求携带上自己的lastLogTerm,lastLogIndex,term
			rf.lock.Lock()
			rf.UpdateCurrentTerm(1)
			rf.SetVotedFor(rf.me)
			term := rf.GetCurrentTerm()
			lastLogIndex, lastLogTerm := rf.GetLastInfo()
			rf.ResetTime(RandTime())

			rf.PrintState("Election", true)

			rf.lock.Unlock()
			// TODO: Pre-Vote 优化，预先发起一次投票看有没有半数节点活着
			// 发送给其他 peers 投票请求，注意这里不要阻塞
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(server int) {
					args := &RequestVoteArgs{
						Term:         term,
						CandidateId:  rf.me,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					}
					rf.sendRequestVote(server, args, respChan)
				}(i)
			}
			voteCount = 1
			doVote = false
		}
		// 超过半数投票成为 leader
		if voteCount*2 > len(rf.peers) {
			rf.ToLeader()
			return
		}
		if rf.killed() {
			return
		}
		select {
		case resp := <-respChan:
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
			case *InstallSnapshotArgs:
				e.returnValue, _ = rf.ProcessInstallSnapshotRequest(req)
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

// lock Required
func (rf *Raft) Commit(log Log) {
	newMsg := ApplyMsg{
		CommandValid:  true,
		Command:       log.Command,
		CommandIndex:  log.Index,
		SnapshotValid: false,
	}

	rf.msgCh <- newMsg
	rf.lastApplied = log.Index

	DPrintf("S%d Submit {Index: %d, Command %v}\n", rf.me, log.Index, log.Command)
	rf.PrintState("AfterSubmit", true)
}

func (rf *Raft) CommitLoop() {
	//rf.lk.Lock()
	for rf.killed() == false {
		lastApplied := rf.GetLastAppliedLocked()
		commitIndex := rf.GetCommitIndexLocked()
		if lastApplied < commitIndex {
			logs := rf.GetLogSliceLocked(lastApplied+1, commitIndex+1)

			DPrintf("---[CommitLoop] logs=%+v\n", logs)

			for _, log := range logs {
				rf.Commit(log)
			}
		}
		time.Sleep(10 * time.Millisecond)
		//rf.cond.Wait()
	}
}

func (rf *Raft) DebugLoop() {
	for rf.killed() == false {
		rf.PrintState("DEBUG", true)
		time.Sleep(200 * time.Millisecond)
	}
}

/*
func (rf *Raft) ApplyLoop() {
  for rf.killed() == false {
    for temp := range rf.applyBuf {
      if rf.killed() {
        break
      }
      rf.msgCh<-temp
      DPrintf("[ApplyLoop] S%d apply %+v\n", rf.me, temp)
    }
  }
}
*/

func (rf *Raft) Loop() {
	go rf.CommitLoop()
	// go rf.ApplyLoop()
	// go rf.DebugLoop()
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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
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
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		leaderID:    -1,
		applyBuf:    make(chan ApplyMsg, 256),
	}
	rf.cond = sync.NewCond(&rf.lk)
	rf.snapshot = nil

	rf.timer = time.NewTicker(RandTime())
	rf.logs = append(rf.logs, Log{0, 0, void})
	// initialize from state persisted before a crash
	rf.state = Follower
	rf.msgCh = applyCh
	rf.readPersist(persister.ReadRaftState())
	rf.PrintState("Reboot", true)

	for i := 0; i < len(peers); i++ {
		rf.SetNextIndex(i, rf.GetLastLogIndex()+1)
		rf.SetMatchIndex(i, 0)
	}

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

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.killedCh <- rf.dead
	DPrintf("server#%d killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func RandTime() time.Duration {
	ms := ElectionTimeout + (rand.Int63() % ElectionTimeout)
	return time.Duration(ms) * time.Millisecond
}

func RandTimer() <-chan time.Time {
	timer := time.NewTimer(RandTime())
	return timer.C
}
