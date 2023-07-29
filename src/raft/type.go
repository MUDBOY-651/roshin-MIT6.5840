package raft

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

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool // true->received vote
	MsgState    string
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



