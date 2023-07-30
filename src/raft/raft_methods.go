package raft

import (
  "time"
  "fmt"
)

func (rf *Raft) PrintState(pos string, ok bool) {
	if !Debug {
		ok = false
	}
	if ok {
    //fmt.Printf("[%s] S%d %s Term=%d lastLogIndex=%d lastApplied=%d commitIndex=%d\n----------Entries=%d\n",
    //pos, rf.me, rf.state, rf.currentTerm, rf.lastLogIndex, rf.lastApplied, rf.commitIndex, rf.logs[0:len(rf.logs)])
    fmt.Printf("[%s] S%d %s Term=%d lastLogIndex=%d lastApplied=%d commitIndex=%d offset=%d\n---Entries=%+v\n\n",
    pos, rf.me, rf.state, rf.currentTerm, rf.GetLastLogIndex(), rf.lastApplied, rf.commitIndex, rf.offset, rf.logs)
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

func (rf *Raft) GetLastInfo() (int, int) {
  return rf.lastLogIndex, rf.lastLogTerm
}

func (rf *Raft) GetLogTerm(index int) int {
  ret := rf.logs[index - rf.offset].Term
  if index == rf.offset {
    ret = rf.offsetTerm
  }
  return ret
}

func (rf *Raft) GetLogTermLocked(index int) int {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  ret := rf.logs[index - rf.offset].Term
  if index == rf.offset {
    ret = rf.offsetTerm
  }
  return ret
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
  rf.matchIndex[server] = max(rf.matchIndex[server], newMatchIndex)
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
  if index == 0 {
    index = rf.offset
  }
  rf.lastLogIndex = index
  rf.persist()
}

func (rf *Raft) SetLastLogIndexLocked(index int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  if index == 0 {
    index = rf.offset
  }
  rf.lastLogIndex = index
  rf.persist()
}

func (rf *Raft) SetLastLogTerm(term int) {
  rf.lastLogTerm = term
  rf.persist()
}

func (rf *Raft) SetLastLogTermLocked(term int) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.lastLogTerm = term
  rf.persist()
}

func (rf *Raft) SetSnapshot(newSnapshot []byte) {
  rf.snapshot = make([]byte, len(newSnapshot))
  copy(rf.snapshot, newSnapshot)
}

func (rf *Raft) SetSnapshotLocked(newSnapshot []byte) {
  rf.lock.Lock()
  defer rf.lock.Unlock()
  rf.snapshot = make([]byte, len(newSnapshot))
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

func (rf *Raft) GetLogSlice(L, R int) []Log {
  ret := make([]Log, R - L)
  copy(ret, rf.logs[L - rf.offset: R - rf.offset])
  return ret
}

func (rf *Raft) GetLogSliceLocked(L, R int) []Log {
  rf.lock.RLock()
  defer rf.lock.RUnlock()
  ret := make([]Log, R - L)
  copy(ret, rf.logs[L - rf.offset: R - rf.offset])
  return ret
}


