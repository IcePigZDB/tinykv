// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// my data
	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	r := &Raft{
		id: c.ID,
		// default 0
		// Term:             0,
		// Vote:             0,
		// Lead:             0,
		// State:            StateFollower,
		// msgs:             []pb.Message{},
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		// (Used in 3A leader transfer)
		// leadTransferee: uint64 ,
		// PendingConfIndex: uint64,
	}
	lastIdx := r.RaftLog.LastIndex()
	hardState, confStat, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = confStat.Nodes
		// c.peers = c.Storage.confStat.Nodes
	}
	for _, p := range c.peers {
		if p == r.id {
			r.Prs[p] = &Progress{Next: lastIdx + 1, Match: lastIdx}
		} else {
			r.Prs[p] = &Progress{Next: lastIdx + 1}
		}
	}

	r.becomeFollower(0, None)
	r.Term, r.Vote, r.RaftLog.committed = hardState.Term, hardState.Vote, hardState.Commit
	return r
}
func (r *Raft) hasLeader() bool { return r.Lead != None }

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// shortcut for newly add peer with empty entries
	if r.Prs[to].Match == 0 && r.Prs[to].Next == 0 {
		log.Infof("%x sendSanpshot to %x", r.id, to)
		r.sendSnapshot(to)
		return false
	}

	prevIdx := r.Prs[to].Next - 1
	// log.Infof("++sendAppend %x, r.Prs %v", r.id, r.Prs)
	prevLogTerm, err := r.RaftLog.Term(prevIdx)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		panic(err)
	}

	var entries []*pb.Entry
	for i := r.RaftLog.toSliceIndex(prevIdx + 1); i < len(r.RaftLog.entries); i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed, // 如果leader更新了commit，通过这个来和follower同步
		LogTerm: prevLogTerm,
		Index:   prevIdx,
		Entries: entries,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, logTerm, idx uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		LogTerm: logTerm,
		Index:   idx,
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  commit,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendRequestVote sends a Request RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64, lastIdx uint64, lastTerm uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   lastIdx,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		// TODO tick transferee
		r.tickHeartbeat()
	}
}
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}

}
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}

}

func (r *Raft) resetTimer() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// r.votes = make(map[uint64]bool)
	// r.Lead = None
}

// candidate 2 follower
// leader 2 follower
// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.resetTimer()
	r.leadTransferee = None
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.resetTimer()
	r.State = StateCandidate
	r.Term++
	r.Lead = None
	r.votes = make(map[uint64]bool)
	r.Vote = r.id
	r.votes[r.Vote] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	lastIdx := r.RaftLog.LastIndex()
	for p := range r.Prs {
		if p == r.id { // because of noop entry
			r.Prs[p].Next = lastIdx + 2
			r.Prs[p].Match = lastIdx + 1
		} else {
			r.Prs[p].Next = lastIdx + 1
		}
	}
	// ** append empty entry
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: lastIdx + 1})
	r.bcastAppend()
	log.Infof("++++ becomLeader: %d", r.id)
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// log.Info("++++ raftID:%d,%s,%s", r.id, r.State, m.MsgType)
	if m.Term > r.Term {
		// NOTE put this line into becomeFollower
		// r.leadTransferee = None
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeaderNotLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		if _, ok := r.Prs[r.id]; !ok {
			log.Infof("%x not in r.Prs, do not do Election after MsgTimeoutNow", r.id)
			return
		}
		r.doElection()
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeaderNotLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		log.Infof("%x [term %d state %v] ignored MsgTimeoutNow from %x",
			r.id, r.Term, r.State, m.From)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.appendEntries(m.Entries)
	case pb.MessageType_MsgAppend:
		// may be leader need not this
		if r.leadTransferee == None {
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State != StateLeader {
			r.handleRequestVoteResponse(m)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgHeartbeatResponse:
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeaderLeader(m)
	case pb.MessageType_MsgTimeoutNow:

	}
}

func (r *Raft) doElection() {

	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	lastIdx := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIdx)
	if err != nil {
		panic(err)
	}
	for p := range r.Prs {
		if p != r.id {
			r.sendRequestVote(p, lastIdx, lastTerm)
		}
	}

}

func (r *Raft) bcastHeartbeat() {
	for p := range r.Prs {
		if p != r.id {
			r.sendHeartbeat(p)
		}
	}
}
func (r *Raft) handleRequestVote(m pb.Message) {
	// 1. Reply false if term < currentTerm
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	// 2.a if voteFor is null
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	//2.b or candidateId,and candidate's log
	// is at least as up-to=date as receiver'slog,grant vote
	lastIdx := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastIdx)
	if err != nil {
		panic(err)
	}
	if lastLogTerm > m.LogTerm || lastLogTerm == m.LogTerm && lastIdx > m.Index {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	// r.resetTimer()
	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendRequestVoteResponse(m.From, false)
}
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	grant := 0
	votes := len(r.votes)
	// int div 5/2=2,6/2=3
	// so grant /votes should > threshold
	threshold := len(r.Prs) / 2
	for _, g := range r.votes {
		if g {
			grant++
		}
	}
	if grant > threshold {
		//
		r.becomeLeader()
	} else if (votes - grant) > threshold {
		r.becomeFollower(r.Term, None)
	}

}
func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastIdx := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Term = r.Term
		entry.Index = lastIdx + uint64(i) + 1
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None {
				// pass entry if PendingConfIndex
				continue
			}
			cc := &eraftpb.ConfChange{}
			cc.Unmarshal(entry.Data)
			log.Infof("++++appendEntries ConfChange %v data%v", r.id, cc)
			r.PendingConfIndex = entry.Index
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	// reflesh leader Match & Next
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.bcastAppend()

	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}

}

func (r *Raft) bcastAppend() {
	for p := range r.Prs {
		if p != r.id {
			r.sendAppend(p)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	lastIdx := r.RaftLog.LastIndex()
	// 1. Replay false if term < currentTerm
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true, None, None)
		return
	}
	// 2. Replay false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if m.Index > lastIdx {
		// need from lastIdx+1
		r.sendAppendResponse(m.From, true, None, lastIdx+1)
		return
	}

	l := r.RaftLog
	if m.Index >= l.FirstIndex {
		logTerm, err := l.Term(m.Index)
		if err != nil {
			panic(err)
		}
		if logTerm != m.LogTerm {
			// TODO binary search must find logTerm !!!
			// idx : the smallest idx of logTerm    sort.Search(length,find_conditon)
			idx := l.toEntryIndex(sort.Search(l.toSliceIndex(m.Index+1),
				func(i int) bool { return l.entries[i].Term == logTerm }))
			r.sendAppendResponse(m.From, true, logTerm, idx)
			return
		}
	}

	for i, entry := range m.Entries {
		if entry.Index < l.FirstIndex {
			continue
		}
		if entry.Index <= l.LastIndex() {
			logTerm, err := l.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			// 3. If an existing entry conflicts with a new one
			// (same index but different terms),
			// delete the existing entry and all that follow it
			if logTerm != entry.Term {
				sliceIdx := l.toSliceIndex(entry.Index)
				l.entries[sliceIdx] = *entry
				l.entries = l.entries[:sliceIdx+1]
				// TODO 需不需要清空storage？storage is still old value (1,1) (2,2)
				// l.storage.Entries(lo uint64, hi uint64)
				l.stabled = min(entry.Index-1, l.stabled)

			}
		} else {
			// 4. Append any new entries not already in the log
			// we can add one by one too,but slower
			for j := i; j < len(m.Entries); j++ {
				l.entries = append(l.entries, *m.Entries[j])
			}
			break
		}
	}
	// If leaderCommit > commitIndex,set commitIndex =
	// min(leaderCommit,index of last new entry)
	if m.Commit > l.committed {
		l.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, false, None, l.LastIndex())
}
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject {
		idx := m.Index
		if idx == None {
			return
		}
		if m.LogTerm != None {
			logTerm := m.LogTerm
			l := r.RaftLog
			sliceIdx := sort.Search(len(l.entries),
				func(i int) bool { return l.entries[i].Term > logTerm })
			if sliceIdx > 0 && l.entries[sliceIdx-1].Term == logTerm {
				idx = l.toEntryIndex(sliceIdx)
			}
		}
		// TODO 会不会出现leader的term 小于 follower的term
		r.Prs[m.From].Next = idx
		r.sendAppend(m.From)
		return
	}
	// leaderCommit iff follower append entries
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.leaderCommit()
		//TODO leaderTransferee
	}
	if m.From == r.leadTransferee && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(r.leadTransferee)
		log.Infof("++handleAppendEntriesResponse %x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, r.leadTransferee, r.leadTransferee)
	}
}

func (r *Raft) leaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	// sort.Sort(sort.Reverse(match))
	sort.Sort(match)
	// TODO 偶数试配点，TestLeaderAcknowledgeCommit2AB
	n := match[(len(match)-1)/2]
	// reflesh r.RaftLog.committed
	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		// do not commit log entries in previous terms
		if logTerm == r.Term {
			r.RaftLog.committed = n
			r.bcastAppend()
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// update commitID
	r.RaftLog.commitTo(m.Commit)

	if m.Term != None && m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) advance(rd Ready) {
	// TODO
	return
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, false, None, r.RaftLog.committed)
		return
	}
	r.becomeFollower(max(r.Term, m.Term), m.From)
	first := meta.Index + 1
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	// update RaftLog state
	r.RaftLog.FirstIndex = first
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
}

func (r *Raft) handleTransferLeaderNotLeader(m pb.Message) {
	if r.Lead == None {
		log.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
		return
	}
	m.To = r.Lead
	r.msgs = append(r.msgs, m)
}

func (r *Raft) sendTimeoutNow(to uint64) {
	msg := pb.Message{From: r.id, To: to, MsgType: pb.MessageType_MsgTimeoutNow}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func (r *Raft) handleTransferLeaderLeader(m pb.Message) {
	if r.leadTransferee != None {
		if r.leadTransferee == m.From {
			log.Debugf("++handleTransferLeaderLeader%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
				r.id, r.Term, r.leadTransferee, m.From)
			return
		}
		// new Transfer stop current leaderTransferee
		r.abortLeaderTransfer()
	}
	// check self transfer
	if m.From == r.id {
		log.Debugf("++handleTransferLeaderLeader %x is already leader. Ignored transferring leadership to self", r.id)
		return
	}
	// check non exist transfer
	if _, ok := r.Prs[m.From]; !ok {
		log.Infof("%x is not in peer not", m.From)
		return
	}

	log.Infof("++handleTransferLeaderLeader %x [term %d] starts to transfer leadership from leader %x to %x", r.id, r.Term, r.Lead, m.From)
	r.leadTransferee = m.From
	// TODO can i use resettimer()? no~
	// r.electionElapsed = 0
	r.resetTimer()
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(r.leadTransferee)
		log.Infof("++handleTransferLeaderLeader %x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, r.leadTransferee, r.leadTransferee)
	} else {
		r.sendAppend(r.leadTransferee)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{Next: 0}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	_, ok := r.Prs[id]
	if ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			r.leaderCommit()
		}
	}
	r.PendingConfIndex = None
}
