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

//raft two lead in same term
var ErrTwoLeadSameTerm = errors.New("raft two lead in same term")

// no mactch index between leader and follower
var ErrNoMatchIndex = errors.New("raft no mactch index between leader and follower")

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

	//total num who vote to me
	VoteNum int
	//total num who refuse me
	RefuseNum int
	//PeerNum
	Peers []uint64
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
	tickFunc        func()
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	raftLog.applied = c.Applied
	raftLog.committed = c.Applied
	raft := &Raft{
		id:               c.ID,
		votes:            make(map[uint64]bool, len(c.peers)),
		RaftLog:          raftLog,
		heartbeatElapsed: c.HeartbeatTick,
		heartbeatTimeout: c.HeartbeatTick,
		electionElapsed:  c.ElectionTick,
		electionTimeout:  c.ElectionTick,
	}
	raft.becomeFollower(0, None)
	state, confState, _ := raft.RaftLog.storage.InitialState()
	if len(c.peers) > 0 {
		raft.Peers = c.peers
	} else {
		raft.Peers = confState.Nodes
	}
	raft.Prs = make(map[uint64]*Progress, len(raft.Peers))
	for _, i := range raft.Peers {
		raft.Prs[i] = &Progress{}
	}
	raft.Term, raft.Vote, raft.RaftLog.committed = state.Term, state.Vote, state.Commit
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	next_index := r.Prs[to].Next
	log_term, _ := r.RaftLog.Term(next_index - 1)
	last_index := r.RaftLog.LastIndex()
	entries := make([]*pb.Entry, 0)
	if next_index <= last_index {
		next_ents, _ := r.RaftLog.Slice(next_index, last_index+1)
		for _, entry := range next_ents {
			temp := entry
			entries = append(entries, &temp)
		}
	}
	message := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Index:   next_index - 1,
		LogTerm: log_term,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, message)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// log.Infof("%v send heart beat msg to %v", r.id, to)
	var match uint64
	match = 0
	if len(r.Prs) != 0 {
		match = r.Prs[to].Match
	}
	message := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  min(r.RaftLog.committed, match),
	}
	r.msgs = append(r.msgs, message)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.tickFunc()
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.VoteNum = 0
	r.RefuseNum = 0
	r.Vote = None
	r.Term = term
	//open election tick func
	r.restElectionTime()
	r.tickFunc = r.electionTickFunc
	for _, p := range r.Peers {
		r.votes[p] = false
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate
}

//send request vote message
func (r *Raft) sendVoteMessage() {
	r.VoteNum = 0
	r.RefuseNum = 0
	lastIndex := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(lastIndex)
	for _, id := range r.Peers {
		if id != r.id {
			message := pb.Message{
				From:    r.id,
				To:      id,
				MsgType: pb.MessageType_MsgRequestVote,
				Term:    r.Term,
				Commit:  r.RaftLog.committed,
				Index:   lastIndex,
				LogTerm: logTerm,
			}
			r.msgs = append(r.msgs, message)
			r.votes[id] = false
		} else {
			r.votes[id] = true
			r.VoteNum++
			r.Vote = r.id
			if r.VoteNum > r.halfPeers() {
				r.becomeLeader()
			}
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	//open heart beat
	// log.Infof("%v become leader,old leader is %v", r.id, r.Lead)
	r.restHeartbeatTime()
	r.tickFunc = r.heartbeatTickFunc
	//close  election time
	r.State = StateLeader
	r.Lead = r.id
	//Delete Vote message
	r.VoteNum = 0
	r.RefuseNum = 0
	r.Vote = None
	for _, p := range r.Peers {
		r.votes[p] = false
	}
	//Send Noop message to all peers
	last_index := r.RaftLog.LastIndex()
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Index: last_index + 1, Term: r.Term})
	if len(r.Peers) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	for _, id := range r.Peers {
		if id == r.id {
			r.Prs[id] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[id] = &Progress{
				Match: r.RaftLog.applied,
				Next:  r.RaftLog.LastIndex(),
			}
			r.sendAppend(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.sendVoteMessage()
		case pb.MessageType_MsgPropose:
			// log.Infof("%v not leader", r.id)
			break
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, r.responseVote(m.From, m.Term, m.Index, m.LogTerm))
		case pb.MessageType_MsgRequestVoteResponse:
			break
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			break
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			break
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.sendVoteMessage()
		case pb.MessageType_MsgPropose:
			// log.Infof("%v not leader", r.id)
			break
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, r.responseVote(m.From, m.Term, m.Index, m.LogTerm))
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m.From, m.Term, m.Reject)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			break
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			break
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			break
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, r.responseVote(m.From, m.Term, m.Index, m.LogTerm))
		case pb.MessageType_MsgBeat:
			for _, id := range r.Peers {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		case pb.MessageType_MsgRequestVoteResponse:
			break
		case pb.MessageType_MsgAppend:
			if m.Term == r.Term && m.From != r.id {
				return ErrTwoLeadSameTerm
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgHeartbeat:
			if m.Term == r.Term && m.From != r.id {
				return ErrTwoLeadSameTerm
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	}
	return nil
}

func (r *Raft) handlePropose(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Index = lastIndex + uint64(i) + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	for _, id := range r.Peers {
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
			r.Prs[id].Next = r.Prs[id].Match + 1
			if len(r.Peers) == 1 {
				r.RaftLog.committed = r.Prs[id].Match
			}
			continue
		}
		r.sendAppend(id)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	message := pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgAppendResponse,
		Term:    r.Term,
		Index:   m.Index,
		Reject:  true,
	}
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
		message.Term = r.Term
	} else if r.Term > m.Term {
		r.msgs = append(r.msgs, message)
		return
	}
	pre_index_term, err := r.RaftLog.Term(m.Index)
	if err == nil && m.Index < r.RaftLog.committed && pre_index_term == m.LogTerm {
		message.Reject = false
		r.msgs = append(r.msgs, message)
		return
	}
	if err != nil || pre_index_term != m.LogTerm {
		for _, entry := range r.RaftLog.uncommitEnts() {
			temp := pb.Entry{Term: entry.Term, Index: entry.Index}
			message.Entries = append(message.Entries, &temp)
		}
		message.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, message)
		return
	}
	// log.Infof("[handleAppendEntries]%d receive Append entries m_log_trem %d,pre_index %d,pre_index_term %d", r.id, m.LogTerm, m.Index, pre_index_term)
	r.RaftLog.Append(m.Entries)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	if r.RaftLog.committed < r.RaftLog.applied {
		log.Panicf("[handleAppendEntries]%d committed index %d smaller than applied index %d", r.id, r.RaftLog.committed, r.RaftLog.applied)
	}
	message.Reject = false
	message.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, message)
	return
}
func (r *Raft) handleAppendEntriesResponse(m pb.Message) error {
	//old message
	if m.Term < r.Term {
		return nil
	}
	if m.Reject {
		//reponse entries will tell leader which match
		index := len(m.Entries) - 1
		for ; index >= 0; index-- {
			if term, err := r.RaftLog.Term(m.Entries[index].Index); err == nil && term == m.Entries[index].Term {
				break
			}
		}
		if index > -1 {
			r.Prs[m.From].Next = m.Entries[index].Index + 1
		} else {
			r.Prs[m.From].Next = m.Index + 1
		}
		r.sendAppend(m.From)
		return nil
	}
	//this message may be old
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
	r.Prs[m.From].Next = r.Prs[m.From].Match + 1
	var match_index []uint64
	for _, id := range r.Peers {
		match_index = append(match_index, r.Prs[id].Match)
	}
	sort.Slice(match_index, func(i, j int) bool {
		return match_index[i] < match_index[j]
	})
	half := (len(r.Peers)+1)/2 - 1
	if term, _ := r.RaftLog.Term(match_index[half]); term < r.Term {
		return nil
	}
	if match_index[half] > r.RaftLog.committed {
		r.RaftLog.committed = match_index[half]
		for _, peer := range r.Peers {
			if r.id != peer {
				r.sendAppend(peer)
			}
		}
	}
	if r.RaftLog.committed < r.RaftLog.applied {
		log.Panicf("[handleAppendEntries]%d committed index %d smaller than applied index %d", r.id, r.RaftLog.committed, r.RaftLog.applied)
	}
	return nil
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term >= r.Term {
		// log.Infof("%v receive %v heart beat msg", r.id, m.From)
		commit := min(r.RaftLog.LastIndex(), m.Commit)
		r.RaftLog.committed = max(r.RaftLog.committed, commit)
		r.becomeFollower(m.Term, m.From)
		return
	}
	if r.Term > m.Term {
		message := pb.Message{
			From:    r.id,
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, message)
		return
	}
	if r.RaftLog.committed < r.RaftLog.applied {
		log.Panicf("[handleAppendEntries]%d committed index %d smaller than applied index %d", r.id, r.RaftLog.committed, r.RaftLog.applied)
	}
}
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

//response to vote rpc
func (r *Raft) responseVote(id, term, index, log_term uint64) pb.Message {
	message := pb.Message{
		From:    r.id,
		To:      id,
		Term:    r.Term,
		Reject:  true,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	}
	if r.Term > term {
		return message
	}
	if term > r.Term {
		r.becomeFollower(term, None)
	}
	if id != r.Vote && r.Vote != None {
		return message
	}
	r_index := r.RaftLog.LastIndex()
	r_log_term, _ := r.RaftLog.Term(r_index)
	if log_term > r_log_term || (log_term == r_log_term && r_index <= index) {
		r.Vote = id
		message.Reject = false
		return message
	}
	return message
}

//hanle vote response
func (r *Raft) handleVoteResponse(id, term uint64, reject bool) {
	if term > r.Term {
		r.becomeFollower(term, None)
		return
	}
	if !reject && !r.votes[id] {
		r.votes[id] = true
		r.VoteNum++
		if r.VoteNum > r.halfPeers() {
			r.becomeLeader()
		}
		return
	}
	if reject && !r.votes[id] {
		r.RefuseNum++
		if r.RefuseNum > r.halfPeers() {
			r.becomeFollower(r.Term, None)
		}
	}
}

func (r *Raft) halfPeers() int {
	return len(r.Peers) / 2
}

func (r *Raft) restHeartbeatTime() {
	r.heartbeatTimeout = r.heartbeatElapsed
}

func (r *Raft) restElectionTime() {
	// r.electionTick.Reset(time.Duration(r.electionElapsed) * time.Millisecond)
	r.electionTimeout = rand.Intn(r.electionElapsed) + r.electionElapsed
}
func (r *Raft) electionTickFunc() {
	r.electionTimeout--
	if r.electionTimeout <= 0 {
		r.restElectionTime()
		r.becomeCandidate()
		r.sendVoteMessage()
	}
}
func (r *Raft) heartbeatTickFunc() {
	r.heartbeatTimeout--
	if r.heartbeatTimeout <= 0 {
		for _, id := range r.Peers {
			if id != r.id {
				r.sendHeartbeat(id)
			}
		}
		r.restHeartbeatTime()
	}
}
func (r *Raft) Advance(ready Ready) {
	r.RaftLog.Advance(ready)
}
func (r *Raft) GetHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
func (r *Raft) GetSoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}
