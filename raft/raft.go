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
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.//没有领导者时使用
const None uint64 = 0

// StateType represents the role of a node in a cluster.//代表节点的状态类型（领导者/跟随者/候选者）
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

// ErrProposalDropped is returned when the proposal is ignored by some cases,//提案被忽略时返回
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.开始raft时的一些参数
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. //peers包含raft集群中所有节点(包括自己)的id。
	// It should only be set when starting a new raft cluster.//它应该只在启动一个新的raft集群时设置。
	// Restarting raft from previous configuration will panic if peers is set.//如果设置了peers，重新启动之前配置的raft会引起恐慌。
	// peer is private and only used for testing right now.//Peer是私有的，目前仅用于测试。
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between elections.//是节点的个数。
	// That is, if a follower does not receive any message from the leader of current term before ElectionTick has elapsed,
	// it will become candidate and start an election.//如果一个跟随者在ElectionTick结束之前没有收到来自当前任期领导者的任何消息，它将成为候选人并开始选举。
	// ElectionTick must be greater than HeartbeatTick.//ElectionTick必须大于HeartbeatTick
	// We suggest ElectionTick = 10 * HeartbeatTick to avoid unnecessary leader switching.//建议ElectionTick = 10 * HeartbeatTick，以避免不必要的leader切换
	ElectionTick int
	// HeartbeatTick is the number of Node.//是节点的个数
	// Tick invocations that must pass between heartbeats.
	// That is, a leader sends heartbeat messages to maintain its leadership every HeartbeatTick ticks.//每当HeartbeatTick滴答作响时，一个领导者就会发送心跳消息来维持自己的领导地位
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be//存储条目和状态
	// stored in storage. raft reads the persisted entries and states out of Storage when it needs.
	// raft reads out the previous state and configuration out of storage when restarting.//Raft在重新启动时从存储中读取之前的状态和配置
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting raft.//是最后一个应用的索引。应该只在重新启动筏时设置。
	// raft will not return entries to the application smaller or equal to Applied.//raft将不会向应用程序返回小于或等于Applied的条目
	// If Applied is unset when restarting, raft might return previous applied entries.
	// This is a very application dependent configuration.
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

// Progress represents a follower’s progress in the view of the leader.//是跟随者的进度
// Leader maintains progresses of all followers, and sends entries to the follower based on its progress.//领导者根据跟随者的进度向跟随者发送条目。
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers 记录每个节点的复制进度
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records 投票记录
	votes map[uint64]bool

	// msgs need to send 被发送的消息
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send 心跳间隔
	heartbeatTimeout int
	// baseline of election interval 选举间隔的基准
	electionTimeout int
	// 2A自定义的真正的选举超时时间，在 [electionTimeout, 2 * electionTimeout) 之间
	realElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed. // 心跳计时
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower. // 选举计时
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer) // 领导换届的目标id
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	rand.Seed(time.Now().UnixNano())
	//得到raft节点的信息以及raft集群的成员信息
	hardState, confState, _ := c.Storage.InitialState()
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage), // 初始化 RaftLog
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower, // 初始状态均为Follower
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		leadTransferee:   None,
	}
	//初始化真正的选举超时时间
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	//判断config中的peers是否为空，如果为空需要使用confState进行赋值
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	//使用config中的peers对raft中的prs进行初始化
	for _, id := range c.peers {
		if id == r.id {
			r.Prs[id] = &Progress{Match: r.RaftLog.LastIndex(), Next: r.RaftLog.LastIndex() + 1}
		} else {
			r.Prs[id] = &Progress{Match: 0, Next: 1}
		}
	}
	r.RaftLog.committed = hardState.Commit
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the //发送一个带有新条目和当前提交索引的附加RPC到其他节点
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	nextIndex := r.Prs[to].Next                   //得到to要发送的索引
	preTerm, err := r.RaftLog.Term(nextIndex - 1) //通过该索引得到发送节点对应的任期
	if err != nil {
		if err == ErrCompacted { //说明to落后太多，只能发快照
			return r.sendSnapshot(to)
		}
	}
	entries := r.RaftLog.getUnstableEntryFromIndex(nextIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preTerm,       //要发送条目的前一个条目term
		Index:   nextIndex - 1, //要发送条目的前一个条目index
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// 2A自定义函数，发送快照给其他节点(可能是2B修改的？)
func (r *Raft) sendSnapshot(to uint64) bool {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return false
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	// to的下一个日志条目索引要修改为快照中的索引（由于to落后太多）
	r.Prs[to].Next = snapshot.Metadata.Index
	return true
}

// 2A自定义函数，发送append响应
func (r *Raft) sendAppendResponse(to uint64, reject bool, term uint64, index uint64) bool {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    term,
		Reject:  reject,
		Index:   index,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.//发送心跳信号
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// 每调用一次，时间++ (electionElapsed/heartbeatElapsed 递增)
	if r.State == StateLeader { //必须是领导者才会有心跳检测
		r.heartbeatElapsed++
	}
	r.electionElapsed++
	// 根据节点不同的状态：当electionElapsed/heartbeatElapsed达到相应的阈值后(选举超时/心跳超时)，发送本地消息，由step()进行处理
	// 定期心跳
	if r.State == StateLeader && r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			To:      r.id,
			From:    r.id,
		})
	}
	// 跟随者太久没收到有效消息，自己发起选举
	if r.State != StateLeader && r.electionElapsed >= r.realElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
			From:    r.id,
		})
	}
}

// becomeFollower transform this peer's state to Follower // 状态转换为Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = 0                      // 假如从候选者/跟随者变为新的跟随者
	r.votes = make(map[uint64]bool) // 假如从候选者变为跟随者
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0   // 假如从跟随者变为新的跟随者
	r.leadTransferee = None // 假如从领导者变为跟随者
}

// becomeCandidate transform this peer's state to candidate // 状态转换为Candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++ // 自增任期号，开始选举
	r.Lead = 0
	r.Vote = r.id //给自己投票
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true //给自己投票
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader // 状态转换为Leader，转换为leader后有一些操作
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.Vote = 0
	r.votes = make(map[uint64]bool)
	//添加空的日志条目
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Index: r.RaftLog.LastIndex() + 1,
		Term:  r.Term,
	})
	//假如集群只有自己，则满足大多数的要求，自己所有的日志条目都是已提交的
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	for id := range r.Prs {
		if id == r.id {
			r.Prs[r.id] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[id] = &Progress{
				Match: r.RaftLog.applied,
				Next:  r.RaftLog.LastIndex(), // 从添加的空日志条目开始发送
			}
			r.sendAppend(id) //向其他节点发送空日志条目
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 任何时候收到消息的任期比自身大，无法判断对方就是领导者
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	// 首先判断消息的类型，再执行对应的操作
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None { //将消息转给领导者
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
			r.handleHup(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			if m.Term == r.Term { //有领导者申明其领导者身份
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			if m.Term == r.Term { //有领导者申明其领导者身份
				r.becomeFollower(m.Term, m.From)
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None { //将消息转给领导者
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
			r.handleHup(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			r.handleBeat(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m) //该信号从客户端发出
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m) //假如领导换届
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m) //假如领导换届
		case pb.MessageType_MsgHeartbeatResponse:
			r.sendAppend(m.From) //得到心跳响应后发送新的日志给其他节点
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		case pb.MessageType_MsgTimeoutNow:
		}
	}
	return nil
}

//---------Follower Handle func---------------
// 2A自定义函数，处理选举超时
func (r *Raft) handleHup(m pb.Message) {
	r.becomeCandidate()
	// 如果集群只有自己，则直接成为领导者
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	// 向其他节点发送投票请求
	for id := range r.Prs {
		if id != r.id {
			index := r.RaftLog.LastIndex()
			logTerm, _ := r.RaftLog.Term(index)
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      id,
				From:    r.id,
				Term:    r.Term,
				LogTerm: logTerm, //要发送条目的前一个条目term
				Index:   index,   //要发送条目的前一个条目index
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request // 处理（复制）接收到的Log，需验证有效性
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	// 发送者的任期更小，拒绝
	if m.Term > 0 && m.Term < r.Term {
		r.sendAppendResponse(m.From, true, r.Term, m.Index)
		return
	}
	r.Lead = m.From
	// r缺少日志，拒绝，需要重发前面的日志
	if m.Index > r.RaftLog.LastIndex() {
		r.sendAppendResponse(m.From, true, r.Term, r.RaftLog.LastIndex())
		return
	}
	mIndex := m.Index                  //m前一个索引
	mTerm := m.LogTerm                 //m前一个任期
	rTerm, _ := r.RaftLog.Term(mIndex) //r最后一个任期
	// r和领导者前面的日志不匹配
	if mIndex >= r.RaftLog.firstIndex && rTerm != mTerm {
		r.sendAppendResponse(m.From, true, r.Term, r.RaftLog.LastIndex())
		return
	}
	// 同步领导者的日志到r
	for _, entry := range m.Entries {
		if entry.Index < r.RaftLog.firstIndex {
			continue
		} else if entry.Index <= r.RaftLog.LastIndex() {
			Term, _ := r.RaftLog.Term(entry.Index)
			if Term != entry.Term {
				r.RaftLog.DeleteFromIndex(entry.Index)
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			}
		} else {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}
	if m.Commit > r.RaftLog.committed {
		//?  r.committed=min(msg.committed,r.LastIndex())
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, false, r.Term, r.RaftLog.LastIndex())
}

// 2A自定义函数，处理投票请求
func (r *Raft) handleRequestVote(m pb.Message) {
	r.electionElapsed = 0
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}
	// 请求者的任期小
	if m.Term > 0 && m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		return
	}
	// 已经投票给别人
	if r.Vote != None && r.Vote != m.From {
		r.msgs = append(r.msgs, msg)
		return
	}
	// 判断投票条件是否符合
	rIndex := r.RaftLog.LastIndex()
	rTerm, _ := r.RaftLog.Term(rIndex)
	if m.LogTerm > rTerm || (m.LogTerm == rTerm && m.Index >= rIndex) {
		r.Vote = m.From
		msg.Reject = false
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request // 处理接收到的心跳
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	if m.Term >= r.Term { //已在step中成为跟随者
		r.Lead = m.From
	}
	// 假如 m.Term >= r.Term ，发送应答给心跳发送者使其发送新日志
	// 假如 m.Term < r.Term ，发送应答给心跳发送者使其立马成为跟随者而不会发送新日志（不会进入该函数）
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	})
}

//---------Candidate Handle Func---------------
// 2A自定义函数，处理投票请求的响应
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.electionElapsed = 0
	// 有节点的任期大于自身，此时r已经在step中成为跟随者，不会进入该函数
	// 有节点的任期小于自身，r发送投票请求的时候对方已成为跟随者，任期将与r相同，所以小于的情况不会再出现
	if m.Term > 0 && m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	trueCount, falseCount := 0, 0
	for _, v := range r.votes {
		if v {
			trueCount++
		} else {
			falseCount++
		}
	}
	if trueCount > len(r.Prs)/2 {
		r.becomeLeader()
	}
	if falseCount > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

//---------Leader Handle Func-------------------
// 2A自定义函数，处理心跳发送请求
func (r *Raft) handleBeat(m pb.Message) {
	r.heartbeatElapsed = 0
	for p := range r.Prs {
		if p != r.id {
			r.sendHeartbeat(p)
		}
	}
}

// 2A自定义函数，处理添加日志请求
func (r *Raft) handlePropose(m pb.Message) {
	// 领导人换届不再接收客户端的propose请求
	if r.leadTransferee != None {
		return
	}
	// 3A中可能会修改
	r.RaftLog.AppendEntries(&r.PendingConfIndex, m.Entries, r.Term)
	// 向其他节点发送复制请求,对自己则修改集群中自己的进度
	for id := range r.Prs {
		if id == r.id {
			r.Prs[r.id] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
			if len(r.Prs) == 1 {
				r.RaftLog.committed = r.RaftLog.LastIndex()
			}
			continue
		}
		r.sendAppend(id)
	}
}

// 2A自定义函数，处理日志复制请求的响应
func (r *Raft) handleAppendResponse(m pb.Message) {
	r.electionElapsed = 0
	// 有节点的任期大于自身，此时r已经在step中成为跟随者，不会进入该函数
	// 有节点的任期小于自身，r发送复制请求的时候对方已成为跟随者，任期将与r相同，所以小于的情况不会再出现
	if m.Term > 0 && m.Term < r.Term {
		return
	}
	if m.Reject {
		// 减小Next重新尝试请求
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}
	// 成功，更新相应跟随者的nextIndex和matchIndex，并检查commit更新
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.checkLeaderCommit()
	}
	// 如果领导换届目标已经具备成为领导的条件，立刻让它超时发起选举，自己放弃领导地位
	if m.From == r.leadTransferee {
		if r.Prs[m.From].Match >= r.RaftLog.LastIndex() {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
			})
		}
		r.leadTransferee = None
		r.becomeFollower(r.Term, r.leadTransferee)
	}
}

// 2A自定义函数，领导者收到 appendresponse 或 removenode 后检查 commit 有没有变化，有就广播
func (r *Raft) checkLeaderCommit() {
	if r.State != StateLeader {
		return
	}
	changed := false
	for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		count := 0
		// 计算该日志复制到服务器上的个数(最后比较时要加上r本身)
		for id, prs := range r.Prs {
			if id != r.id && prs.Match >= i {
				count++
			}
		}
		logTerm, _ := r.RaftLog.Term(i)
		if logTerm == r.Term && count+1 > len(r.Prs)/2 {
			r.RaftLog.committed = i
			changed = true
		}
	}
	// 如果commit发生变化就进行广播
	if changed {
		for id := range r.Prs {
			if id != r.id {
				r.sendAppend(id)
			}
		}
	}
}

// 2A自定义函数，处理领导换届请求
func (r *Raft) handleTransferLeader(m pb.Message) {
	_, p := r.Prs[m.From]
	if !p {
		return
	}
	if m.From == r.id || r.leadTransferee == m.From {
		return
	}
	r.leadTransferee = m.From
	r.sendAppend(r.leadTransferee)
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
