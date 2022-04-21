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
	votes     map[uint64]bool
	voteNum   int
	refuseNum int

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

	// 3A添加，领导转换的过去的 tick 数，防止一直领导转换
	//transferLeaderElapsed int
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
	//得到raft节点的信息以及raft集群的成员信息
	hardState, confState, _ := c.Storage.InitialState()
	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage), // 初始化 RaftLog
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	//判断config中的peers是否为空，如果为空需要使用confState进行赋值
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	//使用config中的peers对raft中的prs进行初始化
	// for _, id := range c.peers {
	// 	if id == r.id {
	// 		r.Prs[id] = &Progress{Match: r.RaftLog.LastIndex(), Next: r.RaftLog.LastIndex() + 1}
	// 	} else {
	// 		r.Prs[id] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
	// 	}
	// }
	for _, i := range c.peers {
		r.Prs[i] = &Progress{}
	}
	r.RaftLog.committed = hardState.Commit
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	r.becomeFollower(0, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the //发送一个带有新条目和当前提交索引的附加RPC到其他节点
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	nextIndex := r.Prs[to].Next                   //得到to要发送的索引
	preTerm, err := r.RaftLog.Term(nextIndex - 1) //通过该索引得到发送节点对应的任期
	// 2C添加
	if err != nil {
		if err == ErrCompacted { //说明to落后太多，只能发快照
			r.sendSnapshot(to)
			return true
		}
		panic(err)
	}
	lastIndex := r.RaftLog.LastIndex()
	entries := make([]*pb.Entry, 0)
	if nextIndex <= lastIndex {
		next_ents, _ := r.RaftLog.Slice(nextIndex, lastIndex+1)
		for _, entry := range next_ents {
			// 一定要用temp防止使用entry的地址而 导致append的数据不变！
			temp := entry
			entries = append(entries, &temp)
		}
	}
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

// 2C自定义函数，发送快照给其他节点
func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Snapshot: &snapshot,
	}
	log.Infof("[sendSnapshot]%d send snapshot to %d", msg.From, msg.To)
	r.msgs = append(r.msgs, msg)
	// to的下一个日志条目索引要修改为快照中索引的下一个（由于to落后太多）
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.//发送心跳信号
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	//log.Infof("%v send heart beat msg to %v", r.id, to)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// 每调用一次，时间++ (electionElapsed/heartbeatElapsed 递增)
	if r.State == StateLeader { //必须是领导者才会有心跳检测
		r.heartbeatElapsed++
	} else {
		r.electionElapsed++
	}
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
		r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
			From:    r.id,
		})
	}
	// 3A添加，领导转换计时
	// if r.leadTransferee != None {
	// 	r.transferLeaderElapsed++
	// 	if r.transferLeaderElapsed >= 2*r.electionElapsed { // 领导转换超时，中止
	// 		r.leadTransferee = None
	// 		r.transferLeaderElapsed = 0
	// 	}
	// }
}

// becomeFollower transform this peer's state to Follower // 状态转换为Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	//log.Infof("[becomeFollower]%d become follower", r.id)
	r.State = StateFollower
	r.Lead = lead
	r.leadTransferee = None
	if term > r.Term {
		r.Vote = None
		r.Term = term
	}
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate // 状态转换为Candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++      // 自增任期号，开始选举
	r.Vote = r.id //给自己投票
	r.votes = make(map[uint64]bool, 0)
	r.votes[r.id] = true //给自己投票
	r.voteNum = 1
	r.refuseNum = 0
	//log.Infof("[becomeCandidate]%d become candidate,term is %v", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader // 状态转换为Leader，转换为leader后有一些操作
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Infof("[becomeLeader]%d become leader", r.id)
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	//r.leadTransferee = None
	r.Vote = None
	r.voteNum = 0
	r.refuseNum = 0
	r.votes = make(map[uint64]bool, 0)
	//添加并发送空的日志条目
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
	// Your Code Here (2A)
	// 首先判断消息的类型，再执行对应的操作
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup()
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
			// 2C添加
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
			// 3A添加
			// 由于可能有网络分区，所以转发给leader的方案不太好，所以采取直接进行选举的方案
			r.handleHup()
		case pb.MessageType_MsgTimeoutNow:
			// 3A添加
			if r.Prs[r.id] != nil {
				r.handleHup()
			}
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup()
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend: //有领导者申明其领导者身份
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgSnapshot:
			// 2C添加
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat: //有领导者申明其领导者身份
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
			// 3A添加
			// 由于可能有网络分区，所以转发给leader的方案不太好，所以采取直接进行选举的方案
			r.handleHup()
		case pb.MessageType_MsgTimeoutNow:
			// 3A添加
			if r.Prs[r.id] != nil {
				r.handleHup()
			}
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			r.handleBeat(m)
		case pb.MessageType_MsgPropose: //该信号从客户端发出
			r.handlePropose(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m) //假如领导换届
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m) //假如领导换届
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgTransferLeader:
			// 3A添加
			r.handleTransferLeader(m)
		case pb.MessageType_MsgTimeoutNow:
		}
	}
	return nil
}

//---------Follower Handle func---------------
// 2A自定义函数，处理选举超时
func (r *Raft) handleHup() {
	// 3b中避免两个节点时尚为同步的新节点与另一个节点竞选而出现的超时
	if len(r.Prs) == 0 {
		return
	}
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
				Commit:  r.RaftLog.committed,
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
	msg := pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgAppendResponse,
		Term:    r.Term,
		Index:   m.Index,
		Reject:  true,
	}
	if m.Term >= r.Term {
		//log.Infof("++%v 【handleAppendEntries】 become follower", r.id)
		r.becomeFollower(m.Term, m.From)
		msg.Term = r.Term
	}
	// 发送者的任期更小，拒绝
	if m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		return
	}
	mIndex := m.Index                  //m前一个索引
	mTerm := m.LogTerm                 //m前一个任期
	rTerm, _ := r.RaftLog.Term(mIndex) //r最后一个任期
	if mIndex < r.RaftLog.committed && rTerm == m.LogTerm {
		msg.Reject = false
		msg.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, msg)
		return
	}
	// r和领导者前面的日志不匹配
	if rTerm != mTerm {
		for _, entry := range r.RaftLog.uncommitEnts() {
			temp := pb.Entry{Term: entry.Term, Index: entry.Index}
			msg.Entries = append(msg.Entries, &temp)
		}
		msg.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, msg)
		return
	}
	// 同步领导者的日志到r
	r.RaftLog.Append(m.Entries)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.electionElapsed = 0
	msg.Reject = false
	msg.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, msg)
}

// 2A自定义函数，处理投票请求
func (r *Raft) handleRequestVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}
	// 请求者的任期小
	if m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		return
	}
	// 请求者的任期大
	if m.Term > r.Term {
		//log.Infof("++%v 【handleRequestVote】 become follower", r.id)
		r.becomeFollower(m.Term, None)
		msg.Term = r.Term // r的任期发生改变
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
		//log.Infof("%v vote to %v", r.id, m.From)
		msg.Reject = false
		r.electionElapsed = 0
		r.msgs = append(r.msgs, msg)
		return
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request // 处理接收到的心跳
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}
	if m.Term >= r.Term {
		//log.Infof("++%v 【handleHeartbeat】 become follower", r.id)
		r.becomeFollower(m.Term, m.From)
		msg.Reject = false
		msg.Term = r.Term // r的任期发生改变
		commit := min(r.RaftLog.LastIndex(), m.Commit)
		r.RaftLog.committed = max(r.RaftLog.committed, commit)
		msg.Index = r.RaftLog.committed
	}
	r.msgs = append(r.msgs, msg)
}

//---------Candidate Handle Func---------------
// 2A自定义函数，处理投票请求的响应
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	//r.electionElapsed = 0
	if m.Term < r.Term {
		return
	}
	if m.Term > r.Term {
		//log.Infof("++%v 【handleRequestVoteResponse】 become follower", r.id)
		r.becomeFollower(m.Term, None)
		return
	}
	if !m.Reject && !r.votes[m.From] {
		r.votes[m.From] = true
		r.voteNum++
		if r.voteNum > len(r.Prs)/2 {
			r.becomeLeader()
		}
		return
	}
	if m.Reject && !r.votes[m.From] {
		r.refuseNum++
		if r.refuseNum > len(r.Prs)/2 {
			//log.Infof("%v 选举失败成为follower", r.id)
			r.becomeFollower(r.Term, None)
		}
	}
}

//---------Leader Handle Func-------------------
// 2A自定义函数，处理心跳发送请求
func (r *Raft) handleBeat(m pb.Message) {
	for p := range r.Prs {
		if p != r.id {
			r.sendHeartbeat(p)
		}
	}
}

// 2A自定义函数，处理添加日志请求
func (r *Raft) handlePropose(m pb.Message) {
	// 3A添加，领导人换届不再接收客户端的propose请求
	if r.leadTransferee != None {
		return
	}
	// 在日志中添加新的日志条目
	for i, entry := range m.Entries {
		entry.Index = r.RaftLog.LastIndex() + uint64(i) + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
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
	if m.Term > r.Term {
		//log.Infof("++%v 【handleAppendResponse】 become follower", r.id)
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Reject {
		// 找到满足的next，再重新尝试请求
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
		return
	}
	r.electionElapsed = 0
	// 成功，更新相应跟随者的nextIndex和matchIndex，并检查commit更新
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
	r.Prs[m.From].Next = r.Prs[m.From].Match + 1
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
	r.checkLeaderCommit()
	// 3A添加，如果领导换届目标已经具备成为领导的条件，立刻让它超时发起选举，自己放弃领导地位
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
		//log.Infof("%v 领导换届成为follower", r.id)
		r.becomeFollower(r.Term, r.leadTransferee)
	}
}

// 2A自定义函数，领导者收到 appendresponse 或 removenode 后检查 commit 有没有变化，有就广播
func (r *Raft) checkLeaderCommit() {
	var match_index []uint64
	for id := range r.Prs {
		match_index = append(match_index, r.Prs[id].Match)
	}
	sort.Slice(match_index, func(i, j int) bool {
		return match_index[i] < match_index[j]
	})
	half := (len(r.Prs)+1)/2 - 1
	if term, _ := r.RaftLog.Term(match_index[half]); term < r.Term {
		return
	}
	if match_index[half] > r.RaftLog.committed {
		r.RaftLog.committed = match_index[half]
		for peer := range r.Prs {
			if r.id != peer {
				r.sendAppend(peer)
			}
		}
	}
}

// 3A自定义函数，处理领导换届请求
func (r *Raft) handleTransferLeader(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      m.From,
		From:    r.id,
		Index:   r.RaftLog.LastIndex(),
		Term:    r.Term,
	}
	if m.From == r.id || r.leadTransferee == m.From || r.Prs[m.From] == nil {
		return
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match >= r.RaftLog.LastIndex() {
		r.msgs = append(r.msgs, msg)
	} else {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Reject:  false,
	}
	snapShot := m.Snapshot
	meta := snapShot.Metadata
	// r本身含有快照的部分
	if meta.Index <= r.RaftLog.committed {
		msg.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, msg)
		return
	}
	// 否则，m比r的日志更多，m同时还是leader
	//log.Infof("++%v 【handleSnapshot】 become follower", r.id)
	r.becomeFollower(max(meta.Term, m.Term), m.From)
	// 根据快照修改r的信息
	r.RaftLog.firstIndex = meta.Index + 1
	r.RaftLog.committed = meta.Index
	r.RaftLog.applied = meta.Index
	r.RaftLog.stabled = meta.Index
	r.RaftLog.pendingSnapshot = snapShot
	r.Prs = make(map[uint64]*Progress)
	for _, node := range meta.ConfState.Nodes {
		r.Prs[node] = &Progress{}
	}
	// 将处在快照中的日志进行压缩
	r.RaftLog.CompactEntires()
	msg.Index = r.RaftLog.committed
	r.msgs = append(r.msgs, msg)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  1,
	}
	// 发送心跳给添加的节点
	r.sendHeartbeat(id)
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	// 删除自己，在apply时如果删除自己不会调用该函数
	// 删除其他节点
	delete(r.Prs, id)
	// 检查是否需要更新commit
	if len(r.Prs) != 0 {
		r.checkLeaderCommit()
	}
}
