package message

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type MsgType int64

const (
	// just a placeholder
	MsgTypeNull MsgType = 0
	// message to start the ticker of peer
	// 启动peer的定时器的消息
	MsgTypeStart MsgType = 1
	// message of base tick to drive the ticker
	MsgTypeTick MsgType = 2
	// message wraps a raft message that should be forwarded to Raft module
	// the raft message is from peer on other store
	/// message包装了一个raft消息，该消息应该转发给raft模块，该raft消息来自其他存储的peer
	MsgTypeRaftMessage MsgType = 3
	// message wraps a raft command that maybe a read/write request or admin request
	// the raft command should be proposed to Raft module
	// 消息包装了一个raft命令，可能是一个读/写请求或管理请求，raft命令应该被提出到raft模块
	MsgTypeRaftCmd MsgType = 4
	// message to trigger split region 触发分割region的消息
	// it first asks Scheduler for allocating new split region's ids, then schedules a
	// MsyTypeRaftCmd with split admin command
	// 它首先请求调度程序分配新的分割区域的id，然后用分割管理命令调度一个MsyTypeRaftCmd
	MsgTypeSplitRegion MsgType = 5
	// message to update region approximate size 更新region大致的大小
	// it is sent by split checker
	MsgTypeRegionApproximateSize MsgType = 6
	// message to trigger gc generated snapshots
	// 触发gc生成快照的消息
	MsgTypeGcSnap MsgType = 7

	// message wraps a raft message to the peer not existing on the Store.
	// It is due to region split or add peer conf change
	MsgTypeStoreRaftMessage MsgType = 101
	// message of store base tick to drive the store ticker, including store heartbeat
	MsgTypeStoreTick MsgType = 106
	// message to start the ticker of store
	MsgTypeStoreStart MsgType = 107
)

type Msg struct {
	Type     MsgType
	RegionID uint64
	Data     interface{}
}

func NewMsg(tp MsgType, data interface{}) Msg {
	return Msg{Type: tp, Data: data}
}

func NewPeerMsg(tp MsgType, regionID uint64, data interface{}) Msg {
	return Msg{Type: tp, RegionID: regionID, Data: data}
}

type MsgGCSnap struct {
	Snaps []snap.SnapKeyWithSending
}

type MsgRaftCmd struct {
	Request  *raft_cmdpb.RaftCmdRequest
	Callback *Callback
}

type MsgSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
	SplitKey    []byte
	Callback    *Callback
}
