package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"

	// 2B自添加
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// 功能1：处理 RawNode 产生的 Ready
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.peer.RaftGroup.HasReady() {
		return
	}
	// 获得需要尚未保存到磁盘的entry，尚未发送的msg和尚未applied的entry
	rd := d.RaftGroup.Ready()
	// 保存entry到磁盘中
	apply, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		return
	}
	// 2C添加, 如果 Ready 中存在 snapshot，则应用它
	if apply != nil {
		if reflect.DeepEqual(apply.PrevRegion, d.Region()) {
			d.peerStorage.region = apply.Region
			// 加锁，防止冲突
			d.ctx.storeMeta.Lock()
			// 更新region信息
			d.ctx.storeMeta.regions[d.regionId] = d.Region()
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			d.ctx.storeMeta.Unlock()
		}
	}
	// 发送消息
	if len(rd.Messages) != 0 {
		d.Send(d.ctx.trans, rd.Messages)
	}
	// apply消息
	if len(rd.CommittedEntries) > 0 {
		for _, e := range rd.CommittedEntries {
			kvWB := new(engine_util.WriteBatch)
			d.applyAndResponseEntry(kvWB, e)
			if d.stopped {
				return // 可能被 destroy 了
			}
			// 更新
			d.peerStorage.applyState.AppliedIndex = e.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
		}
	}
	d.RaftGroup.Advance(rd)
}

// 2B自定义函数，应用并回复消息
func (d *peerMsgHandler) applyAndResponseEntry(kvWB *engine_util.WriteBatch, entry pb.Entry) {
	// 3B添加
	if entry.EntryType == pb.EntryType_EntryConfChange {
		d.applyConfChangeEntry(kvWB, entry)
		return
	}
	msg := raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(entry.GetData())
	if err != nil {
		panic("poorpool: unmarshal error")
	}
	// 2B只考虑非admin消息, 2C开始添加admin消息
	if msg.AdminRequest != nil {
		d.applyAdminRequest(kvWB, entry, msg)
	} else if len(msg.Requests) > 0 {
		d.applyRequest(kvWB, entry, msg)
	}
}

// 2B自定义函数，应用非admin消息
func (d *peerMsgHandler) applyRequest(kvWB *engine_util.WriteBatch, entry pb.Entry, msg raft_cmdpb.RaftCmdRequest) {
	req := msg.Requests[0]
	err := d.checkRequestIfKeyInRegion(req)
	if err != nil {
		d.handleProposal(&entry, func(p *proposal) {
			p.cb.Done(ErrResp(err))
		})
		return
	}
	// 写操作对修改进行保存
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
	case raft_cmdpb.CmdType_Put:
		putRequest := req.Put
		log.Infof("****put key:%v ,value:%v", string(putRequest.Key), string(putRequest.Value))
		kvWB.SetCF(putRequest.Cf, putRequest.Key, putRequest.Value)
	case raft_cmdpb.CmdType_Delete:
		deleteRequest := req.Delete
		kvWB.DeleteCF(deleteRequest.Cf, deleteRequest.Key)
	case raft_cmdpb.CmdType_Snap:
	}
	// callback
	d.handleProposal(&entry, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			// 向前推进，使之读取到最新写入的数据
			d.peerStorage.applyState.AppliedIndex = entry.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			// 读取需要将读到的值保存在回复消息中
			getRequest := req.Get
			value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, getRequest.Cf, getRequest.Key)
			resp.Responses = []*raft_cmdpb.Response{
				{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: value}},
			}
			p.cb.Done(resp) // 将消息发送给指令发送者
		case raft_cmdpb.CmdType_Put:
			resp.Responses = []*raft_cmdpb.Response{
				{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}},
			}
			p.cb.Done(resp) // 将消息发送给指令发送者
		case raft_cmdpb.CmdType_Delete:
			resp.Responses = []*raft_cmdpb.Response{
				{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}},
			}
			p.cb.Done(resp) // 将消息发送给指令发送者
		case raft_cmdpb.CmdType_Snap:
			if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
				return
			}
			// 复制region，防止指针修改
			cloneRegion := &metapb.Region{}
			util.CloneMsg(d.Region(), cloneRegion)
			resp.Responses = []*raft_cmdpb.Response{
				{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: cloneRegion}},
			}
			//  Snap需要将返回一个 txn
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			p.cb.Done(resp) // 将消息发送给指令发送者
		}
	})
}

// 2B自定义函数，检查Get/Put/Delete消息是否key在region范围内
func (d *peerMsgHandler) checkRequestIfKeyInRegion(req *raft_cmdpb.Request) error {
	var key []byte
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	default:
		return nil
	}
	return util.CheckKeyInRegion(key, d.Region())
}

// 2B自定义函数，处理proposal
func (d *peerMsgHandler) handleProposal(entry *eraftpb.Entry, handle func(*proposal)) {
	for len(d.proposals) > 0 {
		proposal := d.proposals[0]
		//1.过期的entry，已经被其他执行了
		if entry.Term < proposal.term {
			return
		}
		//2.更高任期的entry, 本条proposal作废
		if entry.Term > proposal.term {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}
		//3.entry的index小，不回复
		if entry.Term == proposal.term && entry.Index < proposal.index {
			return
		}
		//4.entry的index大，回复发送至错误的领导人
		if entry.Term == proposal.term && entry.Index > proposal.index {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}
		//5.entry的index等于proposal.index
		if entry.Index == proposal.index && entry.Term == proposal.term {
			handle(proposal)
			d.proposals = d.proposals[1:]
		}
	}
}

// 2C自定义函数，应用admin消息
func (d *peerMsgHandler) applyAdminRequest(kvWB *engine_util.WriteBatch, entry pb.Entry, msg raft_cmdpb.RaftCmdRequest) {
	req := msg.AdminRequest
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
	// 3B实现
	case raft_cmdpb.AdminCmdType_Split:
		// 检查region
		log.Infof("%d start split region %d: splitKey:%v, startKey:%v, endKey:%v", d.PeerId(), d.regionId, req.Split.SplitKey, d.Region().StartKey, d.Region().EndKey)
		// if msg.Header.RegionId != d.regionId {
		// 	regionNotFound := &util.ErrRegionNotFound{RegionId: msg.Header.RegionId}
		// 	d.handleProposal(&entry, func(p *proposal) {
		// 		p.cb.Done(ErrResp(regionNotFound))
		// 	})
		// 	return
		// }
		err := util.CheckRegionEpoch(&msg, d.Region(), true)
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			siblingRegion := d.findSiblingRegion()
			if siblingRegion != nil {
				errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
			}
			d.handleProposal(&entry, func(p *proposal) {
				p.cb.Done(ErrResp(errEpochNotMatching))
			})
			return
		}
		err = util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
		if err != nil {
			d.handleProposal(&entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return
		}
		// 如果两者长度不相同，直接拒绝本次 split request
		if len(d.Region().Peers) != len(req.Split.NewPeerIds) {
			return
		}
		peers := make([]*metapb.Peer, 0)
		for i, peer := range d.Region().Peers {
			peers = append(peers, &metapb.Peer{Id: req.Split.NewPeerIds[i], StoreId: peer.StoreId})
		}
		// 新的region
		newRegion := &metapb.Region{
			Id:       req.Split.NewRegionId,
			StartKey: req.Split.SplitKey,
			EndKey:   d.Region().EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: d.Region().RegionEpoch.ConfVer,
				Version: d.Region().RegionEpoch.Version + 1,
			},
			Peers: peers,
		}
		// 更新旧region的信息
		d.Region().EndKey = req.Split.SplitKey
		d.Region().RegionEpoch.Version++
		// 修改storeMeta信息
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		d.ctx.storeMeta.regions[req.Split.NewRegionId] = newRegion
		d.ctx.storeMeta.Unlock()
		// 持久化region信息
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
		// 创建新的 peer并注册进 router，同时发送 MsgTypeStart 启动 peer
		newpeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		d.ctx.router.register(newpeer)
		d.ctx.router.send(req.Split.NewRegionId, message.Msg{
			RegionID: req.Split.NewRegionId,
			Type:     message.MsgTypeStart,
		})
		// callback回复
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_Split,
				Split: &raft_cmdpb.SplitResponse{
					Regions: []*metapb.Region{d.Region(), newRegion},
				},
			},
		}
		d.handleProposal(&entry, func(p *proposal) {
			p.cb.Done(resp)
		})
		// 解决split出现的no region报错，刷新缓存
		d.notifyHeartbeatScheduler(d.Region(), d.peer)
		d.notifyHeartbeatScheduler(newRegion, newpeer)
	case raft_cmdpb.AdminCmdType_TransferLeader:
	// 2C实现
	case raft_cmdpb.AdminCmdType_CompactLog:
		compact := req.CompactLog
		if compact.CompactIndex >= d.peerStorage.truncatedIndex() {
			// 更新truncated State
			d.peerStorage.applyState.TruncatedState.Index = compact.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = compact.CompactTerm
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			d.ScheduleCompactLog(compact.CompactIndex)
		}

	}
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

// 3B自定义函数
func (d *peerMsgHandler) applyConfChangeEntry(kvWB *engine_util.WriteBatch, entry pb.Entry) {
	//log.Infof("%d processConfChange", d.PeerId())
	cc := eraftpb.ConfChange{}
	cc.Unmarshal(entry.Data)
	resp := new(raft_cmdpb.RaftCmdResponse)
	resp.Header = new(raft_cmdpb.RaftResponseHeader)
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{},
	}
	msg := raft_cmdpb.RaftCmdRequest{}
	msg.Unmarshal(cc.Context)
	// 读取原有的 region
	region := d.Region()
	// 检查region的epoch
	err := util.CheckRegionEpoch(&msg, region, true)
	if err != nil {
		d.handleProposal(&entry, func(p *proposal) {
			p.cb.Done(ErrResp(err))
		})
		return
	}
	// 对于添加/删除节点分别进行处理
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		// 判断节点是否已经存在
		if !isPeerExists(region, cc.NodeId) {
			// 将peer添加到region中
			peer := &metapb.Peer{
				Id:      cc.NodeId,
				StoreId: msg.AdminRequest.ChangePeer.Peer.StoreId,
			}
			region.Peers = append(region.Peers, peer)
			// 版本号递增
			region.RegionEpoch.ConfVer++
			// 持久化修改后的 Region，写到 kvDB 里面
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			// 在缓存中插入peer
			d.insertPeerCache(peer)
			// 这是必要的吗？
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[d.regionId] = region
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
			d.ctx.storeMeta.Unlock()
			// 调用 Raft 的 addNode() 方法
			d.RaftGroup.ApplyConfChange(cc)
			log.Infof("[confChange]region %d add peer %d storeId %d", d.regionId, msg.AdminRequest.ChangePeer.Peer.Id, msg.AdminRequest.ChangePeer.Peer.StoreId)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		// 如果删除的是自己，直接销毁并返回
		if d.PeerId() == cc.NodeId {
			log.Infof("[confChange]region %d delete peer %d storeId %d", d.regionId, cc.NodeId, d.storeID())
			kvWB.DeleteMeta(meta.ApplyStateKey(d.regionId)) // 删除前写入
			d.destroyPeer()
			return
		}
		// 如果删除的是其他节点
		if isPeerExists(region, cc.NodeId) {
			// 从region中删除节点
			for i, p := range region.Peers {
				if p.Id == cc.NodeId {
					region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
				}
			}
			// 版本号递增
			region.RegionEpoch.ConfVer++
			// 持久化修改后的 Region，写到 kvDB 里面
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			// 从缓存中删除peer
			d.removePeerCache(cc.NodeId)
			// 这是必要的吗？
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[d.regionId] = region
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
			d.ctx.storeMeta.Unlock()
			// 调用 Raft 的 removeNode() 方法
			d.RaftGroup.ApplyConfChange(cc)
			log.Infof("[confChange]region %d delete peer %d storeId %d", d.regionId, cc.NodeId, d.storeID())
		}
	}
	d.handleProposal(&entry, func(p *proposal) { p.cb.Done(resp) })
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

// 3B自定义函数，判断节点在region中是否存在
func isPeerExists(region *metapb.Region, id uint64) bool {
	for _, p := range region.Peers {
		if p.Id == id {
			return true
		}
	}
	return false
}

// 功能2：处理消息
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage: // 在 Raft peers之间传输的消息
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd: // 包装来自客户端的请求
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick: // 驱动 Raft 的消息
		d.onTick()
	case message.MsgTypeSplitRegion: // 分割region的消息（3B）
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize: // 更新region大致的大小
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap: // 清理已经安装完的 snapshot
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart: // 启动peer的定时器的消息（3B）
		d.startTicker()
	}
}

// 预处理
func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	// 检查存储是否有正确的peer来处理请求
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.检查任期是否过时
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// 接收客户端请求
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// 2B只考虑非admin消息,2C开始添加admin消息
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeRequest(msg, cb)
	}
}

// 2B自定义函数，propose msg 的普通 request 的第一个，别的不 propose
func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.GetRequests()[0]
	err := d.checkRequestIfKeyInRegion(req)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	data, err := msg.Marshal() // 将msg转变为byte[]
	if err != nil {
		panic("marshal error")
	}
	// 保存proposal
	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})
	// 调用Propose函数处理消息（MsgType_MsgPropose）
	d.RaftGroup.Propose(data)
}

// 2C自定义函数，propose msg 的admin request 的第一个，别的不 propose
func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	adminReq := msg.AdminRequest
	switch msg.AdminRequest.CmdType {
	// 3B实现
	case raft_cmdpb.AdminCmdType_ChangePeer:
		context, _ := msg.Marshal()
		//包装消息
		cc := eraftpb.ConfChange{
			ChangeType: adminReq.ChangePeer.ChangeType,
			NodeId:     adminReq.ChangePeer.Peer.Id,
			Context:    context,
		}
		// 解决remove node出现request timeout的问题
		// 只剩两个节点且被移除的节点刚好为leader，此时需要拒绝该propose并转移领导权给另一个节点
		if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && len(d.Region().Peers) == 2 && cc.NodeId == d.LeaderId() {
			for _, peer := range d.Region().Peers {
				if peer.Id != cc.NodeId {
					d.RaftGroup.TransferLeader(peer.Id)
					// 将消息传给客户端
					resp := new(raft_cmdpb.RaftCmdResponse)
					resp.Header = new(raft_cmdpb.RaftResponseHeader)
					resp.AdminResponse = &raft_cmdpb.AdminResponse{
						CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
						ChangePeer: &raft_cmdpb.ChangePeerResponse{},
					}
					cb.Done(resp)
					return
				}
			}
		}
		// 保存proposal
		p := &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}
		d.proposals = append(d.proposals, p)
		d.RaftGroup.ProposeConfChange(cc)
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
	// 3B实现
	case raft_cmdpb.AdminCmdType_Split:
		// 检查是否在region里
		err := util.CheckKeyInRegion(adminReq.Split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		data, err := msg.Marshal()
		if err != nil {
			panic("marshal error")
		}
		// 保存proposal
		p := &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}
		d.proposals = append(d.proposals, p)
		d.RaftGroup.Propose(data)
	// 3B实现
	case raft_cmdpb.AdminCmdType_TransferLeader:
		resp := new(raft_cmdpb.RaftCmdResponse)
		resp.Header = new(raft_cmdpb.RaftResponseHeader)
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		}
		cb.Done(resp)
		d.RaftGroup.TransferLeader(adminReq.TransferLeader.Peer.Id)
	// 2C实现
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic("marshal error")
		}
		d.RaftGroup.Propose(data)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

// 安排压缩日志
func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		// 消息的接收方应该不存在，所以接收方应该删除自己
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
// 查看raft消息是否有效
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
/// 检查消息是否发送到正确的peer
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		// 该消息过时且不在当前region
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

// 处理过时的消息
func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

// 处理垃圾消息
func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
// 如果' msg '不包含快照，或者它包含的快照不与任何其他快照或region冲突，则返回' None '
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// 销毁peer
func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

// 找到兄弟region
func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

// 定时调用判断现在的firstIdx小于appliedIdx就进行compactLog
func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		log.Infof("1 here!")
		panic(err)
	}

	// Create a compact log request and notify directly.
	// 创建一个压缩的日志请求并直接通知
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

// 定时会调用判断自己是否需要分割
func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	// 为了避免频繁的扫描，只在所有之前的任务都完成的情况下添加新的扫描任务
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

// 分割region的方法
func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	// 判断分割范围是否有效
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	// 向 SchedulerTask发送一个请求分割的消息
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

// 验证是否可以分割region
func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

// 创建管理请求
func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

// 创建压缩日志请求
func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
