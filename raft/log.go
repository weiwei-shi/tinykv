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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage // 存储引擎 raft/storage.go

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64 // 已提交的日志的id

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64 // 已应用到状态机的日志id

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64 // 已持久化的日志id

	// all entries that have not yet compact.
	entries []pb.Entry // 还没有压缩的日志

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot // 接收到的未写入存储的快照

	// Your Data Here (2A).
	firstIndex uint64 // 第一个日志id
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot. //将日志恢复到刚刚提交并应用最新快照的状态
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	fIndex, _ := storage.FirstIndex()                 //得到第一个日志条目的索引
	lIndex, _ := storage.LastIndex()                  //得到最后一个日志条目的索引
	entries, err := storage.Entries(fIndex, lIndex+1) //得到所有的日志条目
	if err != nil {
		log.Println(err)
	}
	return &RaftLog{
		storage:    storage,
		committed:  fIndex - 1,
		applied:    fIndex - 1,
		stabled:    lIndex,
		entries:    entries,
		firstIndex: fIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory //防止日志条目无限增长而进行的日志压缩
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex() // 没进入 snapshot 的第一个元素
	// 删除进入 snapshot 的元素
	if l.firstIndex < first {
		l.firstIndex = first
		l.CompactEntires()
	}
}

func (l *RaftLog) CompactEntires() {
	if len(l.entries) == 0 {
		return
	}
	if len(l.entries) < int(l.firstIndex-l.entries[0].Index) {
		l.entries = make([]pb.Entry, 0)
		return
	}
	l.entries = l.entries[int(l.firstIndex-l.entries[0].Index):]
}

// unstableEntries return all the unstable entries //返回所有未被持久化的日志条目
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	//已持久化的日志后面即为未被持久化的日志
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries //返回所有已提交但未被应用的日志条目
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.firstIndex+1 : l.committed-l.firstIndex+1]
	}
	return nil
}

// LastIndex return the last index of the log entries //返回日志条目的最后一个索引
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//最大值要么在 snapshot 里，要么：有 entries 就肯定在 entries，否则 storage
	var lastIndex uint64
	// 2C添加
	if !IsEmptySnap(l.pendingSnapshot) {
		lastIndex = l.pendingSnapshot.Metadata.Index //快照里面存储的索引
	}
	//有 entries
	if len(l.entries) > 0 {
		return max(lastIndex, l.entries[len(l.entries)-1].Index)
	} else { //没有 entries
		index, _ := l.storage.LastIndex()
		return max(lastIndex, index)
	}
}

// Term return the term of the entry in the given index //返回给定索引条目的任期
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, nil
	}
	// 2C添加，查看快照
	if !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		if i < l.pendingSnapshot.Metadata.Index {
			return 0, ErrCompacted
		}
	}
	// 在存储里面找
	last, _ := l.storage.LastIndex()
	if i <= last {
		return l.storage.Term(i)
	}
	// 日志不在存储里
	return l.entries[i-l.firstIndex].Term, nil
}

// 2A自定义函数，得到未提交的日志
func (l *RaftLog) uncommitEnts() (ents []pb.Entry) {
	if len(l.entries) > 0 {
		return l.entries[l.committed-l.firstIndex+1:]
	}
	return nil
}

// 2A自定义函数，同步日志
func (l *RaftLog) Append(entries []*pb.Entry) {
	if len(entries) == 0 {
		return
	}
	for i, ent := range entries {
		if ent.Index < l.firstIndex {
			continue
		}
		if ent.Index <= l.LastIndex() {
			term, err := l.Term(ent.Index)
			if err != nil {
				panic(err)
			}
			if term != ent.Term {
				idx := int(ent.Index - l.firstIndex)
				l.entries[idx] = *ent
				l.entries = l.entries[:idx+1]
				l.stabled = min(l.stabled, ent.Index-1)
			}
		} else {
			n := len(entries)
			for j := i; j < n; j++ {
				l.entries = append(l.entries, *entries[j])
			}
		}
	}
}

// 2A自定义函数,获得从lo到hi对应的entry
func (l *RaftLog) Slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo > hi {
		log.Panicf("[slice]Invaild slice [%d:%d]", lo, hi)
	}
	if lo > l.LastIndex() || hi > l.LastIndex()+1 {
		log.Panicf("[slice]lo %d bigger than lastIndex %d or hi %d bigger than lastIndex puls one %d", lo, l.LastIndex(), hi, l.LastIndex()+1)
	}
	if hi < l.firstIndex {
		return nil, ErrUnavailable
	}
	if lo < l.firstIndex {
		lo = l.firstIndex
	}
	if len(l.entries) > 0 {
		return l.entries[int(lo-l.firstIndex):int(hi-l.firstIndex)], nil
	}
	return nil, nil
}
