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
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64 // 第一个日志id
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot. //将日志恢复到刚刚提交并应用最新快照的状态
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	fIndex, err := storage.FirstIndex() //得到第一个日志条目的索引
	if err != nil {
		log.Println(err)
	}
	lIndex, err := storage.LastIndex() //得到最后一个日志条目的索引
	if err != nil {
		log.Println(err)
	}
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
}

// unstableEntries return all the unstable entries //返回所有未被持久化的日志条目
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.stabled-l.firstIndex+1 <= uint64(len(l.entries)) {
		return l.entries[l.stabled-l.firstIndex+1:] //已持久化的日志后面即为未被持久化的日志
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
	lastIndex := uint64(0)
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
	//先查看快照
	if !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
	}
	//再查看条目
	for _, e := range l.entries {
		if e.Index == i {
			return e.Term, nil
		}
	}
	//最后查看存储
	return l.storage.Term(i)
}

// 2A自定义函数，返回从给定索引开始所有后面的未持久化的条目
func (l *RaftLog) getUnstableEntryFromIndex(i uint64) []*pb.Entry {
	entries := make([]*pb.Entry, 0)
	for _, e := range l.entries {
		if e.Index >= i {
			entries = append(entries, &pb.Entry{
				EntryType: e.EntryType,
				Term:      e.Term,
				Index:     e.Index,
				Data:      e.Data,
			})
		}
	}
	return entries
}

// 2A自定义函数，从该索引开始删除后面的日志
func (l *RaftLog) DeleteFromIndex(index uint64) {
	for i, entry := range l.entries {
		if entry.GetIndex() == index {
			l.entries = l.entries[:i]
			break
		}
	}
	lastIndex := l.LastIndex()
	l.committed = min(l.committed, lastIndex)
	l.applied = min(l.applied, lastIndex)
	l.stabled = min(l.stabled, lastIndex)
}

// 2A自定义函数，在日志中添加新的日志条目
func (l *RaftLog) AppendEntries(pendingConfIndex *uint64, entries []*pb.Entry, term uint64) {
	for _, e := range entries {
		// 3A会修改这里？
		if e.EntryType == pb.EntryType_EntryConfChange {
			if *pendingConfIndex != 0 {
				continue
			} else {
				*pendingConfIndex = e.Index
			}
		}
		// 2A
		l.entries = append(l.entries, pb.Entry{
			EntryType: e.EntryType,
			Term:      term,
			Index:     l.LastIndex() + 1,
			Data:      e.Data,
		})
	}
}
