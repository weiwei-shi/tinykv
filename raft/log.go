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
	"github.com/pingcap-incubator/tinykv/log"
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
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	FirstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hi, herr := storage.LastIndex()
	lo, lerr := storage.FirstIndex()
	if herr != nil {
		panic(herr)
	}
	if lerr != nil {
		panic(lerr)
	}
	entries, err := storage.Entries(lo, hi+1)
	if err != nil {
		entries = make([]pb.Entry, 0)
	}
	return &RaftLog{
		storage:    storage,
		committed:  lo - 1,
		applied:    lo - 1,
		stabled:    hi,
		entries:    entries,
		FirstIndex: lo,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.FirstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.FirstIndex+1 : l.committed-l.FirstIndex+1]
	}
	return nil
}
func (l *RaftLog) uncommitEnts() (ents []pb.Entry) {
	if len(l.entries) > 0 {
		return l.entries[l.committed-l.FirstIndex+1:]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		index, _ := l.storage.LastIndex()
		return index
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	last_index := l.LastIndex()
	if last_index == 0 {
		return 0, nil
	} else if i > last_index {
		return 0, ErrUnavailable
	}
	s_last_index, _ := l.storage.LastIndex()
	if i <= s_last_index {
		return l.storage.Term(i)
	}
	return l.entries[i-l.entries[0].Index].Term, nil
}
func (l *RaftLog) Advance(ready Ready) {
	if len(ready.Entries) > 0 {
		l.stabled = ready.Entries[len(ready.Entries)-1].Index
	}
	if len(ready.CommittedEntries) > 0 {
		l.applied = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
		if l.applied > l.committed {
			log.Panicf("[Advance]applied index %d bigger than commit index %d", l.applied, l.committed)
		}
	}
}
func (l *RaftLog) UpdateEntries(stableIndex uint64) {
	if len(l.entries) != 0 && stableIndex >= l.entries[0].Index {
		l.entries = l.entries[l.stabled-l.entries[0].Index+1:]
	}
	l.stabled = max(l.stabled, stableIndex)
}

func (l *RaftLog) Slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo > hi {
		log.Panicf("[slice]Invaild slice [%d:%d]", lo, hi)
	}
	if lo > l.LastIndex() || hi > l.LastIndex()+1 {
		log.Panicf("[slice]lo %d bigger than lastIndex %d or hi %d bigger than lastIndex puls one %d", lo, l.LastIndex(), hi, l.LastIndex()+1)
	}
	if hi < l.FirstIndex {
		return nil, ErrUnavailable
	}
	if lo < l.FirstIndex {
		lo = l.FirstIndex
	}
	if len(l.entries) > 0 {
		return l.entries[l.toSliceIndex(lo):l.toSliceIndex(hi)], nil
	}
	return nil, nil
}

func (l *RaftLog) Append(entries []*pb.Entry) {
	if len(entries) == 0 {
		return
	}
	for i, ent := range entries {
		if ent.Index < l.FirstIndex {
			continue
		}
		if ent.Index <= l.LastIndex() {
			term, err := l.Term(ent.Index)
			if err != nil {
				panic(err)
			}
			if term != ent.Term {
				idx := l.toSliceIndex(ent.Index)
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

func (l *RaftLog) toSliceIndex(index uint64) int {
	idx := int(index - l.FirstIndex)
	if idx < 0 {
		panic("toSliceIndex: index < 0")
	}
	return idx
}
