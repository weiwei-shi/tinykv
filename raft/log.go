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
	"reflect"

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
		storage:   storage,
		committed: 0,
		applied:   0,
		stabled:   hi,
		entries:   entries,
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
	// Your Code Here (2A).
	if l.stabled == l.LastIndex() {
		return make([]pb.Entry, 0)
	}
	if entries, err := l.Slice(l.stabled+1, l.LastIndex()+1); err != nil {
		log.Errorf(err.Error())
		return make([]pb.Entry, 0)
	} else if entries == nil {
		return make([]pb.Entry, 0)
	} else {
		return entries
	}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied == l.LastIndex() {
		return make([]pb.Entry, 0)
	}
	if entries, err := l.Slice(l.applied+1, l.committed+1); err != nil {
		log.Errorf(err.Error())
		return make([]pb.Entry, 0)
	} else if entries == nil {
		return make([]pb.Entry, 0)
	} else {
		return entries
	}
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
func (l *RaftLog) SetLog(entry pb.Entry) bool {
	if l.LastIndex() < entry.Index {
		l.entries = append(l.entries, entry)
		return false
	}
	temp := l.entries[entry.Index-l.entries[0].Index]
	flag := !reflect.DeepEqual(temp, entry)
	if flag {
		l.entries = l.entries[:entry.Index-l.entries[0].Index]
		l.entries = append(l.entries, entry)
	} else {
		l.entries[entry.Index-l.entries[0].Index] = entry
	}
	return flag
}
func (l *RaftLog) Advance(ready Ready) {
	var appliedIndex uint64
	if len(ready.CommittedEntries) > 0 {
		appliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
	}
	if appliedIndex > l.committed || appliedIndex < l.applied {
		log.Panicf("[advance] new applied index %d is not in range [%d, %d]",
			appliedIndex, l.applied, l.committed)
	}
	l.applied = appliedIndex
	var stableIndex uint64
	if len(ready.Entries) > 0 {
		stableIndex = ready.Entries[len(ready.Entries)-1].Index
	}
	l.UpdateEntries(stableIndex)
}
func (l *RaftLog) UpdateEntries(stableIndex uint64) {
	l.stabled = stableIndex
	if len(l.entries) != 0 && l.stabled >= l.entries[0].Index {
		l.entries = l.entries[l.stabled-l.entries[0].Index+1:]
	}
	l.stabled = max(l.stabled, stableIndex)
}
func (l *RaftLog) Slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo > hi {
		log.Panicf("Invaild slice [%d:%d]", lo, hi)
	}
	lastIndex := l.LastIndex()
	if lastIndex < lo || hi > lastIndex+1 {
		log.Panicf("lo or hi can`t be bigger than lastIndex.lo=%d.hi=%d,lastIndex=%d", lo, hi, lastIndex)
	}
	if lo == hi {
		return nil, nil
	}
	sLastIndex := l.stabled
	if sLastIndex >= hi {
		return l.storage.Entries(lo, hi)
	}
	if sLastIndex < lo {
		if len(l.entries) != 0 {
			return l.entries[lo-l.entries[0].Index : hi-l.entries[0].Index], nil
		}
		return nil, nil
	}
	entries, err := l.storage.Entries(lo, sLastIndex+1)
	if err != nil {
		return nil, err
	}
	if len(l.entries) != 0 {
		if sLastIndex >= l.entries[0].Index {
			entries = append(entries, l.entries[sLastIndex-l.entries[0].Index+1:hi-l.entries[0].Index]...)
		} else {
			entries = append(entries, l.entries[:hi-l.entries[0].Index]...)
		}
	}
	return entries, nil
}

func (l *RaftLog) Append(entries []*pb.Entry) {
	if len(entries) == 0 {
		return
	}
	if entries[0].Index <= l.stabled {
		preStable, err := l.storage.Entries(entries[0].Index, l.stabled+1)
		if err != nil {
			log.Panicf(err.Error())
		}
		var i int
		for i = 0; i < len(preStable) && i < len(entries); i++ {
			if preStable[i].Term != entries[i].Term {
				l.stabled = preStable[i].Index - 1
				break
			}
		}
		entries = entries[i:]
	}
	if len(entries) == 0 {
		return
	}
	if len(l.entries) != 0 && l.entries[len(l.entries)-1].Index >= entries[0].Index {
		l.entries = l.entries[:entries[0].Index-l.entries[0].Index]
	}
	for _, entry := range entries {
		l.entries = append(l.entries, *entry)
	}
}
