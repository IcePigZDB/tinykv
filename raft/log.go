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
	// FirstIndex = entries[0].Index, offset in etcd
	FirstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lo, _ := storage.FirstIndex()             // 1
	hi, _ := storage.LastIndex()              // 0
	entries, err := storage.Entries(lo, hi+1) //(1,1)
	if err != nil {
		log.Panic(err)
	}
	return &RaftLog{
		storage:    storage,
		entries:    entries,
		committed:  lo - 1,
		applied:    lo - 1,
		stabled:    hi, // the high index from storage
		FirstIndex: lo, // 1
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex()
	if first > l.FirstIndex {
		if len(l.entries) > 0 {
			// log.Infof("+++maybeCompact turncat first:%d, l.entries.firt:%d, l.FirstIndex:%d,len(l.entries):%d",
			// first, l.entries[len(l.entries)-1].Index, l.FirstIndex, len(l.entries))
			l.entries = l.entries[l.toSliceIndex(first):]
		}
		l.FirstIndex = first
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		// +1 begin from stable idx
		return l.entries[l.stabled-l.FirstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// etcd: l.applied may larger than l.FirstIndex
	if len(l.entries) > 0 {
		// do not include [l.applid-l.FirstIndex+1,l.committed-l.FirstIndex+1)
		return l.entries[l.applied-l.FirstIndex+1 : l.committed-l.FirstIndex+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// has entries
	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].Index
	}
	// no entries, has snapshot
	if !IsEmptySnap(l.pendingSnapshot) {
		return l.pendingSnapshot.Metadata.Index
	}
	// no entries, no snapshot
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// func (l *RaftLog) LastIndex() uint64 {
// 	// Your Code Here (2A).
// 	var index uint64
// 	if !IsEmptySnap(l.pendingSnapshot) {
// 		index = l.pendingSnapshot.Metadata.Index
// 	}
// 	if len(l.entries) > 0 {
// 		return max(l.entries[len(l.entries)-1].Index, index)
// 	}
// 	i, _ := l.storage.LastIndex()
// 	return max(i, index)
// }

// Term return the term of the entry in the given indexG
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// index larger than last entries
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	// check in snapshot
	if i < l.FirstIndex {
		if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		if !IsEmptySnap(l.pendingSnapshot) && i < l.pendingSnapshot.Metadata.Index {
			return 0, ErrCompacted
		}
	}
	// in entries
	if len(l.entries) > 0 && i >= l.FirstIndex {
		return l.entries[i-l.FirstIndex].Term, nil
	}
	// not in entries
	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

// func (l *RaftLog) Term(i uint64) (uint64, error) {
// 	// Your Code Here (2A).
// 	if len(l.entries) > 0 && i >= l.FirstIndex {
// 		return l.entries[i-l.FirstIndex].Term, nil
// 	}
// 	term, err := l.storage.Term(i)
// 	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
// 		if i == l.pendingSnapshot.Metadata.Index {
// 			term = l.pendingSnapshot.Metadata.Term
// 			err = nil
// 		} else if i < l.pendingSnapshot.Metadata.Index {
// 			err = ErrCompacted
// 		}
// 	}
// 	return term, err
// }

func (l *RaftLog) toSliceIndex(i uint64) int {
	idx := int(i - l.FirstIndex)
	if idx < 0 {
		log.Panic("toSliceIndex: index<0")
	}
	return idx
}

func (l *RaftLog) toEntryIndex(i int) uint64 {
	return uint64(i) + l.FirstIndex
}

func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
	}
}

func (l *RaftLog) GetEntries() []pb.Entry {
	return l.entries
}
