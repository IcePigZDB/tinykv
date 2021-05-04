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
	lo, _ := storage.FirstIndex()             // 1
	hi, _ := storage.LastIndex()              // 0
	entries, err := storage.Entries(lo, hi+1) //(1,1)
	if err != nil {
		log.Panic(err)
	}
	// log.Infof("+++++newLog lo:%d,hi:%d,", lo, hi)
	return &RaftLog{
		storage:    storage,
		entries:    entries,
		committed:  lo - 1,
		applied:    lo - 1,
		stabled:    hi, // 因为如果storage不为空的话，从storage拿出来最大的就是Stable的
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
			log.Infof("+++maybeCompact turncat first:%d, l.entries.firt:%d, l.FirstIndex:%d,len(l.entries):%d",
				first, l.entries[len(l.entries)-1].Index, l.FirstIndex, len(l.entries))
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
	if len(l.entries) > 0 {
		// do not include [l.applid-l.FirstIndex+1,l.committed-l.FirstIndex+1)
		return l.entries[l.applied-l.FirstIndex+1 : l.committed-l.FirstIndex+1]
	}
	return nil
}

// func (l *RaftLog) getEnts(lo int, hi int) (ents []pb.Entry) {
// return l.entries[l.toSliceIndex(uint64(lo)) : l.toSliceIndex(uint64(hi))+1]
// }

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var idx uint64
	if !IsEmptySnap(l.pendingSnapshot) {
		idx = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		// TODO 不加max好像也没问题，应该是没测试到
		return max(l.entries[len(l.entries)-1].Index, idx)
	}
	i, _ := l.storage.LastIndex()
	return max(idx, i)
}

// Term return the term of the entry in the given indexG
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// return are the same

	// if len(l.entries) > 0 {
	// 	log.Infof("first:%d", l.entries[len(l.entries)-1].Index)
	// }
	// first, _ := l.storage.FirstIndex()
	// log.Infof("i:%d, FirstIndex:%d,storage FirstIndex:%d,len(l.entries):%d", i, l.FirstIndex, first, len(l.entries))

	if len(l.entries) > 0 && i >= l.FirstIndex {
		return l.entries[i-l.FirstIndex].Term, nil
	}

	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		// luckly want snapshot point's term
		if i == l.pendingSnapshot.Metadata.Index {
			term = l.pendingSnapshot.Metadata.Term
			err = nil
			// ErrCompacted small than ErrUnavailable
		} else if i < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}

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
