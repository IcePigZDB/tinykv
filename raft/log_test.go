package raft

import (
	"fmt"
	"sort"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

/*
	use for simple debug only.
*/
func TestRaftlog(t *testing.T) {
	storage := NewMemoryStorage()
	// config := newTestConfig(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	l := newLog(storage)
	test_entries := []pb.Entry{
		{Term: 1, Index: 1, Data: []byte("some data")},
		{Term: 1, Index: 2, Data: []byte("some data")},
		{Term: 1, Index: 3, Data: []byte("some data")},
		{Term: 4, Index: 4, Data: []byte("some data")},
		{Term: 4, Index: 4, Data: []byte("some data")},
		{Term: 5, Index: 5, Data: []byte("some data")},
		{Term: 5, Index: 5, Data: []byte("some data")},
		{Term: 6, Index: 6, Data: []byte("some data")},
		{Term: 6, Index: 6, Data: []byte("some data")},
		{Term: 6, Index: 6, Data: []byte("some data")},
	}
	l.entries = append(l.entries, test_entries...)
	fmt.Println(l.entries)
	fmt.Println(l.FirstIndex)
	fmt.Println(l.LastIndex())
	fmt.Println(l.Term(l.LastIndex()))
	// for _, entry := range test_entries {
	// l.entries = append(l.entries, entry)
	// }
	mIdx := 11
	logTerm, _ := l.Term(uint64(mIdx))
	idx := l.toEntryIndex(sort.Search(l.toSliceIndex(uint64(mIdx)+1),
		func(i int) bool { return l.entries[i].Term == logTerm }))
	fmt.Println(idx)
}

/*
TestCompactionSideEffects
TestHasNextEnts
TestNextEnts
TestUnstableEnts
TestCompaction
TestLogRestore
TestIsOutOfBounds
TestTerm
TestTermWithUnstableSnapshot
TestSlice
*/
