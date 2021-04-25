package raft

import (
	"fmt"
	"sort"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func TestRaftlog(t *testing.T) {
	storage := NewMemoryStorage()
	// config := newTestConfig(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	l := newLog(storage)
	test_entries := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 2},
		{Index: 5, Term: 2},
		{Index: 6, Term: 2},
		{Index: 7, Term: 3},
		{Index: 8, Term: 3},
		{Index: 9, Term: 3},
		{Index: 10, Term: 3},
		{Index: 11, Term: 3}}
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

func TestSortSearch(t *testing.T) {
	test_entries := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 4},
		{Index: 5, Term: 4},
		{Index: 6, Term: 5},
		{Index: 7, Term: 5},
		{Index: 8, Term: 6},
		{Index: 9, Term: 6},
		{Index: 10, Term: 6},
	}
	logTerm := uint64(3)
	sliceIdx := sort.Search(len(test_entries),
		func(i int) bool { return test_entries[i].Term > logTerm })
	fmt.Println(sliceIdx)
}
