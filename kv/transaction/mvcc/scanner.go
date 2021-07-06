package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter       engine_util.DBIterator
	ts         uint64
	reader     storage.StorageReader
	deleteKyes [][]byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(startKey)
	ts := txn.StartTS

	return &Scanner{iter: iter, ts: ts, reader: txn.Reader}

}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for ; scan.iter.Valid(); scan.iter.Next() {
		item := scan.iter.Item()
		ts := decodeTimestamp(item.Key())
		if ts <= scan.ts {
			key := item.KeyCopy(nil)
			key = DecodeUserKey(key)
			if scan.isDeleted(key) {
				continue
			}
			writeBytes, err := item.ValueCopy(nil)
			if err != nil {
				return nil, nil, err
			}
			write, err := ParseWrite(writeBytes)
			if err != nil {
				return nil, nil, err
			}
			if write.Kind == WriteKindDelete || write.Kind == WriteKindRollback {
				scan.deleteKyes = append(scan.deleteKyes, key)
				continue
			}
			value, err := scan.reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
			if err != nil {
				scan.iter.Next()
				return nil, nil, err
			}
			scan.iter.Next()
			return key, value, nil
		}
	}
	return nil, nil, nil
}

func (scan *Scanner) isDeleted(key []byte) bool {
	for _, dKey := range scan.deleteKyes {
		if bytes.Equal(dKey, key) {
			return true
		}
	}
	return false
}
