package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val, NotFound: false}, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	put := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	batch := storage.Modify{Data: put}

	// args2:array
	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	delete := storage.Delete{Key: req.Key, Cf: req.Cf}
	batch := storage.Modify{Data: delete}

	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	var kvs []*kvrpcpb.KvPair
	limit := req.Limit

	iter.Seek(req.StartKey)
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		val, _ := item.Value()
		pair := &kvrpcpb.KvPair{Key: item.Key(), Value: val}

		kvs = append(kvs, pair)
		limit--
		if limit == 0 {
			break
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return &kvrpcpb.GetResponse{NotFound: true}, err
	}
	// version TS
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)

	lock, err := mvccTxn.GetLock(req.Key)
	if err != nil {
		return &kvrpcpb.GetResponse{NotFound: true}, err
	}
	// lock futher txn with larger TS
	if lock != nil && req.Version >= lock.Ts {
		return &kvrpcpb.GetResponse{Error: &kvrpcpb.KeyError{Locked: lock.Info(req.Key)}}, err
	}

	value, err := mvccTxn.GetValue(req.Key)
	if err != nil {
		return &kvrpcpb.GetResponse{NotFound: true}, err
	}
	// process nil case (key not exist)
	if value == nil {
		return &kvrpcpb.GetResponse{NotFound: true}, err
	}
	return &kvrpcpb.GetResponse{Value: value}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	var keyErrors []*kvrpcpb.KeyError
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return resp, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	log.Infof("mutation.Ts:%d", req.StartVersion)
	for _, mutation := range req.Mutations {
		write, commitTs, err := mvccTxn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTs >= req.StartVersion {
			keyErrors = append(keyErrors,
				&kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTs,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock}})
			continue
		}

		lock, err := mvccTxn.GetLock(mutation.Key)
		// should it return directly? yes!
		if err != nil {
			return resp, err
		}
		if lock != nil {
			log.Infof("lock.TS:%d,mutation.Ts:%d", lock.Ts, req.StartVersion)
			keyErrors = append(keyErrors,
				&kvrpcpb.KeyError{Locked: lock.Info(mutation.Key)})
			continue
		}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			// log.Infof("______________value:%v", mutation.Value)
			mvccTxn.PutValue(mutation.Key, mutation.Value)
			mvccTxn.PutLock(mutation.Key, &mvcc.Lock{Primary: req.PrimaryLock, Ts: req.StartVersion, Ttl: req.LockTtl, Kind: mvcc.WriteKindFromProto(mutation.Op)})
			// case kvrpcpb.Op_Del:
			// case kvrpcpb.Op_Rollback:
		}
	}
	log.Infof("writes:%v", mvccTxn.Writes())
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	resp.Errors = keyErrors
	return resp, err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		_, commitTs, err := mvccTxn.MostRecentWrite(key)
		if err != nil {
			return resp, err
		}
		// check the TS
		if commitTs >= req.StartVersion {
			// resp.Error = &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
			// 	StartTs:    write.StartTS,
			// 	ConflictTs: commitTs,
			// 	Key:        key,
			// 	Primary:    key}}
			mvccTxn.DeleteLock(key)
			continue
		}
		lock, err := mvccTxn.GetLock(key)
		// should it return directly? yes
		if err != nil {
			return resp, err
		}
		// TestCommitMissingPrewrite4a
		if lock == nil {
			continue
		}
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Locked: lock.Info(key)}
			continue
		}
		mvccTxn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind})
		mvccTxn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	return resp, err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer scanner.Close()

	for count := uint32(0); count < req.Limit; count++ {
		key, value, err := scanner.Next()
		if err != nil {
			return resp, err
		}
		if key == nil && value == nil && err == nil {
			break
		}
		pair := &kvrpcpb.KvPair{Key: key, Value: value}
		resp.Pairs = append(resp.Pairs, pair)
	}
	return resp, err
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	// ts should use req.LockTS, req.CurrentTs will fail
	// because DeleteValue will use mvccTxn.Ts to delete value, value's TS = req.LockTs
	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)
	lock, err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	if lock == nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	}
	if lock == nil || mvcc.PhysicalTime(req.CurrentTs)-mvcc.PhysicalTime(lock.Ts) > lock.Ttl {
		write, commitTs, err := mvccTxn.MostRecentWrite(req.PrimaryKey)
		if err != nil {
			return resp, err
		}
		if write != nil {
			// continue if rollback already
			if write.Kind == mvcc.WriteKindRollback {
				resp.Action = kvrpcpb.Action_NoAction
				return resp, err
			}
			// it should not rollback a committed transaction
			if commitTs >= req.LockTs {
				resp.Action = kvrpcpb.Action_NoAction
				log.Infof("commitTs%d", commitTs)
				resp.CommitVersion = commitTs
				return resp, nil
			}
		}
		if lock != nil {
			resp.Action = kvrpcpb.Action_TTLExpireRollback
		}
		mvccTxn.DeleteValue(req.PrimaryKey)
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback})
		mvccTxn.DeleteLock(req.PrimaryKey)
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	return resp, err
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		// When scanning, some errors can be recorded for an individual key and should not cause the whole scan to stop.
		// For other commands, any single key causing an error should cause the whole operation to stop.
		write, commitTs, err := mvccTxn.MostRecentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil {
			// continue if rollback already
			if write.Kind == mvcc.WriteKindRollback {
				continue
			}
			// it should not rollback a committed transaction
			if commitTs >= req.StartVersion {
				resp.Error = &kvrpcpb.KeyError{Abort: fmt.Sprintf("key:%s abort rollback,with commitTs:%d and startTs:%d",
					key, commitTs, write.StartTS)}
				return resp, nil
			}
		}
		lock, err := mvccTxn.GetLock(key)
		// should it return directly? yes!
		if err != nil {
			return resp, err
		}
		// TODO should add this ?
		// if lock == nil {
		// 	continue
		// }
		// do not Delte ohter transaction (lock.Ts != req.StartVersion)'s lock and value
		// and should write a WriteKindRollback
		if lock != nil && lock.Ts != req.StartVersion {
			mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
			continue
		}
		mvccTxn.DeleteValue(key)
		mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
		mvccTxn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	return resp, err
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		value, err := iter.Item().Value()
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == req.StartVersion {
			key := iter.Item().Key()
			keys = append(keys, key)
		}
	}

	if len(keys) == 0 {
		return resp, nil
	}
	log.Infof("keys:%b", keys)
	if req.CommitVersion == 0 {
		respC, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		resp.Error = respC.Error
		return resp, err
	} else {
		respR, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		resp.Error = respR.Error
		return resp, err

	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
