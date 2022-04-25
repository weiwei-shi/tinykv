package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
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
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	// 获取 Lock
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}
	// 如果 Lock 的 startTs 小于当前的 startTs，说明存在你之前存在尚未 commit 的请求，中断操作，返回 LockInfo
	if lock != nil && lock.Ts < req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl, // 生存时间
			}}
		return resp, nil
	}
	// 获取对应版本的值
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return resp, err
	}
	// 如果value不存在
	if value == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	// 如果请求的操作为空
	if req.Mutations == nil {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	var keyError []*kvrpcpb.KeyError
	for _, m := range req.Mutations {
		// 检查所有 key 的最新 Write，如果存在，且其 commitTs 大于当前事务的 startTs，说明存在 write conflict，终止操作
		write, ts, err := txn.MostRecentWrite(m.Key)
		if err != nil {
			return resp, err
		}
		if write != nil && ts > req.StartVersion {
			keyError = append(keyError, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        m.Key,
					Primary:    req.PrimaryLock,
				}})
			continue
		}
		// 检查所有 key 是否有 Lock，如果存在 Lock，说明当前 key 被其他事务使用中，终止操作
		lock, err := txn.GetLock(m.Key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts != req.StartVersion {
			keyError = append(keyError, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         m.Key,
					LockTtl:     lock.Ttl, // 生存时间
				}})
			continue
		}
		// 此时可以执行预写操作，根据mutation的类型做出相应操作
		switch m.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(m.Key)
		default:
			return nil, nil
		}
		// 加锁
		kind := mvcc.WriteKindFromProto(m.Op)
		txn.PutLock(m.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}
	// 返回错误信息
	if keyError != nil {
		resp.Errors = keyError
		return resp, nil
	}
	// 将事务的write操作保存
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	// 如果请求的操作为空
	if req.Keys == nil {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// 上锁？
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		// 获取每一个 key 的 Lock
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock == nil {
			continue
		}
		// 检查 Lock 的 StartTs 和当前事务的 startTs 是否一致，不一致直接返回
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return resp, err
			}
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					resp.Error = &kvrpcpb.KeyError{Retryable: "false"}
				}
			}
			return resp, nil
		}
		// 此时lock为当前事务加的锁，执行commit，写入Write
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		// 删除该事务加的锁
		txn.DeleteLock(key)
	}
	// 将事务的write操作保存
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	// 获取 scanner
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	// 开始扫描
	var pairs []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit); i++ {
		key, value, err := scanner.Next()
		if err != nil {
			return resp, err
		}
		if key == nil {
			break
		}
		// 获取 Lock
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		// 如果 Lock 的 startTs 小于当前的 startTs，说明存在你之前存在尚未 commit 的请求，返回错误，继续循环
		if lock != nil && lock.Ts <= req.Version {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl, // 生存时间
					}},
				Key: key,
			})
			continue
		}
		// 如果value存在
		if value != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
		}
	}
	resp.Pairs = pairs
	return resp, nil
}

//  在 Client failure 后，想继续执行时先检查 Primary Key 的状态，以此决定是回滚还是继续推进 commit
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	// 获取主键的锁
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	// 如果没有锁
	if lock == nil {
		//  获取 primary key 的 Write
		write, ts, err := txn.CurrentWrite(req.PrimaryKey)
		if err != nil {
			return resp, err
		}
		// 如果不是 WriteKindRollback，则说明已经被 commit,需返回其 commitTs
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = ts
			return resp, nil
		}
		// 如果是 WriteKindRollback，说明 primary key 已经被回滚了，执行写入回滚操作
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		// 将事务的write操作保存
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return resp, err
		}
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}
	// 如果有锁
	// 计算ttl是否超时(使用时间戳的物理部分)
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		// 超时，移除该 Lock 和 Value
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		// 执行写入回滚操作
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		// 将事务的write操作保存
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return resp, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		return resp, nil
	}
	// 不超时，返回ttl，等待 Lock 超时为止
	resp.LockTtl = lock.Ttl
	return resp, nil
}

// 批量回滚 key
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	// 如果请求的操作为空
	if req.Keys == nil {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// 上锁？
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		// 获取每一个 key 的 write
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil {
			// 如果已经是 WriteKindRollback，说明这个 key 已经被回滚完毕，跳过这个 key
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				// 否则删除该事务的value
				txn.DeleteValue(key)
				// 写入Write
				txn.PutWrite(key, req.StartVersion, &mvcc.Write{
					StartTS: req.StartVersion,
					Kind:    mvcc.WriteKindRollback,
				})
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
		}
		// 获取Lock
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		// 写入Write
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
		// 如果没有锁或者锁的 startTs 不是当前事务的 startTs，跳过这个 key,说明该 key 被其他事务拥有
		if lock == nil || lock.Ts != req.StartVersion {
			continue
		}
		// 删除该事务加的锁和该事务的value
		txn.DeleteLock(key)
		txn.DeleteValue(key)
	}
	// 将事务的write操作保存
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// 解决锁冲突
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	if req.StartVersion == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	// 得到Lock的迭代器
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close() // 延迟调用
	// 得到对应请求开始版本的已加锁的 key
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()               // 迭代器对应的键值对
		value, err := item.ValueCopy(nil) // 获得value的副本
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value) // 因为put的时候进行了ToBytes()，此处需要还原为lock形式
		if err != nil {
			return resp, err
		}
		// 如果 lock 的 Ts 等于请求的开始版本，说明是想要的key
		if lock.Ts == req.StartVersion {
			keys = append(keys, item.KeyCopy(nil))
		}
	}
	// 如果没有keys，说明没有锁冲突
	if len(keys) == 0 {
		return resp, nil
	}
	// 如果 CommitVersion 为0，回滚所有锁
	if req.CommitVersion == 0 {
		respRollback, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		//resp.RegionError = respRollback.RegionError
		resp.Error = respRollback.Error
		return resp, err
	} else { // 否则提交所有锁
		respCommit, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		//resp.RegionError = respCommit.RegionError
		resp.Error = respCommit.Error
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
