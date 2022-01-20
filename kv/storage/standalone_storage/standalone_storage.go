package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := path.Join(conf.DBPath, "kv")          //定义存储kv的路径
	raftPath := path.Join(conf.DBPath, "raft")      //定义存储raft的路径
	kvEngine := engine_util.CreateDB(kvPath, false) //在该路径创建新的存储引擎BargerDB
	raftEngine := engine_util.CreateDB(raftPath, true)

	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath),
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneReader(s.engine.Kv.NewTransaction(false))
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, b := range batch {
		switch b.Data.(type) { //需要判断数据的类型是插入还是删除
		case storage.Put:
			put := b.Data.(storage.Put)
			err := engine_util.PutCF(s.engine.Kv, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := b.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engine.Kv, delete.Cf, delete.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//以下部分需要自己实现，storage.go中有相关api
type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneReader(kvTxn *badger.Txn) (*StandAloneReader, error) {
	return &StandAloneReader{kvTxn: kvTxn}, nil
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.kvTxn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.kvTxn)
}

func (s *StandAloneReader) Close() {
	s.kvTxn.Discard()
}
