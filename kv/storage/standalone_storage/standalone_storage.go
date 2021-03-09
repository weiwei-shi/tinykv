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
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := path.Join(dbPath, "kv")
	raftPath := path.Join(dbPath, "raft")
	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)
	return &StandAloneStorage{
		config:  conf,
		engines: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engines.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneReader(s.engines.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			if err := engine_util.PutCF(s.engines.Kv, put.Cf, put.Key, put.Value); err != nil {
				return err
			}
		case storage.Delete:
			delete := b.Data.(storage.Delete)
			if err := engine_util.DeleteCF(s.engines.Kv, delete.Cf, delete.Key); err != nil {
				return err
			}
		}
	}
	return nil
}
func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		txn: txn,
	}
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	if value, err := engine_util.GetCFFromTxn(s.txn, cf, key); err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		return value, nil
	}
}
func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}
func (s *StandAloneReader) Close() {
	s.txn.Discard()
}
