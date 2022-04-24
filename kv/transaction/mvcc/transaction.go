package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, ts), // key 和时间戳组合成一个编码 key
			Cf:    engine_util.CfWrite,
			Value: write.ToBytes(),
		},
	}
	txn.writes = append(txn.writes, modify)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	lock, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		return nil, nil
	}
	parseLock, err := ParseLock(lock) // 因为put的时候进行了ToBytes()，此处需要还原为lock形式
	if err != nil {
		return nil, err
	}
	return parseLock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key:   key,
			Cf:    engine_util.CfLock,
			Value: lock.ToBytes(),
		},
	}
	txn.writes = append(txn.writes, modify)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
// 查询当前事务下，传入 key 对应的 Value
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	// 得到迭代器
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close() // 延迟调用
	// 遍历Write以找到key
	iter.Seek(EncodeKey(key, txn.StartTS))
	// 如果没找到返回
	if !iter.Valid() {
		return nil, nil
	}
	item := iter.Item()                 // 迭代器对应的键值对
	gotKey := item.KeyCopy(nil)         // 获得key的副本
	decodedKey := DecodeUserKey(gotKey) // 之前put时对key和时间戳进行了encoder，这里要解码获得key
	// 判断找到 Write 的 key 是不是就是需要的 key
	if !bytes.Equal(key, decodedKey) {
		return nil, nil
	}
	gotValue, err := item.ValueCopy(nil) // 获得value的副本
	if err != nil {
		return nil, err
	}
	parseWrite, err := ParseWrite(gotValue) // 因为put的时候进行了ToBytes()，此处需要还原为write形式
	if err != nil {
		return nil, err
	}
	// 判断 Write 的 Kind 是不是 WriteKindPut
	if parseWrite.Kind != WriteKindPut {
		return nil, nil
	}
	// 从 Default 中通过获取值
	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, parseWrite.StartTS))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS), // key 和时间戳组合成一个编码 key
			Cf:    engine_util.CfDefault,
			Value: value,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Key: EncodeKey(key, txn.StartTS), // key 和时间戳组合成一个编码 key
			Cf:  engine_util.CfDefault,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 查询当前事务下，传入 key 的最新 Write
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	// 得到迭代器
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close() // 延迟调用
	// 从最大时间戳开始遍历以找到该 key 在该事务下的最新 Write
	for iter.Seek(EncodeKey(key, TsMax)); iter.Valid(); iter.Next() {
		item := iter.Item()                 // 迭代器对应的键值对
		gotKey := item.KeyCopy(nil)         // 获得key的副本
		decodedKey := DecodeUserKey(gotKey) // 之前put时对key和时间戳进行了encoder，这里要解码获得key
		// 判断该 Write 的 key 是不是就是需要的 key
		if !bytes.Equal(key, decodedKey) {
			return nil, 0, nil
		}
		gotValue, err := item.ValueCopy(nil) // 获得value的副本
		if err != nil {
			return nil, 0, err
		}
		parseWrite, err := ParseWrite(gotValue) // 因为put的时候进行了ToBytes()，此处需要还原为write形式
		if err != nil {
			return nil, 0, err
		}
		// 判断 Write 的时间戳是否等于该事务的开始时间戳
		if parseWrite.StartTS == txn.StartTS {
			return parseWrite, decodeTimestamp(gotKey), nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 查询传入 key 的最新 Write，这里不需要考虑事务的 startTs
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	// 得到迭代器
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close() // 延迟调用
	// 使用最大时间戳遍历以找到该 key 的最新 Write
	iter.Seek(EncodeKey(key, TsMax))
	// 如果没找到返回
	if !iter.Valid() {
		return nil, 0, nil
	}
	item := iter.Item()                 // 迭代器对应的键值对
	gotKey := item.KeyCopy(nil)         // 获得key的副本
	decodedKey := DecodeUserKey(gotKey) // 之前put时对key和时间戳进行了encoder，这里要解码获得key
	// 判断该 Write 的 key 是不是就是需要的 key
	if !bytes.Equal(key, decodedKey) {
		return nil, 0, nil
	}
	gotValue, err := item.ValueCopy(nil) // 获得value的副本
	if err != nil {
		return nil, 0, err
	}
	parseWrite, err := ParseWrite(gotValue) // 因为put的时候进行了ToBytes()，此处需要还原为write形式
	if err != nil {
		return nil, 0, err
	}
	return parseWrite, decodeTimestamp(gotKey), nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		log.Infof("27 here!")
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		log.Infof("28 here!")
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
