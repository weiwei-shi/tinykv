package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
	scanner.iter.Seek(EncodeKey(startKey, scanner.txn.StartTS))
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	item := scan.iter.Item()            // 迭代器对应的键值对
	gotKey := item.KeyCopy(nil)         // 获得 key 的副本
	decodedKey := DecodeUserKey(gotKey) // 之前对 key 和时间戳进行了 encoder，这里要解码获得key
	scan.iter.Seek(EncodeKey(decodedKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	item = scan.iter.Item()      // 迭代器对应的键值对
	gotKey = item.KeyCopy(nil)   // 获得 key 的副本
	key := DecodeUserKey(gotKey) // 之前对 key 和时间戳进行了 encoder，这里要解码获得key
	// 判断找到 Write 的 key 是不是就是需要的 key
	if !bytes.Equal(decodedKey, key) {
		return scan.Next()
	}
	for {
		if !bytes.Equal(key, decodedKey) {
			break
		}
		scan.iter.Next()
		if !scan.iter.Valid() {
			break
		}
		tempItem := scan.iter.Item()
		gotKey = tempItem.KeyCopy(nil)
		key = DecodeUserKey(gotKey)
	}
	writeValue, err := item.ValueCopy(nil) //获得value的副本
	if err != nil {
		return decodedKey, nil, err
	}
	write, err := ParseWrite(writeValue)
	if err != nil {
		return decodedKey, nil, err
	}
	if write.Kind == WriteKindDelete {
		return decodedKey, nil, nil
	}
	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(decodedKey, write.StartTS))
	return decodedKey, value, err
}
