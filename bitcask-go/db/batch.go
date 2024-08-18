package db

import (
	"bitcask-go/data"
	error2 "bitcask-go/error"
	"bitcask-go/option"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const NonTrans = 1

type WriteBatch struct {
	mu      *sync.Mutex
	options option.WriteBatchOptions
	db      *DB
	temp    map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opts option.WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		mu:      new(sync.Mutex),
		options: opts,
		db:      db,
		temp:    make(map[string]*data.LogRecord),
	}
}

func (w *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return error2.EmptyKeyError
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	logrecords := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	w.temp[string(key)] = logrecords
	return nil
}

func (w *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return error2.EmptyKeyError
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	pos := w.db.Index.Get(key)
	if pos == nil {
		if w.temp[string(key)] != nil {
			delete(w.temp, string(key))
		}
		return nil
	}

	logRecords := &data.LogRecord{
		Key:  key,
		Type: data.Deleted,
	}
	w.temp[string(key)] = logRecords
	return nil
}

// Commit 先持久化再更新内存是怕丢失数据
func (w *WriteBatch) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.temp) == 0 {
		return nil
	}
	if uint(len(w.temp)) > w.options.MaxBatchSize {
		return error2.ExceedMaxBatchSizeError
	}

	seqNo := atomic.AddUint64(&w.db.SeqNo, 1)
	t := make(map[string]*data.LogRecordPos)
	for _, record := range w.temp {
		logRecordPos, err := w.db.AppendLogRecordWithLock(&data.LogRecord{
			Key:   LogRecordWithKeySeqNo(record.Key, seqNo),
			Type:  record.Type,
			Value: record.Value,
		})
		if err != nil {
			return err
		}
		t[string(record.Key)] = logRecordPos
	}

	//加一个标识事务完成
	finish := &data.LogRecord{
		Key:  LogRecordWithKeySeqNo([]byte("txn-finish"), seqNo),
		Type: data.TxnFinished,
	}
	if _, err := w.db.AppendLogRecordWithLock(finish); err != nil {
		return err
	}

	if w.options.SyncWrites && w.db.GetActiveFile() != nil {
		if err := w.db.Sync(); err != nil {
			return err
		}
	}

	//更新内存索引
	for _, record := range w.temp {
		pos := t[string(record.Key)]
		if record.Type == data.Normal {
			w.db.Index.Put(record.Key, pos)
		}
		if record.Type == data.Deleted {
			w.db.Index.Delete(record.Key)
		}

	}
	w.temp = make(map[string]*data.LogRecord)
	return nil
}
func LogRecordWithKeySeqNo(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 的 key，获取实际的 key 和事务序列号
func ParseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
