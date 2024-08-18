package db

import (
	"bitcask-go/data"
	error2 "bitcask-go/error"
	"bitcask-go/index"
	"bitcask-go/merge"
	"bitcask-go/option"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DB struct {
	option    option.Options
	Mu        *sync.RWMutex
	active    *data.DataFile            //allow write
	old       map[uint32]*data.DataFile //only read old file
	Index     index.Indexer
	fileIds   []int //加载索引的时候
	IsMerging bool

	SeqNo uint64
}

func (d *DB) GetOptions() option.Options {
	return d.option
}

func (d *DB) GetOld() map[uint32]*data.DataFile {
	return d.old
}
func (d *DB) GetActiveFile() *data.DataFile {
	return d.active
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.active == nil {
		return nil
	}
	db.Mu.Lock()
	defer db.Mu.Unlock()
	return db.active.Sync()
}
func OpenDB(option option.Options) (*DB, error) {
	if err := CheckOptions(option); err != nil {
		return nil, err
	}
	//create dir if dir path not exist
	if _, err := os.Stat(option.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(option.DirPath, os.ModePerm); err != nil {
			return nil, err

		}
	}

	db := &DB{
		option: option,
		Mu:     new(sync.RWMutex),
		old:    make(map[uint32]*data.DataFile),
		Index:  index.NewIndexer(index.IndexType(option.IndexType)), //cast
	}
	if err := merge.LoadMergeFiles(db); err != nil {
		return nil, err
	}

	//加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}
	//从hint文件中加载索引文件
	if err := merge.LoadIndexFromIndexFile(db); err != nil {
		return nil, err
	}

	//加载索引
	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}
	return db, nil

}

func CheckOptions(option option.Options) error {
	if option.DirPath == "" {
		return errors.New("dirpath is required")
	}
	if option.DataFileSize <= 0 {
		return errors.New("datafile size is required")
	}
	return nil
}
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return error2.EmptyKeyError
	}

	record := &data.LogRecord{
		LogRecordWithKeySeqNo(key, NonTrans),
		value,
		data.Normal,
	}

	pos, err := db.AppendLogRecordWithLock(record)

	if err != nil {

		return err
	}
	if ok := db.Index.Put(key, pos); !ok {
		return error2.IndexFailError
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, error2.EmptyKeyError
	}
	recordPos := db.Index.Get(key)
	if recordPos == nil {
		return nil, error2.KeyNotExistsError
	}
	return db.getValueByPosition(recordPos)
}

func (db *DB) getValueByPosition(recordPos *data.LogRecordPos) ([]byte, error) {
	var datafile *data.DataFile
	if db.active.FileId == recordPos.Fid {
		datafile = db.active
	} else {
		datafile = db.old[recordPos.Fid]
	}
	if datafile == nil {
		return nil, error2.DataFileNotFoundError
	}
	log, _, err := datafile.Read(recordPos.Offset)
	if err != nil {
		return nil, err
	}
	if log.Type == data.Deleted {
		return nil, error2.KeyNotExistsError
	}
	return log.Value, nil
}
func (db *DB) AppendLogRecordWithLock(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	return db.AppendLogRecord(record)
}

func (db *DB) AppendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {

	if db.active == nil {
		if err := db.SetActiveFile(); err != nil {
			return nil, err
		}
	}

	encodelogrecord, size := data.EncodeLogRecord(record)
	//如果当前活跃文件写入空间不足，需要重新建一个
	if db.active.WriteOff+size > db.option.DataFileSize {
		//先持久化当前数据
		if err := db.active.Sync(); err != nil {

			return nil, err
		}

		db.old[db.active.FileId] = db.active
		if err := db.SetActiveFile(); err != nil {
			return nil, err
		}

	}

	writeoff := db.active.WriteOff
	if err := db.active.Write(encodelogrecord); err != nil {
		return nil, err
	}

	if db.option.SyncWrites {
		if err := db.active.Sync(); err != nil {
			return nil, err
		}

	}
	pos := &data.LogRecordPos{db.active.FileId, writeoff}

	return pos, nil
}

// when visiting this method, need a lock to avoid competition during 并发
func (db *DB) SetActiveFile() error {
	var initialFileId uint32 = 0
	if db.active != nil {
		initialFileId = db.active.FileId + 1
	}

	//open new file, user determines these param through configurations
	datafile, err := data.OpenDataFile(db.option.DirPath, initialFileId)
	if err != nil {
		return err
	}

	db.active = datafile
	return nil
}

func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	for _, dirEntry := range dirEntries {
		if strings.HasSuffix(dirEntry.Name(), data.DataFileNameSuffix) {
			names := strings.Split(dirEntry.Name(), ".")
			fileId, err := strconv.Atoi(names[0])
			if err != nil {
				return error2.DataDirCorruptedError
			}
			fileIds = append(fileIds, fileId)
		}
	}

	sort.Ints(fileIds)
	db.fileIds = fileIds

	for i, id := range fileIds {
		dataFile, err := data.OpenDataFile(db.option.DirPath, uint32(id))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.active = dataFile
		} else {
			db.old[uint32(id)] = dataFile

		}
	}
	return nil

}

func (db *DB) loadIndexFromDataFile() error {
	if len(db.fileIds) == 0 {
		return nil
	}
	//查看是否发生过merge
	hasMerge := false
	noneMergeFile := uint32(0)

	mergeFinishedFile := filepath.Join(db.option.DirPath, data.MergeTagFile)
	if _, err := os.Stat(mergeFinishedFile); err == nil {

		file, err := merge.GetNonMergeFiles(db, db.option.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		noneMergeFile = file
	}

	updateIndex := func(key []byte, logRecordType data.LogRecordType, logRecordPos *data.LogRecordPos) {
		var ok bool
		if logRecordType == data.Deleted {
			ok = db.Index.Delete(key)
		} else {
			ok = db.Index.Put(key, logRecordPos)
		}

		if !ok {
			panic("failed to update index at startup")
		}
	}

	//暂存事务数据
	transactionRecord := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = NonTrans
	for i, fileId := range db.fileIds {
		var fid = uint32(fileId)
		var datafile *data.DataFile
		if hasMerge && uint32(fileId) < noneMergeFile {
			continue
		}
		if db.active.FileId == fid {
			datafile = db.active
		} else {
			datafile = db.old[fid]
		}
		var offset int64 = 0
		for {
			logRecord, size, err := datafile.Read(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			logRecordPos := &data.LogRecordPos{
				fid,
				offset,
			}
			//改造1：writebatch会有一部分数据
			realKey, seqNo := ParseLogRecordKey(logRecord.Key)
			if seqNo == NonTrans {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				if logRecord.Type == data.TxnFinished {
					for _, record := range transactionRecord[seqNo] {
						updateIndex(record.Record.Key, record.Record.Type, record.Pos)
					}
					delete(transactionRecord, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecord[seqNo] = append(transactionRecord[seqNo], &data.TransactionRecord{
						logRecord,
						logRecordPos,
					})

				}
			}
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size

		}
		if i == len(db.fileIds)-1 {
			db.active.WriteOff = offset
		}
	}
	db.SeqNo = currentSeqNo

	fmt.Print("-------11111------")
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return error2.EmptyKeyError
	}

	if pos := db.Index.Get(key); pos != nil {
		return nil
	}

	logRecord := &data.LogRecord{Key: LogRecordWithKeySeqNo(key, NonTrans), Type: data.Deleted}

	_, err := db.AppendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	if ok := db.Index.Delete(key); !ok {
		return error2.IndexFailError
	}
	return nil
}
func (db *DB) ListKeys() [][]byte {
	iter := db.Index.Iterator(false)
	keys := make([][]byte, db.Index.Size())
	var idx int
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys[idx] = iter.Key()
		idx++
	}
	return keys

}

// Fold 获取所有数据，并执行用户指定的操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.Mu.RLock()
	defer db.Mu.RUnlock()

	iterator := db.Index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) Close() error {
	if db.active == nil {
		return nil
	}

	db.Mu.Lock()
	defer db.Mu.Unlock()

	// 同步活跃文件内容并关闭
	if err := db.active.Sync(); err != nil {
		return err
	}
	if err := db.active.Close(); err != nil {
		return err
	}

	// 添加延迟，确保文件句柄释放
	time.Sleep(100 * time.Millisecond)

	// 尝试关闭旧文件，记录第一个错误但继续关闭其他文件
	var firstErr error
	for _, file := range db.old {
		if err := file.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// 添加延迟，确保所有文件资源释放
	time.Sleep(100 * time.Millisecond)

	return firstErr
}
