package db

import (
	"bitcask-go/data"
	error2 "bitcask-go/error"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/merge"
	"bitcask-go/option"
	"bitcask-go/utils"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SeqKey        = "SeqNo"
	FileFlockName = "flock"
)

// Stat 存储引擎数量统计信息
type Stat struct {
	KeyNum         uint
	DataFileNum    uint
	ReclaimableNum int64
	DiskSize       int64 //数据目录所占空间大小
}

func (db *DB) BackUp(dir string) error {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	return utils.CopyDir(db.option.DirPath, dir, []string{FileFlockName})
}

func (db *DB) Stat() *Stat {
	db.Mu.Lock()
	defer db.Mu.Unlock()

	var datafiles = uint(len(db.old))
	if db.active != nil {
		datafiles += 1
	}

	dirsize, err := utils.DirSize(db.option.DirPath)

	if err != nil {
		return nil
	}

	return &Stat{
		KeyNum:         uint(db.Index.Size()),
		DataFileNum:    datafiles,
		ReclaimableNum: db.ReclaimSize,
		DiskSize:       int64(dirsize), //Todo
	}
}

type DB struct {
	option       option.Options
	Mu           *sync.RWMutex
	active       *data.DataFile            //allow write
	old          map[uint32]*data.DataFile //only read old file
	Index        index.Indexer
	fileIds      []int //加载索引的时候
	IsMerging    bool
	SeqNo        uint64
	SeqFileExist bool
	isInitial    bool //是否第一次初始化
	fileLock     *flock.Flock
	bytesWrite   uint

	ReclaimSize int64 //累计有效数据多少之后才merge
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
	var isInitial bool
	//create dir if dir path not exist
	if _, err := os.Stat(option.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.Mkdir(option.DirPath, os.ModePerm); err != nil {
			return nil, err

		}
	}
	fileFlock := flock.New(filepath.Join(option.DirPath, FileFlockName))
	hold, err := fileFlock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, error2.DataBaseIsUsingError
	}

	entries, err := os.ReadDir(option.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	db := &DB{
		option:    option,
		Mu:        new(sync.RWMutex),
		old:       make(map[uint32]*data.DataFile),
		Index:     index.NewIndexer(option.IndexType, option.DirPath, option.SyncWrites), //cast
		isInitial: isInitial,
		fileLock:  fileFlock,
	}
	if err := merge.LoadMergeFiles(db); err != nil {
		return nil, err
	}

	//加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	if option.IndexType != index.BplusTree {
		////从hint文件中加载索引文件
		//if err := merge.LoadIndexFromIndexFile(db); err != nil {
		//	return nil, err
		//}

		//加载索引
		if err := db.loadIndexFromDataFile(); err != nil {
			return nil, err
		}
	}

	if option.IndexType == index.BplusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
	}
	if db.option.MMap {
		if err := db.resetIoType(); err != nil {
			return nil, err
		}
	}

	return db, nil

}
func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.option.DirPath, data.SeqNoFile)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqFile, err := data.OpenSeqNoFile(db.option.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqFile.Read(0)
	if err != nil {
		return err
	}

	seqNum, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.SeqNo = seqNum
	db.SeqFileExist = true
	return os.Remove(fileName)

}

func CheckOptions(option option.Options) error {
	if option.DirPath == "" {
		return errors.New("dirpath is required")
	}
	if option.DataFileSize <= 0 {
		return errors.New("datafile size is required")
	}
	if option.MergeRatio > 1 || option.MergeRatio < 0 {
		return errors.New("Should >= 0 and <= 1")
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
	if oldPos := db.Index.Put(key, pos); oldPos != nil {
		db.ReclaimSize += int64(oldPos.Size)
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
	db.bytesWrite += uint(size)
	var needSync = db.option.SyncWrites
	if !needSync && db.option.BytesSync > 0 && db.bytesWrite >= db.option.BytesSync {
		needSync = true
	}

	if needSync {
		if err := db.active.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}

	}
	pos := &data.LogRecordPos{db.active.FileId, writeoff, uint32(size)}

	return pos, nil
}

// when visiting this method, need a lock to avoid competition during 并发
func (db *DB) SetActiveFile() error {
	var initialFileId uint32 = 0
	if db.active != nil {
		initialFileId = db.active.FileId + 1
	}

	//open new file, user determines these param through configurations
	datafile, err := data.OpenDataFile(db.option.DirPath, initialFileId, fio.StandardFIO)
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
	iotype := fio.StandardFIO
	if db.option.MMap {
		iotype = fio.MemoryMap
	}
	for i, id := range fileIds {
		dataFile, err := data.OpenDataFile(db.option.DirPath, uint32(id), iotype)
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
		var oldPos *data.LogRecordPos
		if logRecordType == data.Deleted {
			oldPos, _ = db.Index.Delete(key)
			db.ReclaimSize += int64(logRecordPos.Size)
		} else {
			oldPos = db.Index.Put(key, logRecordPos)
		}
		if oldPos != nil {
			db.ReclaimSize += int64(oldPos.Size)
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
				uint32(size),
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

	pos, err := db.AppendLogRecordWithLock(logRecord)

	if err != nil {
		return err
	}
	db.ReclaimSize += int64(pos.Size)
	oldVal, ok := db.Index.Delete(key)
	if !ok {
		return error2.IndexFailError
	}
	if oldVal != nil {
		db.ReclaimSize += int64(oldVal.Size)
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
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("Failed to unlock file lock: %s", err))
		}
	}()
	if db.active == nil {
		return nil
	}

	db.Mu.Lock()
	defer db.Mu.Unlock()

	//保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.option.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(SeqKey),
		Value: []byte(strconv.FormatUint(db.SeqNo, 10)),
	}

	encRecord, _ := data.EncodeLogRecord(record)

	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}
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

// 将数据文件的 IO 类型设置为标准文件 IO
func (db *DB) resetIoType() error {
	if db.active == nil {
		return nil
	}

	if err := db.active.SetIOManager(db.option.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.old {
		if err := dataFile.SetIOManager(db.option.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
