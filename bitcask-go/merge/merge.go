package merge

import (
	"bitcask-go/data"
	db3 "bitcask-go/db"
	error2 "bitcask-go/error"
	"bitcask-go/utils"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

// Previously, we have inserted log record to help us delete the data. To be clear,
// we just removed them from memory data structure, but we didn't remove it out of data files(disk)
// In merge, we will loop through all data files and compare each records pos with the pos stored in memory
// data structure.
const (
	MergeDirName     = "-merge"
	MergeFinishedKey = "merge.Finished"
)

func Merge(db *db3.DB) error {
	if db.GetActiveFile() == nil {
		return nil
	}
	db.Mu.Lock()

	if db.IsMerging {
		db.Mu.Unlock()
		return error2.MergeInProgressError
	}

	dirsize, err := utils.DirSize(db.GetOptions().DirPath)
	if err != nil {
		db.Mu.Unlock()
		return err
	}
	if float32(db.ReclaimSize)/float32(dirsize) < db.GetOptions().MergeRatio {
		db.Mu.Unlock()
		return errors.New("has not reached merge ratio")
	}
	availablediskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.Mu.Unlock()
		return err
	}

	if uint64(dirsize-db.ReclaimSize) >= availablediskSize {
		db.Mu.Unlock()
		return error2.MergeInProgressError
	}
	db.IsMerging = true
	defer func() {
		db.IsMerging = false
	}()

	if err := db.GetActiveFile().Sync(); err != nil {
		return err
	}
	db.GetOld()[db.GetActiveFile().FileId] = db.GetActiveFile()
	if err := db.SetActiveFile(); err != nil {
		db.Mu.Unlock()
		return err
	}
	//取出所有要merge的文件
	var mergeFiles []*data.DataFile

	for _, file := range db.GetOld() {
		mergeFiles = append(mergeFiles, file)
	}
	db.Mu.Unlock() //新用户继续往新活跃文件写，不会受影响
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := GetMergePath(db)
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	mergeOptions := db.GetOptions()
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false

	mergeDB, err := db3.OpenDB(mergeOptions)
	if err != nil {
		return err
	}
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.Read(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := db3.ParseLogRecordKey(logRecord.Key)
			pos := db.Index.Get(realKey)
			if pos != nil && pos.Fid == dataFile.FileId && pos.Offset == offset {
				//清除事务标记
				logRecord.Key = db3.LogRecordWithKeySeqNo(realKey, db3.NonTrans)
				pos, err := mergeDB.AppendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//将当前位置写入hint file中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err

				}
			}
			offset += size

		}
	}
	//sync持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	mergeFinishedFile, err := data.OpenMergeTagFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(MergeFinishedKey),
		Value: []byte(strconv.Itoa(int(db.GetActiveFile().FileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

func GetMergePath(db *db3.DB) string {
	dir := path.Dir(path.Clean(db.GetOptions().DirPath))
	base := path.Base(db.GetOptions().DirPath)
	return path.Join(dir, base+MergeDirName)
}

func LoadMergeFiles(db *db3.DB) error {
	mergePath := GetMergePath(db)
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dir, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	var mergeFinished bool
	var mergeFileNames []string
	for _, file := range dir {
		if file.Name() == data.MergeTagFile {
			mergeFinished = true

		}
		if file.Name() == data.SeqNoFile {
			continue
		}
		if file.Name() == db3.FileFlockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, file.Name())
	}
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := GetNonMergeFiles(db, mergePath)
	if err != nil {
		return err
	}

	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.GetOptions().DirPath, fileId)
		if _, err := os.Stat(path.Join(mergePath, fileName)); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.GetOptions().DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func GetNonMergeFiles(db *db3.DB, path string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeTagFile(path)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.Read(0)
	if err != nil {
		return 0, err
	}
	noneMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(noneMergeFileId), nil

}

func LoadIndexFromIndexFile(db *db3.DB) error {
	hintFileName := filepath.Join(db.GetOptions().DirPath, data.HintFileNameSuffix)

	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	hintFile, err := data.OpenHintFile(db.GetOptions().DirPath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil
		}
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.Index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
