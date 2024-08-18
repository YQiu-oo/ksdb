package data

import (
	"bitcask-go/fio"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const (
	DataFileNameSuffix = ".data"
	HintFileNameSuffix = "hint-index"
	MergeTagFile       = "merge-Finished"
)

type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IOManager fio.IOManager
}

func OpenMergeTagFile(path string) (*DataFile, error) {
	fileName := filepath.Join(path, MergeTagFile)
	manager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{FileId: 0, WriteOff: 0, IOManager: manager}, nil
}
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}

	encRecord, _ := EncodeLogRecord(record)

	return df.Write(encRecord)
}

func GetDataFileName(path string, fileId uint32) string {
	return filepath.Join(path, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}
func OpenHintFile(path string) (*DataFile, error) {
	fileName := filepath.Join(path, HintFileNameSuffix)
	manager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{FileId: 0, WriteOff: 0, IOManager: manager}, nil

}
func OpenDataFile(path string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(path, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	manager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{FileId: fileId, WriteOff: 0, IOManager: manager}, nil
}
func (d *DataFile) Sync() error {

	return d.IOManager.Sync()
}
func (d *DataFile) Write(buf []byte) error {
	n, err := d.IOManager.Write(buf)
	if err != nil {
		return err
	}
	d.WriteOff += int64(n)
	return nil
}
func (d *DataFile) Close() error {
	return d.IOManager.Close()
}

func (d *DataFile) Read(offset int64) (*LogRecord, int64, error) {

	sl, err := d.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var hb int64 = maxHeaderSize
	if offset+maxHeaderSize > sl {
		hb = sl - offset
	}
	h, err := d.readNBytes(hb, offset)

	if err != nil {
		return nil, 0, err
	}

	header, size := DecodeLogRecord(h)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.Crc == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF
	}
	keysize, valuesize := int64(header.KeySize), int64(header.ValueSize)
	var recordSize = size + keysize + valuesize

	logrecord := &LogRecord{Type: header.RecordType}
	if keysize > 0 || valuesize > 0 {
		b, err := d.readNBytes(keysize+valuesize, offset+size)
		if err != nil {
			return nil, 0, err
		}

		logrecord.Key = b[:keysize]
		logrecord.Value = b[keysize:]

	}
	crc := GetLogRecordCRC(logrecord, h[crc32.Size:size])

	if crc != header.Crc {
		return nil, 0, fmt.Errorf("crc mismatch")
	}
	return logrecord, recordSize, nil
}

func (d *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = d.IOManager.Read(b, offset)
	return
}
