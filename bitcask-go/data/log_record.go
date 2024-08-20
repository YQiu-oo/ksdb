package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	Normal LogRecordType = iota
	Deleted
	TxnFinished
)

// crc,type,key,value
const maxHeaderSize = binary.MaxVarintLen32*2 + 5

type LogRecord struct {
	Key   []byte
	Value []byte

	Type LogRecordType
}

type logRecorderHeader struct {
	Crc        uint32
	RecordType LogRecordType
	KeySize    uint32
	ValueSize  uint32
}
type LogRecordPos struct {
	Fid    uint32
	Offset int64
	Size   uint32 //数据在磁盘上的大小
}

// return byte and len of content
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	header := make([]byte, maxHeaderSize)

	header[4] = record.Type

	var index = 5

	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	index += binary.PutVarint(header[index:], int64(len(record.Value)))

	var size = index + len(record.Key) + len(record.Value)
	encoded := make([]byte, size)

	copy(encoded[:index], header[:index])

	copy(encoded[index:], record.Key)
	copy(encoded[index+len(record.Key):], record.Value)

	crc := crc32.ChecksumIEEE(encoded[4:])
	binary.LittleEndian.PutUint32(encoded[:4], crc)

	return encoded, int64(size)

}

func DecodeLogRecord(data []byte) (*logRecorderHeader, int64) {
	if len(data) <= 4 {
		return nil, 0
	}

	header := &logRecorderHeader{
		Crc:        binary.LittleEndian.Uint32(data[:4]),
		RecordType: data[4],
	}

	var index = 5

	keySize, n := binary.Varint(data[index:])
	header.KeySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(data[index:])
	header.ValueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}
func GetLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fid, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	Size, n := binary.Varint(buf[index:])

	return &LogRecordPos{
		uint32(fid),
		offset,
		uint32(Size),
	}
}

type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}
