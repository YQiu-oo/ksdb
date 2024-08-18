package Test

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	rec1 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  data.Normal,
	}
	res1, n1 := data.EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// value 为空的情况
	rec2 := &data.LogRecord{
		Key:  []byte("name"),
		Type: data.Normal,
	}
	res2, n2 := data.EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// 对 Deleted 情况的测试
	rec3 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  data.Deleted,
	}
	res3, n3 := data.EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := data.DecodeLogRecord(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(2532332136), h1.Crc)
	assert.Equal(t, data.Normal, h1.RecordType)
	assert.Equal(t, uint32(4), h1.KeySize)
	assert.Equal(t, uint32(10), h1.ValueSize)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := data.DecodeLogRecord(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.Crc)
	assert.Equal(t, data.Normal, h2.RecordType)
	assert.Equal(t, uint32(4), h2.KeySize)
	assert.Equal(t, uint32(0), h2.ValueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  data.Normal,
	}
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := data.GetLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2532332136), crc1)

	rec2 := &data.LogRecord{
		Key:  []byte("name"),
		Type: data.Normal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := data.GetLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	rec3 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  data.Deleted,
	}
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := data.GetLogRecordCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(290887979), crc3)
}
