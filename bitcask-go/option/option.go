package option

import "os"

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrites   bool
	IndexType    IndexType
	BytesSync    uint

	MMap bool //是否需要在启动的时候用mmap

	//merge阈值
	MergeRatio float32
}

type IndexType = int8

const (
	BTree IndexType = iota + 1
	Art
	BplusTree
)

type IteratorOptions struct {
	Reverse bool
	Prefix  []byte
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTree,
	BytesSync:    0,
	MMap:         true,
	MergeRatio:   0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	MaxBatchSize uint

	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 10000,
	SyncWrites:   true,
}
