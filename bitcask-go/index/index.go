package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool

	// Size 索引中的数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator

	// Close 关闭索引
	Close() error
}

type IndexType = int8

const (
	bt IndexType = iota + 1
	Art
	BplusTree
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case bt:
		return NewBtree(32)

	case Art:
		return NewArt()
	case BplusTree:
		return NewBPlusTree(dirPath, sync)

	default:
		panic("unknown index type")

	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// 因为btree.Item是接口类型，包括了指针，所以不加*
func (it *Item) Less(b btree.Item) bool {
	return bytes.Compare(it.key, b.(*Item).key) == -1
}

// Index Iterator for vary types of index
type Iterator interface {
	Rewind()
	Seek(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() *data.LogRecordPos
	Close()
}
