package db

import (
	"bitcask-go/index"
	"bitcask-go/option"
	"bytes"
	"fmt"
)

type Iterator struct {
	iter    index.Iterator
	db      *DB
	options option.IteratorOptions
}

func (db *DB) NewIterator(options option.IteratorOptions) *Iterator {
	indexIter := db.Index.Iterator(options.Reverse)
	return &Iterator{
		db:      db,
		options: options,
		iter:    indexIter,
	}
}
func (it *Iterator) Rewind() {
	it.iter.Rewind()
}
func (it *Iterator) Seek(key []byte) {
	it.iter.Seek(key)
	it.FindPrefix()
}
func (it *Iterator) Next() {
	it.iter.Next()
	it.FindPrefix()
}
func (it *Iterator) Key() []byte {
	return it.iter.Key()
}
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.iter.Value()
	it.db.Mu.Lock()
	defer it.db.Mu.Unlock()
	fmt.Print(logRecordPos)
	return it.db.getValueByPosition(logRecordPos)
}
func (it *Iterator) Valid() bool {
	return it.iter.Valid()
}
func (it *Iterator) Close() {
	it.iter.Close()
}

func (it *Iterator) FindPrefix() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.iter.Valid(); it.iter.Next() {
		key := it.iter.Key()
		if len(key) >= prefixLen && bytes.Equal(key[:prefixLen], it.options.Prefix) {
			break
		}
	}
}
