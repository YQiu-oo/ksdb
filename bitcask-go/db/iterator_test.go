package db

import (
	"bitcask-go/option"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destroyDB(db *DB) {
	if db != nil {
		if db.active != nil {
			_ = db.Close()
		}
		for _, of := range db.old {
			if of != nil {
				_ = of.Close()
			}
		}
		err := os.RemoveAll(db.option.DirPath)
		if err != nil {
			panic(err)
		}
	}
}
func TestDB_NewIterator(t *testing.T) {
	opts := option.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(option.DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := option.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	t.Log(db.Get(utils.GetTestKey(10)))
	assert.Nil(t, err)

	iterator := db.NewIterator(option.DefaultIteratorOptions)
	defer iterator.Close()
	t.Log(iterator.Value())
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), val)
}
func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := option.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("annde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("cnedc"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aeeue"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("esnue"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bnede"), utils.RandomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	iter1 := db.NewIterator(option.DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()
	for iter1.Seek([]byte("c")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	// 反向迭代
	iterOpts1 := option.DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()

	// 指定了 prefix
	iterOpts2 := option.DefaultIteratorOptions
	iterOpts2.Prefix = []byte("aee")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()
}
