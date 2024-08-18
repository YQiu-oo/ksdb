package Test

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := index.NewBtree(32)

	res := bt.Put([]byte("aaaa"), &data.LogRecordPos{1, 100})

	assert.True(t, res)

}

func TestBtree_Get(t *testing.T) {
	bt := index.NewBtree(32)
	res := bt.Put([]byte("aaaa"), &data.LogRecordPos{1, 100})
	assert.True(t, res)

	res1 := bt.Get([]byte("aaaa"))

	assert.Equal(t, int64(100), res1.Offset)
}

func TestBtree_Delete(t *testing.T) {

	bt := index.NewBtree(32)
	res := bt.Put([]byte("aaaa"), &data.LogRecordPos{1, 100})
	assert.True(t, res)

	res2 := bt.Delete([]byte("aaaa"))
	fmt.Print(res2)
	assert.True(t, res2)

}
func TestBTree_Iterator(t *testing.T) {
	bt1 := index.NewBtree(32)
	// 1.BTree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//	2.BTree 有数据的情况
	bt1.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.有多条数据
	bt1.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4.测试 seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5.反向遍历的 seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
