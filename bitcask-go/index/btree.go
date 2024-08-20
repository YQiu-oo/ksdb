package index

import (
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"

	"bitcask-go/data"
)

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree(degree int) *Btree {
	return &Btree{
		tree: btree.New(degree),
		lock: new(sync.RWMutex),
	}

}
func (b *Btree) Close() error {
	return nil
}
func (bt *Btree) Size() int {
	return bt.tree.Len()
}
func (b *Btree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	b.lock.Lock()
	defer b.lock.Unlock()
	oldval := b.tree.ReplaceOrInsert(it)
	if oldval == nil {
		return nil
	}
	return oldval.(*Item).pos
}
func (b *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := b.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (b *Btree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}

	b.lock.Lock()
	defer b.lock.Unlock()
	oldItem := b.tree.Delete(it)
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true

}

type btreeIterator struct {
	current int
	reverse bool
	values  []*Item
}

func NewBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	helper := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(helper)
	} else {
		tree.Ascend(helper)
	}

	return &btreeIterator{
		current: 0,
		reverse: reverse,
		values:  values,
	}

}

func (bit *Btree) Iterator(reverse bool) Iterator {
	if bit.tree == nil {
		return nil
	}
	bit.lock.RLock()
	defer bit.lock.RUnlock()
	return NewBtreeIterator(bit.tree, reverse)
}

func (bit *btreeIterator) Rewind() {
	bit.current = 0
}

// binary search
func (bit *btreeIterator) Seek(key []byte) {
	if bit.reverse {
		bit.current = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].key, key) <= 0
		})
	} else {
		bit.current = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].key, key) >= 0
		})
	}
}
func (bit *btreeIterator) Next() {
	bit.current += 1
}
func (bit *btreeIterator) Valid() bool {
	return bit.current < len(bit.values)
}
func (bit *btreeIterator) Key() []byte {
	return bit.values[bit.current].key
}
func (bit *btreeIterator) Value() *data.LogRecordPos {
	return bit.values[bit.current].pos
}
func (bit *btreeIterator) Close() {
	bit.values = nil
}
