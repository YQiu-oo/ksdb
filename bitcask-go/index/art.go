package index

import (
	"bitcask-go/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// Adaptive Radix Tree
// https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock sync.RWMutex
}

func NewArt() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: sync.RWMutex{},
	}
}
func (ad *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	ad.lock.Lock()
	defer ad.lock.Unlock()
	ad.tree.Insert(key, pos)
	return true
}
func (ad *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	ad.lock.RLock()
	defer ad.lock.RUnlock()
	value, found := ad.tree.Search(key)

	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}
func (ad *AdaptiveRadixTree) Delete(key []byte) bool {
	ad.lock.Lock()
	defer ad.lock.Unlock()
	_, deleted := ad.tree.Delete(key)
	return deleted
}
func (ad *AdaptiveRadixTree) Close() error {

	return nil
}

// Size 索引中的数据量
func (ad *AdaptiveRadixTree) Size() int {
	ad.lock.RLock()
	defer ad.lock.RUnlock()
	return ad.tree.Size()
}

// Iterator 索引迭代器
func (ad *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if ad.tree == nil {
		return nil
	}
	ad.lock.RLock()
	defer ad.lock.RUnlock()
	return NewArtIterator(ad.tree, reverse)
}

type ArtIterator struct {
	current int
	reverse bool
	values  []*Item
}

func NewArtIterator(tree goart.Tree, reverse bool) *ArtIterator {
	var idx int

	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		values[idx] = &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &ArtIterator{
		current: 0,
		reverse: reverse,
		values:  values,
	}

}

func (at *ArtIterator) Rewind() {
	at.current = 0
}

// binary search
func (at *ArtIterator) Seek(key []byte) {
	if at.reverse {
		at.current = sort.Search(len(at.values), func(i int) bool {
			return bytes.Compare(at.values[i].key, key) <= 0
		})
	} else {
		at.current = sort.Search(len(at.values), func(i int) bool {
			return bytes.Compare(at.values[i].key, key) >= 0
		})
	}
}
func (at *ArtIterator) Next() {
	at.current += 1
}
func (at *ArtIterator) Valid() bool {
	return at.current < len(at.values)
}
func (at *ArtIterator) Key() []byte {
	return at.values[at.current].key
}
func (at *ArtIterator) Value() *data.LogRecordPos {
	return at.values[at.current].pos
}
func (at *ArtIterator) Close() {
	at.values = nil
}
