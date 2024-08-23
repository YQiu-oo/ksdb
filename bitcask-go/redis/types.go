package redis

import (
	bitcask "bitcask-go/db"
	error2 "bitcask-go/error"
	"bitcask-go/option"
	"bitcask-go/utils"
	"encoding/binary"
	"errors"
	"time"
)

type redisDsType = byte

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

const (
	string redisDsType = iota
	Hash
	Set
	List
	ZSet
)

type RedisDataStructure struct {
	db *bitcask.DB
}

func NewRedisDataStructure(options option.Options) (*RedisDataStructure, error) {
	db, err := bitcask.OpenDB(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// String
func (red *RedisDataStructure) Set(key []byte, expire time.Duration, value []byte) error {
	if value == nil {
		return nil
	}
	//value : type + expire +payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = string
	var index = 1
	var exp int64 = 0
	if expire != 0 {
		exp = time.Now().Add(expire).UnixNano()
	}
	index += binary.PutVarint(buf[index:], exp)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	return red.db.Put(key, encValue)
}

func (red *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := red.db.Get(key)
	if err != nil {
		return nil, err
	}
	dataType := encValue[0]
	if dataType != string {
		return nil, errors.New("invalid data type")
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}

// HashSet, Update metadata and value
func (red *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	//Looking for Metadata
	meta, err := red.findMetadata(key, Hash)

	if err != nil {
		return false, err
	}

	hk := &hashInternalKey{
		key,
		meta.version,
		field,
	}
	encKey := hk.encode()

	var exist = true

	if _, err := red.db.Get(encKey); err == error2.KeyNotExistsError {
		exist = false
	}

	wb := red.db.NewWriteBatch(option.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode()) // 1.
	}
	_ = wb.Put(encKey, value) // 2.

	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (red *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := red.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	hk := &hashInternalKey{
		key,
		meta.version,
		field,
	}

	return red.db.Get(hk.encode())

}

func (red *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := red.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKey{
		key,
		meta.version,
		field,
	}

	encKey := hk.encode()

	// 先查看是否存在
	var exist = true
	if _, err = red.db.Get(encKey); err == error2.KeyNotExistsError {
		exist = false
	}

	if exist {
		wb := red.db.NewWriteBatch(option.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil

}

// Set
func (red *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := red.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := &SetInternalKey{
		key,
		meta.version,
		member,
	}

	var ok bool
	encKey := sk.encode()
	if _, err := red.db.Get(encKey); err == error2.KeyNotExistsError {
		wb := red.db.NewWriteBatch(option.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil

}

func (red *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := red.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := &SetInternalKey{
		key,
		meta.version,
		member,
	}

	_, err = red.db.Get(sk.encode())
	if err != nil && err != error2.KeyNotExistsError {
		return false, err
	}
	if err == error2.KeyNotExistsError {
		return false, nil
	}
	return true, nil

}

func (red *RedisDataStructure) SRemove(key, member []byte) (bool, error) {
	meta, err := red.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := &SetInternalKey{
		key,
		meta.version,
		member,
	}

	if _, err = red.db.Get(sk.encode()); err == error2.KeyNotExistsError {
		return false, nil
	}
	// 更新
	wb := red.db.NewWriteBatch(option.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// List

func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 构造数据部分的 key
	lk := &ListInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新元数据和数据部分
	wb := rds.db.NewWriteBatch(option.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造数据部分的 key
	lk := &ListInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
}

// zset
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 查看是否已经存在
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != error2.KeyNotExistsError {
		return false, err
	}
	if err == error2.KeyNotExistsError {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(option.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDsType) (*Metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != error2.KeyNotExistsError {
		return nil, err
	}

	var meta *Metadata
	var exist = true
	if err == error2.KeyNotExistsError {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &Metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
