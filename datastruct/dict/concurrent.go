package dict

import (
	"math"
	"sync"
)

// ConcurrentDict 是使用分片锁的线程安全映射
type ConcurrentDict struct {
	table      []*shard
	count      int32  // 表示当前字典中的元素数量
	shardCount int  // 分片数
}

type shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

// 将 param 调整为大于等于 param 的最小的 2 的幂次方值
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// MakeConcurrent 使用给定的分片数量创建 ConcurrentDict
func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		count: 0,
		table: table,
		shardCount: shardCount,
	}
	return d;
}

const prime32 = uint32(16777619)

// 实现了 FNV-1a 哈希算法，用于将字符串转换成一个 32 位无符号整数哈希值
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 通过哈希值计算出对应的分片索引，确保哈希值均匀分布到不同的分片中
func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(dict.table))
	return (tableSize - 1) & hashCode
}

// 根据分片索引返回对应的分片
func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

