package idgenerator

/*
实现了一个基于 Twitter Snowflake 算法的唯一 ID 生成器。它通过时间戳、节点 ID 和序列号的组合，生成 64 位的唯一 ID。这个生成器具有以下特点：

时间有序性：生成的 ID 按时间顺序递增。
分布式唯一性：通过节点 ID 区分不同的生成器实例，确保在分布式系统中的唯一性。
高并发性：通过互斥锁保护并发访问，确保线程安全。
*/

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const (
	// epoch0 设置为 2010 年 11 月 4 日 01:42:54 UTC 的 twitter 雪花纪元（以毫秒为单位）
    //您可以自定义它来为您的应用程序设置不同的纪元。
	epoch0      int64 = 1288834974657
	// 序列号的最大值，用于防止序列号溢出  1023
	maxSequence int64 = -1 ^ (-1 << uint64(nodeLeft))
	// 时间戳在生成的 ID 中左移的位数
	timeLeft    uint8 = 22
	// 节点 ID 在生成的 ID 中左移的位数
	nodeLeft    uint8 = 10
	// 用于生成节点 ID 的掩码
	nodeMask    int64 = -1 ^ (-1 << uint64(timeLeft-nodeLeft))
)

// IDGenerator generates unique uint64 ID using snowflake algorithm
type IDGenerator struct {
	mu        *sync.Mutex
	lastStamp int64
	nodeID    int64
	sequence  int64
	epoch     time.Time
}

// MakeGenerator creates a new IDGenerator
func MakeGenerator(node string) *IDGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(node))
	nodeID := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	epoch := curTime.Add(time.Unix(epoch0/1000, (epoch0%1000)*1000000).Sub(curTime))

	return &IDGenerator{
		mu:        &sync.Mutex{},
		lastStamp: -1,
		nodeID:    nodeID,
		sequence:  1,
		epoch:     epoch,
	}
}

// NextID returns next unique ID
func (w *IDGenerator) NextID() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	timestamp := time.Since(w.epoch).Nanoseconds() / 1000000
	if timestamp < w.lastStamp {
		log.Fatal("can not generate id")
	}
	if w.lastStamp == timestamp {
		w.sequence = (w.sequence + 1) & maxSequence
		if w.sequence == 0 {
			for timestamp <= w.lastStamp {
				timestamp = time.Since(w.epoch).Nanoseconds() / 1000000
			}
		}
	} else {
		w.sequence = 0
	}
	w.lastStamp = timestamp
	id := (timestamp << timeLeft) | (w.nodeID << nodeLeft) | w.sequence
	//fmt.Printf("%d %d %d\n", timestamp, w.sequence, id)
	return id
}