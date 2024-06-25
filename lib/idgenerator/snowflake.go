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
	// 用于生成节点 ID 的掩码  4095  2^12 - 1
	nodeMask    int64 = -1 ^ (-1 << uint64(timeLeft-nodeLeft))
)

// IDGenerator使用雪花算法生成唯一的 uint64 ID
type IDGenerator struct {
	mu        *sync.Mutex
	lastStamp int64 // 上一次生成 ID 的时间戳
	nodeID    int64 // 节点 ID，用于区分不同的生成器实例
	sequence  int64 // 序列号，用于在同一毫秒内生成多个唯一 ID
	epoch     time.Time // 起始时间戳
}

// MakeGenerator 创建一个新的 IDGenerator
func MakeGenerator(node string) *IDGenerator {
	// 创建一个 FNV-1 64 位哈希函数实例
	fnv64 := fnv.New64()
	// 将节点字符串写入哈希函数
	_, _ = fnv64.Write([]byte(node))
	// 计算节点 ID，并使用掩码确保其在合法范围内
	nodeID := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	// 计算当前时间与起始时间戳的差值
	epoch := curTime.Add(time.Unix(epoch0/1000, (epoch0%1000)*1000000).Sub(curTime))

	return &IDGenerator{
		mu:        &sync.Mutex{},
		lastStamp: -1,
		nodeID:    nodeID,
		sequence:  1,
		epoch:     epoch,
	}
}

// NextID 返回下一个唯一 ID
// 时间戳部分： 42 位
// 节点 ID 部分： 10 位
// 序列号部分： 10 位
func (w *IDGenerator) NextID() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 计算当前时间戳，单位为毫秒
	timestamp := time.Since(w.epoch).Nanoseconds() / 1000000
	// 如果当前时间戳小于上一次生成 ID 的时间戳，说明系统时钟回退，无法生成 ID
	if timestamp < w.lastStamp {
		log.Fatal("can not generate id")
	}
	// 如果当前时间戳与上一次相同，增加序列号。
	if w.lastStamp == timestamp {
		w.sequence = (w.sequence + 1) & maxSequence
		// 如果序列号达到最大值，等待下一个毫秒。
		if w.sequence == 0 {
			for timestamp <= w.lastStamp {
				timestamp = time.Since(w.epoch).Nanoseconds() / 1000000
			}
		}
	} else {
		w.sequence = 0
	}
	w.lastStamp = timestamp
	// 生成唯一 ID，包含时间戳、节点 ID 和序列号
	id := (timestamp << timeLeft) | (w.nodeID << nodeLeft) | w.sequence
	//fmt.Printf("%d %d %d\n", timestamp, w.sequence, id)
	return id
}