package cluster

/*
定义了一个用于管理 Redis 集群的包 cluster，其中包含了哈希槽（Slot）和节点（Node）的结构体，以及用于管理集群拓扑结构的接口 topology。
通过这些结构体和方法，可以实现对 Redis 集群中节点和哈希槽的管理和操作
*/

import (
	"hash/crc32"
	"strings"
	"time"

	"github.com/CodingCaius/godis/redis/protocol"
)

// Slot 代表一个哈希槽，用于集群内部消息
type Slot struct {
	// ID is uint between 0 and 16383
	ID uint32
	// 当前持有该哈希槽的节点的 ID。
	// 如果该哈希槽正在迁移中，NodeID 是目标节点的 ID
	NodeID string
	// Flags 存储哈希槽的更多信息（例如状态标志）
	Flags uint32
}

// getPartitionKey 于提取键中的哈希标签
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

// 计算给定键的哈希槽 ID
func getSlot(key string) uint32 {
	partitionKey := getPartitionKey(key)
	// 使用 crc32.ChecksumIEEE 计算哈希标签的 CRC32 校验和，并对哈希槽总数（slotCount）取模，得到哈希槽 ID
	return crc32.ChecksumIEEE([]byte(partitionKey)) % uint32(slotCount)
}

// Node 表示一个节点及其持有的哈希槽，用于集群内部消息传递
type Node struct {
	ID        string // 节点的唯一标识
	Addr      string // 节点的地址
	Slots     []*Slot // 节点持有的哈希槽列表，按哈希槽 ID 升序排列
	Flags     uint32 // 存储节点的更多信息（例如状态标志）
	lastHeard time.Time // 记录节点最后一次通信的时间
}

// 用于管理集群的拓扑结构
type topology interface {
	GetSelfNodeID() string // 返回当前节点的 ID
	GetNodes() []*Node // 返回集群中所有节点的副本
	GetNode(nodeID string) *Node // 根据节点 ID 返回对应的节点
	GetSlots() []*Slot //  返回集群中所有哈希槽的副本
	StartAsSeed(addr string) protocol.ErrorReply // 使当前节点作为种子节点启动
	SetSlot(slotIDs []uint32, newNodeID string) protocol.ErrorReply // 设置指定哈希槽的持有节点
	LoadConfigFile() protocol.ErrorReply // 加载配置文件
	Join(seed string) protocol.ErrorReply // 使当前节点加入到指定的种子节点
	Close() error //关闭当前节点
}
