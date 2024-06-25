package cluster

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const slotCount int = 16384

type raftState int

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	nodeFlagLeader uint32 = 1 << iota
	nodeFlagCandidate
	nodeFlagLearner
)

const (
	follower raftState = iota
	leader
	candidate
	learner
)

var stateNames = map[raftState]string{
	follower:  "follower",
	leader:    "leader",
	candidate: "candidate",
	learner:   "learner",
}

// 设置节点的状态
func (node *Node) setState(state raftState) {
	node.Flags &= ^uint32(0x7) // 清除当前状态
	switch state {
	case follower:
		break
	case leader:
		node.Flags |= nodeFlagLeader
	case candidate:
		node.Flags |= nodeFlagCandidate
	case learner:
		node.Flags |= nodeFlagLearner
	}
}

// 获取节点的 Raft 状态
func (node *Node) getState() raftState {
	if node.Flags&nodeFlagLeader > 0 {
		return leader
	}
	if node.Flags&nodeFlagCandidate > 0 {
		return candidate
	}
	if node.Flags&nodeFlagLearner > 0 {
		return learner
	}
	return follower
}

// 表示一个日志条目，通常用于分布式系统中的日志复制
type logEntry struct {
	Term  int // 日志条目所属的任期
	Index int // 日志条目的索引
	Event int // 日志条目的事件类型
	wg    *sync.WaitGroup // 用于同步操作
	// payload
	SlotIDs []uint32 // 表示相关的哈希槽 ID
	NodeID  string // 节点的 ID
	Addr    string // 节点的地址
}

// 将 logEntry 结构体序列化为 JSON 字节数组
func (e *logEntry) marshal() []byte {
	bin, _ := json.Marshal(e)
	return bin
}

// 将 JSON 字节数组反序列化为 logEntry 结构体
func (e *logEntry) unmarshal(bin []byte) error {
	err := json.Unmarshal(bin, e)
	if err != nil {
		return fmt.Errorf("illegal message: %v", err)
	}
	return nil
}
