package cluster

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/CodingCaius/godis/datastruct/lock"
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
	Term  int             // 日志条目所属的任期
	Index int             // 日志条目的索引
	Event int             // 日志条目的事件类型
	wg    *sync.WaitGroup // 用于同步操作
	// payload
	SlotIDs []uint32 // 表示相关的哈希槽 ID
	NodeID  string   // 节点的 ID
	Addr    string   // 节点的地址
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

type Raft struct {
	cluster        *Cluster // 指向包含所有节点的集群信息的指针
	mu             sync.RWMutex
	selfNodeID     string // 当前节点的唯一标识符
	slots          []*Slot
	leaderId       string           // 当前集群中的领导者节点的唯一标识符
	nodes          map[string]*Node // 存储集群中的所有节点信息
	log            []*logEntry      // log index begin from 0
	baseIndex      int              // baseIndex + 1 == log[0].Index, it can be considered as the previous log index
	baseTerm       int              // 基准任期，表示前一个日志条目的任期
	state          raftState
	term           int
	votedFor       string          // 在当前任期内投票支持的候选人节点ID
	voteCount      int             // 当前任期内收到的选票数量（用于选举）
	committedIndex int             // 最后一个已经提交的日志条目的索引
	proposedIndex  int             // 最后一个提议（但可能尚未提交）的日志条目的索引
	heartbeatChan  chan *heartbeat // 一个通道，用于接收心跳消息（心跳机制是Raft算法的一部分，用于Leader与Follower之间的通信）
	persistFile    string          // 用于持久化Raft状态的文件名
	electionAlarm  time.Time       // 用于选举超时检测
	closeChan      chan struct{}   // 用于关闭Raft节点的通道
	closed         bool            // 表示节点是否已经关闭

	//  leader 特有字段
	nodeIndexMap map[string]*nodeStatus // 一个映射表，键为节点的唯一标识符，值为 nodeStatus 结构体指针，存储每个节点的日志接收状态
	nodeLock     *lock.Locks            // 用于保护对节点状态访问的锁
}

// 创建并初始化一个新的 Raft 实例。它接受两个参数：集群信息和持久化文件名，并返回一个初始化后的 Raft 指针
func newRaft(cluster *Cluster, persistFilename string) *Raft {
	return &Raft{
		cluster:     cluster,
		persistFile: persistFilename,
		closeChan:   make(chan struct{}),
	}
}

type heartbeat struct {
	sender   string      // 发送心跳消息的节点ID
	term     int         // 发送心跳消息时的任期
	entries  []*logEntry // 附加的日志条目数组
	commitTo int         // 发送心跳消息时的已提交日志条目的索引
}

type nodeStatus struct {
	receivedIndex int // 节点已接收到的日志条目的索引（未提交的索引）
}

// 返回当前Raft集群中的所有节点
func (raft *Raft) GetNodes() []*Node {
	raft.mu.RLock()
	defer raft.mu.RUnlock()
	result := make([]*Node, 0, len(raft.nodes))
	for _, v := range raft.nodes {
		result = append(result, v)
	}
	return result
}

func (raft *Raft) GetNode(nodeID string) *Node {
	raft.mu.RLock()
	defer raft.mu.RUnlock()
	return raft.nodes[nodeID]
}

// 返回从 beg 到 end 范围内的日志条目
func (raft *Raft) getLogEntries(beg, end int) []*logEntry {
	if beg <= raft.baseIndex || end > raft.baseIndex+len(raft.log)+1 {
		return nil
	}
	i := beg - raft.baseIndex - 1
	j := end - raft.baseIndex - 1
	return raft.log[i:j]
}

// 返回从 beg 开始的所有日志条目
func (raft *Raft) getLogEntriesFrom(beg int) []*logEntry {
	if beg <= raft.baseIndex {
		return nil
	}
	i := beg - raft.baseIndex - 1
	return raft.log[i:]
}

// 返回指定 idx 的单个日志条目
func (raft *Raft) getLogEntry(idx int) *logEntry {
	if idx < raft.baseIndex || idx >= raft.baseIndex+len(raft.log) {
		return nil
	}
	return raft.log[idx-raft.baseIndex]
}

// 初始化 Raft 实例的日志
func (raft *Raft) initLog(baseTerm, baseIndex int, entries []*logEntry) {
	raft.baseIndex = baseIndex
	raft.baseTerm = baseTerm
	raft.log = entries
}

// 定义了选举超时的最大值和最小值（以毫秒为单位）
const (
	electionTimeoutMaxMs = 4000
	electionTimeoutMinMs = 2800
)

// 生成一个位于 from 和 to 之间的随机整数
func randRange(from, to int) int {
	return rand.Intn(to-from) + from
}

// nextElectionAlarm 生成一个带有随机性的正常选举超时
func nextElectionAlarm() time.Time {
	return time.Now().Add(time.Duration(randRange(electionTimeoutMinMs, electionTimeoutMaxMs)) * time.Millisecond)
}

// 比较两个日志条目的索引
func compareLogIndex(term1, index1, term2, index2 int) int {
	if term1 != term2 {
		return term1 - term2
	}
	return index1 - index2
}

// 将 Cluster 实例的 topology 属性转换为 Raft 类型
// 用于从 Cluster 实例中获取 Raft 实例
func (cluster *Cluster) asRaft() *Raft {
	return cluster.topology.(*Raft)
}

