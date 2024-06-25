// cluster 包提供了对客户端透明的服务器端集群，你可以连接到集群中的任何节点来访问集群中的所有数据
package cluster

import (
	"fmt"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/CodingCaius/godis/config"
	database2 "github.com/CodingCaius/godis/database"
	"github.com/CodingCaius/godis/datastruct/dict"
	"github.com/CodingCaius/godis/datastruct/set"
	"github.com/CodingCaius/godis/interface/database"
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/logger"
	"github.com/CodingCaius/godis/redis/parser"
	"github.com/CodingCaius/godis/redis/protocol"
	"github.com/hdt3213/rdb/core"
)

// cluster代表godis集群的一个节点
// 它持有部分数据并协调其他节点完成交易
type Cluster struct {
	self string //当前节点的 ID
	addr string // 当前节点的地址
	db   database.DBEngine
	// 一个字典，存储正在进行的事务，键是事务 ID，值是事务对象。
	transactions  *dict.SimpleDict // id -> Transaction
	transactionMu sync.RWMutex
	// 集群拓扑，用于管理集群节点的信息（如使用 Raft 协议）
	topology topology

	slotMu sync.RWMutex
	// 一个映射，键是槽位（slot）的 ID，值是 hostSlot 对象，表示当前节点负责的槽位
	slots map[uint32]*hostSlot
	// ID 生成器实例，用于生成唯一的事务 ID
	idGenerator *idgenerator.IDGenerator

	// 客户端工厂，用于创建和管理与其他节点的连接
	clientFactory clientFactory
}

// 用来与其他节点通信的客户端
type peerClient interface {
	Send(args [][]byte) redis.Reply // 用于发送命令并返回回复
}

// 用来与其他节点的流式通信
type peerStream interface {
	Stream() <-chan *parser.Payload //返回一个接收 Payload 对象的通道
	Close() error
}

// 客户端工厂模式接口
type clientFactory interface {
	// 获取与指定节点通信的客户端
	GetPeerClient(peerAddr string) (peerClient, error)
	// 归还客户端到工厂
	ReturnPeerClient(peerAddr string, peerClient peerClient) error
	// 创建与指定节点的流式通信
	NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error)
	// 关闭工厂，释放资源
	Close() error
}

const (
	slotStateHost      = iota // 表示当前节点是槽位的主机
	slotStateImporting        // 表示当前节点正在导入槽位
	slotStateMovingOut        // 表示当前节点正在迁出槽位
)

// hostSlot 存储当前节点托管的主机的状态
// 表示当前节点负责的槽位状态
type hostSlot struct {
	// 槽位状态，可以是 slotStateHost、slotStateImporting 或 slotStateMovingOut
	state uint32
	// 读写锁，用于保护槽位状态的并发访问
	mu sync.RWMutex
	// OldNodeID 是要移出此槽的节点
	//仅在槽导入期间有效
	oldNodeID string
	//newNodeID 是导入此槽的节点
	//仅在槽位移出期间有效
	newNodeID string

	/*importedKeys 在迁移过程中存储导入的密钥
	*当该slot迁移时，如果importedKeys没有给定的key，则当前节点将在执行命令之前导入key
	*
	*在迁移槽中，旧节点上的槽是不可变的，我们只删除新节点中的一个键。
	*因此，我们必须区分未迁移的key和已删除的key。
	*即使某个key被删除了，它仍然存在于importedKeys中，因此我们可以区分未迁移和已删除。
	 */
	importedKeys *set.Set
	//keys 存储该槽中的所有密钥
	//Cluster.makeInsertCallback 和 Cluster.makeDeleteCallback 将使键保持最新状态
	keys *set.Set
}

// 如果一个事务只涉及一个节点，则直接执行命令，不采用 TCC（Try-Confirm/Cancel）事务处理过程
var allowFastTransaction = true

// MakeCluster 创建并启动集群的节点
func MakeCluster() *Cluster {
	cluster := &Cluster{
		self:          config.Properties.Self,
		addr:          config.Properties.AnnounceAddress(),
		db:            database2.NewStandaloneServer(),
		transactions:  dict.MakeSimple(),
		idGenerator:   idgenerator.MakeGenerator(config.Properties.Self),
		clientFactory: newDefaultClientFactory(),
	}
	// 设置拓扑持久化文件路径，用于存储集群拓扑信息
	topologyPersistFile := path.Join(config.Properties.Dir, config.Properties.ClusterConfigFile)
	// 使用 Raft 协议初始化集群拓扑
	cluster.topology = newRaft(cluster, topologyPersistFile)
	// 设置键插入和删除时的回调函数，用于更新集群节点的 slot 状态
	cluster.db.SetKeyInsertedCallback(cluster.makeInsertCallback())
	cluster.db.SetKeyDeletedCallback(cluster.makeDeleteCallback())
	cluster.slots = make(map[uint32]*hostSlot)
	var err error
	if topologyPersistFile != "" && fileExists(topologyPersistFile) {
		err = cluster.LoadConfig()
	} else if config.Properties.ClusterAsSeed {
		err = cluster.startAsSeed(config.Properties.AnnounceAddress())
	} else {
		err = cluster.Join(config.Properties.ClusterSeed)
	}
	/*
			根据不同情况处理集群的启动：

		如果拓扑持久化文件存在，加载拓扑配置。
		如果当前节点配置为种子节点，启动为种子节点。
		否则，加入指定的集群种子节点。
	*/
	if err != nil {
		panic(err)
	}
	return cluster
}

// CmdFunc 代表redis命令的处理程序
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

// Close 停止当前集群节点并进行必要的清理工作
func (cluster *Cluster) Close() {
	_ = cluster.topology.Close()
	cluster.db.Close()
	cluster.clientFactory.Close()
}

// 用于检查客户端连接是否已通过身份验证
func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// Exec 用于在集群中执行命令
// Exec 函数是集群节点执行命令的入口，负责：
// 解析命令名称并处理特定命令。
// 检查身份验证。
// 处理事务命令。
// 查找并执行命令处理函数
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	// 获取命令名称并转换为小写
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 处理特定命令:
	// 返回集群信息
	if cmdName == "info" {
		if ser, ok := cluster.db.(*database2.Server); ok {
			return database2.Info(ser, cmdLine[1:])
		}
	}
	// 进行身份验证
	if cmdName == "auth" {
		return database2.Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	// 返回数据库大小。
	if cmdName == "dbsize" {
		if ser, ok := cluster.db.(*database2.Server); ok {
			return database2.DbSize(c, ser)
		}
	}

	// 开始事务
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.StartMulti(c)
		// 放弃事务
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.DiscardMulti(c)
		// 执行事务。
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(cluster, c, nil)
		// 返回错误，因为集群模式不支持 select 命令
	} else if cmdName == "select" {
		return protocol.MakeErrReply("select not supported in cluster")
	}
	// 如果客户端处于事务状态，则将命令加入事务队列
	if c != nil && c.InMultiState() {
		return database2.EnqueueCmd(c, cmdLine)
	}
	//查找命令路由表中的处理函数并执行。如果命令不支持或未知，返回错误回复。
	cmdFunc, ok := router[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdLine)
	return
}

// AfterClientClose 客户端关闭连接后进行一些清理
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}

func (cluster *Cluster) LoadRDB(dec *core.Decoder) error {
	return cluster.db.LoadRDB(dec)
}

func (cluster *Cluster) makeInsertCallback() database.KeyEventCallback {
	return func(dbIndex int, key string, entity *database.DataEntity) {
		// 根据键值 key 计算出该键所属的 slot ID
		slotId := getSlot(key)
		// 使用读锁读取 cluster.slots 映射，获取对应 slot ID 的 slot 信息
		cluster.slotMu.RLock()
		slot, ok := cluster.slots[slotId]
		cluster.slotMu.RUnlock()
		// 如果 slot 存在，则加锁更新该 slot 的键集合，将新插入的键添加到集合中
		if ok {
			slot.mu.Lock()
			defer slot.mu.Unlock()
			slot.keys.Add(key)
		}
	}
}

func (cluster *Cluster) makeDeleteCallback() database.KeyEventCallback {
	return func(dbIndex int, key string, entity *database.DataEntity) {
		slotId := getSlot(key)
		cluster.slotMu.RLock()
		slot, ok := cluster.slots[slotId]
		cluster.slotMu.RUnlock()
		// As long as the command is executed, we should update slot.keys regardless of slot.state
		if ok {
			slot.mu.Lock()
			defer slot.mu.Unlock()
			slot.keys.Remove(key)
		}
	}
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
