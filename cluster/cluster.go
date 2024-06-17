// cluster 包提供了对客户端透明的服务器端集群，你可以连接到集群中的任何节点来访问集群中的所有数据
package cluster

import (
	"sync"

	"github.com/CodingCaius/godis/datastruct/dict"
	"github.com/CodingCaius/godis/datastruct/set"
	"github.com/CodingCaius/godis/interface/database"
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/redis/parser"
	"github.com/openzipkin/zipkin-go/idgenerator"
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
	topology      topology

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

// 客户端工厂模式借口
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

//如果一个事务只涉及一个节点，则直接执行命令，不采用 TCC（Try-Confirm/Cancel）事务处理过程
var allowFastTransaction = true

