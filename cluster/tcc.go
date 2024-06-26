package cluster

import (
	"strings"
	"sync"
	"time"

	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/logger"
)

// 一个映射，用于存储不同命令的预处理函数
// 相关key锁定后执行prepareFunc，并使用额外的逻辑来判断事务是否可以提交
// 例如，如果任何相关键已存在，prepareMSetNX将返回错误以阻止MSetNx事务提交
var prepareFuncMap = make(map[string]CmdFunc)

// 注册预处理函数，将命令名称（cmdName）和对应的预处理函数（fn）存储到 prepareFuncMap 中。命令名称被转换为小写以确保一致性
func registerPrepareFunc(cmdName string, fn CmdFunc) {
	prepareFuncMap[strings.ToLower(cmdName)] = fn
}

// Transaction 存储分布式事务的状态和数据
type Transaction struct {
	id      string   // transaction id
	cmdLine [][]byte // 事务中的命令行
	cluster *Cluster // 事务所属的集群
	conn    redis.Connection
	dbIndex int

	writeKeys  []string  // 事务中涉及的写操作的键
	readKeys   []string  // 事务中涉及的读操作的键
	keysLocked bool      // 标识键是否已被锁定
	undoLog    []CmdLine // 存储回滚操作的日志

	status int8        // 事务的状态
	mu     *sync.Mutex // 保护事务的并发访问
}

const (
	maxLockTime       = 3 * time.Second // 键的最大锁定时间，设置为3秒
	waitBeforeCleanTx = 2 * maxLockTime // 在清理事务之前的等待时间，设置为 2 * maxLockTime。

	createdStatus    = 0 //事务已创建
	preparedStatus   = 1 // 事务已准备好
	committedStatus  = 2 // 事务已提交
	rolledBackStatus = 3 // 事务已回滚
)

// 生成事务的任务键。它将事务ID（txID）前加上前缀 "tx:"，以生成唯一的任务键
func genTaskKey(txID string) string {
	return "tx:" + txID
}

// NewTransaction 创建一个新的分布式事务
func NewTransaction(cluster *Cluster, c redis.Connection, id string, cmdLine [][]byte) *Transaction {
	return &Transaction{
		id:      id,
		cmdLine: cmdLine,
		cluster: cluster,
		conn:    c,
		dbIndex: c.GetDBIndex(),
		status:  createdStatus,
		mu:      new(sync.Mutex),
	}
}

// 可重入
// 调用者应该持有 tx.mu
// 锁定事务中涉及的键（读写锁）
func (tx *Transaction) lockKeys() {
	if !tx.keysLocked {
		tx.cluster.db.RWLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = true
	}
}

// 解锁事务中涉及的键
func (tx *Transaction) unLockKeys() {
	if tx.keysLocked {
		tx.cluster.db.RWUnLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = false
	}
}

// t should contain Keys and ID field
// 用于准备事务
func (tx *Transaction) prepare() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	// 获取事务中涉及的写键和读键
	tx.writeKeys, tx.readKeys = database.GetRelatedKeys(tx.cmdLine)
	// lock writeKeys
	tx.lockKeys()

	// 确保所有写键和读键在集群中存在
	for _, key := range tx.writeKeys {
		err := tx.cluster.ensureKey(key)
		if err != nil {
			return err
		}
	}
	for _, key := range tx.readKeys {
		err := tx.cluster.ensureKey(key)
		if err != nil {
			return err
		}
	}
	// 构建回滚日志
	tx.undoLog = tx.cluster.db.GetUndoLogs(tx.dbIndex, tx.cmdLine)
	tx.status = preparedStatus
	// 生成任务键
	taskKey := genTaskKey(tx.id)
	// 使用 timewheel.Delay 方法设置一个延迟任务，如果在 maxLockTime 时间内事务仍处于 preparedStatus 状态，则回滚事务（调用 tx.rollbackWithLock 方法）
	timewheel.Delay(maxLockTime, taskKey, func() {
		if tx.status == preparedStatus { // rollback transaction uncommitted until expire
			logger.Info("abort transaction: " + tx.id)
			tx.mu.Lock()
			defer tx.mu.Unlock()
			_ = tx.rollbackWithLock()
		}
	})
	return nil
}
