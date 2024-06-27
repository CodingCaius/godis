package cluster

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/logger"
	"github.com/CodingCaius/godis/lib/timewheel"
	"github.com/CodingCaius/godis/redis/protocol"
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

func (tx *Transaction) rollbackWithLock() error {
	curStatus := tx.status

	if tx.status != curStatus { // ensure status not changed by other goroutine
		return fmt.Errorf("tx %s status changed", tx.id)
	}
	if tx.status == rolledBackStatus { // no need to rollback a rolled-back transaction
		return nil
	}
	tx.lockKeys()
	// 遍历事务的撤销日志（undo log），执行每个撤销命令
	for _, cmdLine := range tx.undoLog {
		tx.cluster.db.ExecWithLock(tx.conn, cmdLine)
	}
	tx.unLockKeys()
	tx.status = rolledBackStatus
	return nil
}

// cmdLine: Prepare id cmdName args...
func execPrepare(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'prepare' command")
	}
	txID := string(cmdLine[1])
	cmdName := strings.ToLower(string(cmdLine[2]))
	tx := NewTransaction(cluster, c, txID, cmdLine[2:])
	cluster.transactionMu.Lock()
	// 将事务对象存储到事务集合中
	cluster.transactions.Put(txID, tx)
	cluster.transactionMu.Unlock()
	err := tx.prepare()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	// 从预处理函数映射中查找对应的预处理函数
	prepareFunc, ok := prepareFuncMap[cmdName]
	if ok {
		return prepareFunc(cluster, c, cmdLine[2:])
	}
	return &protocol.OkReply{}
}

// execRollback rollbacks local transaction
func execRollback(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}
	txID := string(cmdLine[1])
	cluster.transactionMu.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.transactionMu.RUnlock()
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	tx.mu.Lock()
	defer tx.mu.Unlock()
	err := tx.rollbackWithLock()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	// clean transaction
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactionMu.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.transactionMu.Unlock()
	})
	return protocol.MakeIntReply(1)
}

// execCommit 当从协调者接收到 execCommit 命令时，作为工作节点提交本地事务
// 工作节点接收到提交命令时执行本地事务的提交操作，并在失败时进行回滚
func execCommit(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}
	txID := string(cmdLine[1])
	cluster.transactionMu.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.transactionMu.RUnlock()
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	tx.mu.Lock()
	defer tx.mu.Unlock()
	// 在持有锁的情况下执行事务命令
	result := cluster.db.ExecWithLock(c, tx.cmdLine)

	if protocol.IsErrorReply(result) {
		// failed
		err2 := tx.rollbackWithLock()
		return protocol.MakeErrReply(fmt.Sprintf("err occurs when rollback: %v, origin err: %s", err2, result))
	}
	// after committed
	tx.unLockKeys()
	tx.status = committedStatus
	// clean finished transaction
	// do not clean immediately, in case rollback
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactionMu.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.transactionMu.Unlock()
	})
	return result
}

// requestCommit在协调者节点向所有工作节点发送提交请求，并在任何一个节点提交失败时进行回滚操作
func requestCommit(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) ([]redis.Reply, protocol.ErrorReply) {
	var errReply protocol.ErrorReply
	txIDStr := strconv.FormatInt(txID, 10)
	// 初始化一个回复列表，用于存储各节点的回复
	respList := make([]redis.Reply, 0, len(groupMap))
	for node := range groupMap {
		// 向节点发送提交命令，并获取回复
		resp := cluster.relay(node, c, makeArgs("commit", txIDStr))
		if protocol.IsErrorReply(resp) {
			errReply = resp.(protocol.ErrorReply)
			break
		}
		respList = append(respList, resp)
	}
	if errReply != nil {
		requestRollback(cluster, c, txID, groupMap)
		return nil, errReply
	}
	return respList, nil
}


// groupMap: node -> keys
// 作为协调者（coordinator）向所有节点发送回滚请求
func requestRollback(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) {
	// 将事务ID从整数类型转换为字符串类型
	txIDStr := strconv.FormatInt(txID, 10)
	for node := range groupMap {
		cluster.relay(node, c, makeArgs("rollback", txIDStr))
	}
}