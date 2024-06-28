package database
// 包database是一个兼容redis接口的内存数据库实现
// 这里是数据库接口的具体实现

// 实现了一个内存数据库，提供了与Redis兼容的接口。
// 它支持基本的数据存储和检索功能，并提供了事务控制、过期时间管理、版本管理和并发控制等高级功能。

import (
	"strings"

	"github.com/CodingCaius/godis/datastruct/dict"
	"github.com/CodingCaius/godis/interface/database"
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/redis/protocol"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 16
)

type DB struct {
	index int
	// key -> DataEntity
	data *dict.ConcurrentDict
	// key -> expireTime (time.Time)
	ttlMap *dict.ConcurrentDict
	// key -> version(uint32)
	versionMap *dict.ConcurrentDict

	// addaof is used to add command to aof
	addAof func(CmdLine)

	// callbacks
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback
}

// Exec Func 是命令执行器的接口
// args 不包含命令行
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc 在将命令排队为“multi”时分析命令行
// 返回相关的写入键和读取键
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine是[][]byte的别名，代表命令行
type CmdLine = [][]byte

// UndoFunc 返回给定命令行的撤消日志
// undo时从头到尾执行
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// makeDB 创建数据库实例
func makeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// makeBasicDB 创建仅具有基本能力的数据库实例
func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// Exec 在一个数据库内执行命令
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	// 根据命令类型区分处理事务控制命令和普通命令
	// 对于事务控制命令，它会根据具体命令调用相应的处理函数，并处理参数数量错误的情况
	// 对于普通命令，如果连接在事务状态中，会将命令加入事务队列，否则直接执行命令
	cmdName := strings.ToLower(string(cmdLine[0]))

	// 处理事务控制命令
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName) // 如果参数数量不对，返回错误
		}
		return StartMulti(c) // 开始事务
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName) // 如果参数数量不对，返回错误
		}
		return DiscardMulti(c) // 丢弃事务
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName) // 如果参数数量不对，返回错误
		}
		return execMulti(db, c) // 执行事务
	} else if cmdName == "watch" {
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName) // 如果参数数量不对，返回错误
		}
		return Watch(db, c, cmdLine[1:]) // 监视键
	}

	// 如果连接在事务状态中，将命令加入队列
	if c != nil && c.InMultiState() {
		return EnqueueCmd(c, cmdLine)
	}

	// 执行普通命令
	return db.execNormalCommand(cmdLine)
}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	// 将命令名称转换为小写
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 从命令表中查找命令
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	// 验证命令参数数量
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	// 准备命令
	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	// 添加版本控制
	db.addVersion(write...)
	// 对读写键进行锁定
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	// 获取并执行命令的执行函数
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// execWithLock 执行普通命令，调用者应该提供锁
func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// 验证命令的参数数量是否正确
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- 增删改查 ----- */
// 通过使用带锁的方式，这些操作可以在多线程环境中安全地执行

// GetEntity 获取与给定键绑定的数据实体
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	ret := db.data.PutWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExistsWithLock(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	ret := db.data.PutIfAbsentWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	raw, deleted := db.data.RemoveWithLock(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
	if cb := db.deleteCallback; cb != nil {
		var entity *database.DataEntity
		if deleted > 0 {
			entity = raw.(*database.DataEntity)
		}
		cb(db.index, key, entity)
	}
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.GetWithLock(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}