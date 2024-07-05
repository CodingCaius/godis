package database

// 实现了一个功能齐全的 Redis 服务器，支持多数据库、持久化、复制、发布/订阅等功能。
// 它通过定义 Server 结构体和一系列方法，提供了对 Redis 命令的处理和数据库操作的支持

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/CodingCaius/godis/aof"
	"github.com/CodingCaius/godis/config"
	"github.com/CodingCaius/godis/interface/database"
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/logger"
	"github.com/CodingCaius/godis/lib/utils"
	"github.com/CodingCaius/godis/pubsub"
	"github.com/CodingCaius/godis/redis/protocol"
)

var godisVersion = "1.2.8"

// Server 是一个redis服务器，支持多数据库, rdb 加载，aof持久化, 复制等能力
type Server struct {
	dbSet []*atomic.Value // 一个包含多个数据库的切片，每个数据库是一个 *atomic.Value 类型的指针，指向 *DB

	// 处理发布/订阅
	hub *pubsub.Hub
	// 处理 AOF持久化
	persister *aof.Persister

	// for replication
	// 表示服务器的角色（主节点或从节点）
	role int32
	// 从节点的状态
	slaveStatus *slaveStatus
	// 主节点的状态
	masterStatus *masterStatus

	// 用于数据库键事件的回调函数
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback
}

// 检查给定的文件是否存在且不是一个目录
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

// NewStandaloneServer 创建一个独立的redis服务器，具有多数据库和所有其他功能
func NewStandaloneServer() *Server {
	server := &Server{}
	// 默认设置为 16 个数据库
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	// creat tmp dir
	err := os.MkdirAll(config.GetTmpDir(), os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("create tmp dir failed: %v", err))
	}
	// 创建并初始化数据库集合
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range server.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	// 创建一个发布订阅中心
	server.hub = pubsub.MakeHub()
	// 如果配置启用了 AOF 持久化，检查 AOF 文件是否存在，并创建 AOF 持久化处理器
	validAof := false
	if config.Properties.AppendOnly {
		validAof = fileExists(config.Properties.AppendFilename)
		aofHandler, err := NewPersister(server,
			config.Properties.AppendFilename, true, config.Properties.AppendFsync)
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofHandler)
	}
	if config.Properties.RDBFilename != "" && !validAof {
		// 如果配置中指定了 RDB 文件且没有有效的 AOF 文件，加载 RDB 文件
		err := server.loadRdbFile()
		if err != nil {
			logger.Error(err)
		}
	}
	// 初始化从节点状态和主节点状态，并启动复制定时任务
	server.slaveStatus = initReplSlaveStatus()
	server.initMaster()
	server.startReplCron()
	// 将服务器角色设置为主节点
	server.role = masterRole // The initialization process does not require atomicity
	return server
}

// 处理和执行客户端发送的命令。
// 它首先解析命令名称，然后根据命令名称执行相应的处理逻辑，包括处理特殊命令、只读从服务器、事务中不能执行的命令以及普通命令。通过使用 defer 和 recover，它还能够捕获和处理运行时错误，确保服务器的稳定性
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (server *Server) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	// ping
	if cmdName == "ping" {
		return Ping(c, cmdLine[1:])
	}
	// authenticate
	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	// info
	if cmdName == "info" {
		return Info(server, cmdLine[1:])
	}
	if cmdName == "dbsize" {
		return DbSize(c, server)
	}
	if cmdName == "slaveof" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot use slave of database within multi")
		}
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("SLAVEOF")
		}
		return server.execSlaveOf(c, cmdLine[1:])
	} else if cmdName == "command" {
		return execCommand(cmdLine[1:])
	}

	// read only slave
	role := atomic.LoadInt32(&server.role)
	if role == slaveRole && !c.IsMaster() {
		// only allow read only command, forbid all special commands except `auth` and `slaveof`
		if !isReadOnlyCommand(cmdName) {
			return protocol.MakeErrReply("READONLY You can't write against a read only slave.")
		}
	}

	// special commands which cannot execute within transaction
	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrReply("subscribe")
		}
		return pubsub.Subscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "publish" {
		return pubsub.Publish(server.hub, cmdLine[1:])
	} else if cmdName == "unsubscribe" {
		return pubsub.UnSubscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "bgrewriteaof" {
		if !config.Properties.AppendOnly {
			return protocol.MakeErrReply("AppendOnly is false, you can't rewrite aof file")
		}
		// aof.go imports router.go, router.go cannot import BGRewriteAOF from aof.go
		return BGRewriteAOF(server, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		if !config.Properties.AppendOnly {
			return protocol.MakeErrReply("AppendOnly is false, you can't rewrite aof file")
		}
		return RewriteAOF(server, cmdLine[1:])
	} else if cmdName == "flushall" {
		return server.flushAll()
	} else if cmdName == "flushdb" {
		if !validateArity(1, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		if c.InMultiState() {
			return protocol.MakeErrReply("ERR command 'FlushDB' cannot be used in MULTI")
		}
		return server.execFlushDB(c.GetDBIndex())
	} else if cmdName == "save" {
		return SaveRDB(server, cmdLine[1:])
	} else if cmdName == "bgsave" {
		return BGSaveRDB(server, cmdLine[1:])
	} else if cmdName == "select" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot select database within multi")
		}
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return execSelect(c, server, cmdLine[1:])
	} else if cmdName == "copy" {
		if len(cmdLine) < 3 {
			return protocol.MakeArgNumErrReply("copy")
		}
		return execCopy(server, c, cmdLine[1:])
	} else if cmdName == "replconf" {
		return server.execReplConf(c, cmdLine[1:])
	} else if cmdName == "psync" {
		return server.execPSync(c, cmdLine[1:])
	}
	// todo: support multi database transaction

	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(c, cmdLine)
}

// AfterClientClose 客户端关闭连接后进行一些清理
func (server *Server) AfterClientClose(c redis.Connection) {
	// 取消客户端 c 订阅的所有频道
	pubsub.UnsubscribeAll(server.hub, c)
}

// Close 优雅关闭数据库
func (server *Server) Close() {
	// 停止从节点状态
	server.slaveStatus.close()
	// 如果服务器有持久化机制（server.persister 不为 nil），则调用其 Close 方法关闭持久化机制
	if server.persister != nil {
		server.persister.Close()
	}
	// 停止主节点的相关操作
	server.stopMaster()
}

// 处理 SELECT 命令，用于选择数据库
func execSelect(c redis.Connection, mdb *Server, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}

// 清空指定的数据库
func (server *Server) execFlushDB(dbIndex int) redis.Reply {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, utils.ToCmdLine("FlushDB"))
	}
	return server.flushDB(dbIndex)
}

// flushDB flushes the selected database
func (server *Server) flushDB(dbIndex int) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	newDB := makeDB()
	server.loadDB(dbIndex, newDB)
	return &protocol.OkReply{}
}

func (server *Server) loadDB(dbIndex int, newDB *DB) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	oldDB := server.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof // inherit oldDB
	server.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
}

// flushAll flushes all databases.
func (server *Server) flushAll() redis.Reply {
	for i := range server.dbSet {
		server.flushDB(i)
	}
	if server.persister != nil {
		server.persister.SaveCmdLine(0, utils.ToCmdLine("FlushAll"))
	}
	return &protocol.OkReply{}
}

// selectDB returns the database with the given index, or an error if the index is out of range.
func (server *Server) selectDB(dbIndex int) (*DB, *protocol.StandardErrReply) {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR DB index is out of range")
	}
	return server.dbSet[dbIndex].Load().(*DB), nil
}

// MustSelectDB 与 selectDB 类似，但如果发生错误，则会出现恐慌
func (server *Server) mustSelectDB(dbIndex int) *DB {
	selectedDB, err := server.selectDB(dbIndex)
	if err != nil {
		panic(err)
	}
	return selectedDB
}

// ForEach 遍历指定数据库中的所有键
func (server *Server) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	server.mustSelectDB(dbIndex).ForEach(cb)
}

// GetEntity 返回指定键对应的数据实体
func (server *Server) GetEntity(dbIndex int, key string) (*database.DataEntity, bool) {
	return server.mustSelectDB(dbIndex).GetEntity(key)
}

// 获取指定键的过期时间
func (server *Server) GetExpiration(dbIndex int, key string) *time.Time {
	raw, ok := server.mustSelectDB(dbIndex).ttlMap.Get(key)
	if !ok {
		return nil
	}
	expireTime, _ := raw.(time.Time)
	return &expireTime
}

// ExecMulti 原子性和隔离性地执行多命令事务
func (server *Server) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	selectedDB, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectedDB.ExecMulti(conn, watching, cmdLines)
}

// RWLocks lock keys for writing and reading
func (server *Server) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (server *Server) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

// GetUndoLogs 返回回滚命令
func (server *Server) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return server.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

// ExecWithLock 执行带锁的普通命令，调用者应提供锁
func (server *Server) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	db, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return db.execWithLock(cmdLine)
}

// BGRewriteAOF 异步重写 AOF 文件
func BGRewriteAOF(db *Server, args [][]byte) redis.Reply {
	go db.persister.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// RewriteAOF 同步重写 AOF 文件，直到完成
func RewriteAOF(db *Server, args [][]byte) redis.Reply {
	err := db.persister.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// SaveRDB 同步保存 RDB 文件，直到完成
func SaveRDB(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	rdbFilename := config.Properties.RDBFilename
	if rdbFilename == "" {
		rdbFilename = "dump.rdb"
	}
	err := db.persister.GenerateRDB(rdbFilename)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// BGSaveRDB 异步保存 RDB 文件
func BGSaveRDB(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(err)
			}
		}()
		rdbFilename := config.Properties.RDBFilename
		if rdbFilename == "" {
			rdbFilename = "dump.rdb"
		}
		err := db.persister.GenerateRDB(rdbFilename)
		if err != nil {
			logger.Error(err)
		}
	}()
	return protocol.MakeStatusReply("Background saving started")
}

// GetDBSize 指定数据库的键数量和 TTL 键数量
func (server *Server) GetDBSize(dbIndex int) (int, int) {
	db := server.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

// 启动复制定时任务，每 10 秒执行一次
func (server *Server) startReplCron() {
	go func(mdb *Server) {
		ticker := time.Tick(time.Second * 10)
		for range ticker {
			mdb.slaveCron()
			mdb.masterCron()
		}
	}(server)
}

// GetAvgTTL 计算指定数据库中键的平均过期时间
func (server *Server) GetAvgTTL(dbIndex, randomKeyCount int) int64 {
	var ttlCount int64
	db := server.mustSelectDB(dbIndex)
	keys := db.data.RandomKeys(randomKeyCount)
	for _, k := range keys {
		t := time.Now()
		rawExpireTime, ok := db.ttlMap.Get(k)
		if !ok {
			continue
		}
		expireTime, _ := rawExpireTime.(time.Time)
		// if the key has already reached its expiration time during calculation, ignore it
		if expireTime.Sub(t).Microseconds() > 0 {
			ttlCount += expireTime.Sub(t).Microseconds()
		}
	}
	return ttlCount / int64(len(keys))
}

func (server *Server) SetKeyInsertedCallback(cb database.KeyEventCallback) {
	server.insertCallback = cb
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.insertCallback = cb
	}

}

func (server *Server) SetKeyDeletedCallback(cb database.KeyEventCallback) {
	server.deleteCallback = cb
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.deleteCallback = cb
	}
}
