package database

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/CodingCaius/godis/aof"
	"github.com/CodingCaius/godis/config"
	"github.com/CodingCaius/godis/interface/database"
	"github.com/CodingCaius/godis/lib/logger"
)

var godisVersion = "1.2.8"

// Server 是一个redis服务器，支持多数据库, rdb 加载，aof持久化, 主从复制等能力
type Server struct {
	dbSet []*atomic.Value // 一个包含多个数据库的切片，每个数据库是一个 *atomic.Value 类型的指针，指向 *DB

	// 处理发布/订阅
	hub *pubsub.Hub
	// 处理 AOF持久化
	persister *aof.Persister

	// for replication
	// 表示服务器的角色（主节点或从节点）
	role         int32
	// 从节点的状态
	slaveStatus  *slaveStatus
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