package database

/*
[Server](https://github.com/HDT3213/godis/blob/master/database/database.go) 是接口 [DB](https://github.com/HDT3213/godis/blob/master/interface/database/db.go) 的一个实现。

[server.Handler](https://github.com/HDT3213/godis/blob/master/redis/server/server.go) 持有一个 Server 实例作为存储引擎，并通过 db.Exec 方法将命令行传递给 Server。

Server 是一个支持 `SELECT` 命令的多数据库引擎。除了多个数据库实例外，它还持有 pubsub.Hub 和 aof.Handler 用于发布订阅和 AOF 持久化。

Server.Exec 是 Server 的主要入口，它处理身份验证、发布订阅、aof 以及系统命令本身，并调用所选数据库的 Exec 函数执行其他命令。

[godis.DB.Exec](https://github.com/HDT3213/godis/blob/master/database/single_db.go) 自己处理事务控制命令（比如 watch、multi、exec），调用 DB.execNormalCommand 处理普通命令。普通命令是指读写有限 key 的命令，可以在事务内执行，支持回滚。例如 get、set、lpush 是普通命令，而 flushdb、keys 不是。

[registerCommand](https://github.com/HDT3213/godis/blob/master/database/router.go) 用于注册普通命令。一个普通的命令需要三个函数：

- ExecFunc：实际执行命令的函数，例如 [execHSet](https://github.com/HDT3213/godis/blob/master/database/hash.go)
- PrepareFunc 在 ExecFunc 之前执行，它分析命令行并返回锁的读/写键
- UndoFunc 仅在事务中调用，它生成撤消日志以防事务中需要回滚
*/