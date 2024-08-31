package cluster

//  Redis 集群中的一部分，
// 主要处理 PUBLISH、SUBSCRIBE 和 UNSUBSCRIBE 命令

import (
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/logger"
	"github.com/CodingCaius/godis/redis/protocol"
)

// 一个常量字符串，用于标识发布命令
const (
	relayPublish = "publish_"
)

// Publish broadcasts msg to all peers in cluster when receive publish command from client
// 处理 PUBLISH 命令，将消息广播到集群中的所有节点
func Publish(cluster *Cluster, c redis.Connection, cmdLine [][]byte) redis.Reply {
	var count int64 = 0
	results := cluster.broadcast(c, modifyCmd(cmdLine, relayPublish))
	for _, val := range results {
		if errReply, ok := val.(protocol.ErrorReply); ok {
			logger.Error("publish occurs error: " + errReply.Error())
		} else if intReply, ok := val.(*protocol.IntReply); ok {
			// 处理 PUBLISH 命令，将消息广播到集群中的所有节点
			count += intReply.Code
		}
	}
	return protocol.MakeIntReply(count)
}

// Subscribe puts the given connection into the given channel
// 处理 SUBSCRIBE 命令，将客户端连接加入到指定的频道
func Subscribe(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	// 让本地 db.hub 处理订阅
	return cluster.db.Exec(c, args)
}

// UnSubscribe removes the given connection from the given channel
func UnSubscribe(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.db.Exec(c, args) // let local db.hub handle subscribe
}
