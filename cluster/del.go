package cluster

/*

实现了一个分布式 Redis 集群中的 DEL 命令，
该命令用于原子性地删除给定的键。
由于这些键可能分布在不同的节点上，
因此需要使用两阶段提交（2PC）协议来确保操作的原子性

*/

import (
	"strconv"

	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/redis/protocol"
)

// Del atomically removes given writeKeys from cluster, writeKeys can be distributed on any node
// if the given writeKeys are distributed on different node, Del will use try-commit-catch to remove them
func Del(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	// 参数检查
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}
	// 将参数中的键提取出来，存储在 keys 切片中
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	// 将键按所属节点分组，存储在 groupMap 中
	groupMap := cluster.groupBy(keys)
	// 如果所有键都在同一个节点上，并且允许快速事务处理，则直接将 DEL 命令转发到该节点
	if len(groupMap) == 1 && allowFastTransaction { // do fast
		for peer, group := range groupMap { // only one peerKeys
			return cluster.relay(peer, c, makeArgs("Del_", group...))
		}
	}
	// 如果键分布在多个节点上，则需要使用两阶段提交协议
	// prepare
	// 生成一个事务 ID，并将其转换为字符串。
	// 然后，向每个节点发送 Prepare 请求，包含事务 ID 和要删除的键。
	// 如果任何一个节点返回错误，则标记为需要回滚
	var errReply redis.Reply
	txID := cluster.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	rollback := false
	for peer, peerKeys := range groupMap {
		peerArgs := []string{txIDStr, "DEL"}
		peerArgs = append(peerArgs, peerKeys...)
		var resp redis.Reply
		resp = cluster.relay(peer, c, makeArgs("Prepare", peerArgs...))
		if protocol.IsErrorReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}
	// 提交或回滚阶段
	// 如果准备阶段没有错误，则向每个节点发送 Commit 请求，并收集响应。如果有任何错误，则标记为需要回滚
	var respList []redis.Reply
	if rollback {
		// rollback
		requestRollback(cluster, c, txID, groupMap)
	} else {
		// commit
		respList, errReply = requestCommit(cluster, c, txID, groupMap)
		if errReply != nil {
			rollback = true
		}
	}
	// 如果提交成功，则计算删除的键的总数并返回。
	// 如果回滚，则返回错误回复
	if !rollback {
		var deleted int64 = 0
		for _, resp := range respList {
			intResp := resp.(*protocol.IntReply)
			deleted += intResp.Code
		}
		return protocol.MakeIntReply(int64(deleted))
	}
	return errReply
}
