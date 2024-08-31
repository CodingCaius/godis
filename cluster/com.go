package cluster

// 涉及命令的转发、广播和键的迁移

import (
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/utils"
	"github.com/CodingCaius/godis/redis/connection"
	"github.com/CodingCaius/godis/redis/protocol"
)

// relay function relays command to peer or calls cluster.Exec
// 将命令转发到指定的节点。如果 peerId 是当前节点（cluster.self），则直接在本地执行命令；否则，将命令转发到指定的节点
func (cluster *Cluster) relay(peerId string, c redis.Connection, cmdLine [][]byte) redis.Reply {
	// use a variable to allow injecting stub for testing, see defaultRelayImpl
	if peerId == cluster.self {
		// to self db
		return cluster.Exec(c, cmdLine)
	}
	// peerId is peer.Addr
	// 获取目标节点的客户端连接
	cli, err := cluster.clientFactory.GetPeerClient(peerId)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	// 确保客户端连接在函数结束时被正确归还
	defer func() {
		_ = cluster.clientFactory.ReturnPeerClient(peerId, cli)
	}()
	//发送命令到目标节点
	return cli.Send(cmdLine)
}

// relayByKey 根据给定的 routeKey 确定目标节点，并将命令转发到该节点
func (cluster *Cluster) relayByKey(routeKey string, c redis.Connection, args [][]byte) redis.Reply {
	slotId := getSlot(routeKey)
	peer := cluster.pickNode(slotId)
	return cluster.relay(peer.ID, c, args)
}

// broadcast 将命令广播到集群中的所有节点，并收集每个节点的执行结果
func (cluster *Cluster) broadcast(c redis.Connection, args [][]byte) map[string]redis.Reply {
	result := make(map[string]redis.Reply)
	for _, node := range cluster.topology.GetNodes() {
		reply := cluster.relay(node.ID, c, args)
		result[node.Addr] = reply
	}
	return result
}

// ensureKey will migrate key to current node if the key is in a slot migrating to current node
// invoker should provide with locks of key
// 确保某个键已经被迁移到当前节点。如果键在迁移过程中，则从旧节点获取键的数据并将其导入到当前节点
func (cluster *Cluster) ensureKey(key string) protocol.ErrorReply {
	slotId := getSlot(key)
	cluster.slotMu.RLock()
	slot := cluster.slots[slotId]
	cluster.slotMu.RUnlock()
	// 如果槽为空或槽状态不是 slotStateImporting，或者键已经被导入，则直接返回
	if slot == nil {
		return nil
	}
	if slot.state != slotStateImporting || slot.importedKeys.Has(key) {
		return nil
	}
	// 使用 relay 方法从旧节点获取键的数据。如果获取失败或返回的数据格式不正确，则返回错误
	resp := cluster.relay(slot.oldNodeID, connection.NewFakeConn(), utils.ToCmdLine("DumpKey_", key))
	if protocol.IsErrorReply(resp) {
		return resp.(protocol.ErrorReply)
	}
	if protocol.IsEmptyMultiBulkReply(resp) {
		slot.importedKeys.Add(key)
		return nil
	}
	dumpResp := resp.(*protocol.MultiBulkReply)
	if len(dumpResp.Args) != 2 {
		return protocol.MakeErrReply("illegal dump key response")
	}
	// 使用 CopyTo 命令将键的数据导入当前节点。如果导入失败，则返回错误。成功导入后，将键添加到 importedKeys 集合中
	resp = cluster.db.Exec(connection.NewFakeConn(), [][]byte{
		[]byte("CopyTo"), []byte(key), dumpResp.Args[0], dumpResp.Args[1],
	})
	if protocol.IsErrorReply(resp) {
		return resp.(protocol.ErrorReply)
	}
	slot.importedKeys.Add(key)
	return nil
}

// 在调用 ensureKey 方法之前，先对键进行读写锁定，以确保线程安全
func (cluster *Cluster) ensureKeyWithoutLock(key string) protocol.ErrorReply {
	cluster.db.RWLocks(0, []string{key}, nil)
	defer cluster.db.RWUnLocks(0, []string{key}, nil)
	return cluster.ensureKey(key)
}
