package cluster

import (
	"strconv"

	"github.com/CodingCaius/godis/database"
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/utils"
	"github.com/CodingCaius/godis/redis/connection"
	"github.com/CodingCaius/godis/redis/protocol"
)

const relayMulti = "multi_"
const innerWatch = "watch_"

var relayMultiBytes = []byte(relayMulti)

// cmdLine == []string{"exec"}
// 处理 EXEC 命令，它用于执行之前通过 MULTI 命令开始的事务
func execMulti(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	// 首先检查连接是否处于事务状态，如果不是，返回错误
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	cmdLines := conn.GetQueuedCmdLine()

	// analysis related keys
	// 从事务队列中提取所有相关的键，包括写键和读键
	keys := make([]string, 0) // may contains duplicate
	for _, cl := range cmdLines {
		wKeys, rKeys := database.GetRelatedKeys(cl)
		keys = append(keys, wKeys...)
		keys = append(keys, rKeys...)
	}
	watching := conn.GetWatching()
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	// 将监视的键也加入到相关键列表中
	keys = append(keys, watchingKeys...)
	if len(keys) == 0 {
		// empty transaction or only `PING`s
		return cluster.db.ExecMulti(conn, watching, cmdLines)
	}
	// 根据键的哈希槽将键分组，确保所有键都在同一个哈希槽中
	groupMap := cluster.groupBy(keys)
	if len(groupMap) > 1 {
		return protocol.MakeErrReply("ERR MULTI commands transaction must within one slot in cluster mode")
	}
	var peer string
	// assert len(groupMap) == 1
	for p := range groupMap {
		peer = p
	}

	// out parser not support protocol.MultiRawReply, so we have to encode it
	if peer == cluster.self {
		for _, key := range keys {
			if errReply := cluster.ensureKey(key); errReply != nil {
				return errReply
			}
		}
		return cluster.db.ExecMulti(conn, watching, cmdLines)
	}
	return execMultiOnOtherNode(cluster, conn, peer, watching, cmdLines)
}

func execMultiOnOtherNode(cluster *Cluster, conn redis.Connection, peer string, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	defer func() {
		conn.ClearQueuedCmds()
		conn.SetMultiState(false)
	}()
	relayCmdLine := [][]byte{ // relay it to executing node
		relayMultiBytes,
	}
	// watching commands
	var watchingCmdLine = utils.ToCmdLine(innerWatch)
	for key, ver := range watching {
		verStr := strconv.FormatUint(uint64(ver), 10)
		watchingCmdLine = append(watchingCmdLine, []byte(key), []byte(verStr))
	}
	relayCmdLine = append(relayCmdLine, encodeCmdLine([]CmdLine{watchingCmdLine})...)
	relayCmdLine = append(relayCmdLine, encodeCmdLine(cmdLines)...)
	var rawRelayResult redis.Reply
	rawRelayResult = cluster.relay(peer, connection.NewFakeConn(), relayCmdLine)
	if protocol.IsErrorReply(rawRelayResult) {
		return rawRelayResult
	}
	_, ok := rawRelayResult.(*protocol.EmptyMultiBulkReply)
	if ok {
		return rawRelayResult
	}
	relayResult, ok := rawRelayResult.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("execute failed")
	}
	rep, err := parseEncodedMultiRawReply(relayResult.Args)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return rep
}

// execRelayedMulti execute relayed multi commands transaction
// cmdLine format: _multi watch-cmdLine base64ed-cmdLine
// result format: base64ed-protocol list
func execRelayedMulti(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("_exec")
	}
	decoded, err := parseEncodedMultiRawReply(cmdLine[1:])
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	var txCmdLines []CmdLine
	for _, rep := range decoded.Replies {
		mbr, ok := rep.(*protocol.MultiBulkReply)
		if !ok {
			return protocol.MakeErrReply("exec failed")
		}
		txCmdLines = append(txCmdLines, mbr.Args)
	}
	watching := make(map[string]uint32)
	watchCmdLine := txCmdLines[0] // format: watch_ key1 ver1 key2 ver2...
	for i := 2; i < len(watchCmdLine); i += 2 {
		key := string(watchCmdLine[i-1])
		verStr := string(watchCmdLine[i])
		ver, err := strconv.ParseUint(verStr, 10, 64)
		if err != nil {
			return protocol.MakeErrReply("watching command line failed")
		}
		watching[key] = uint32(ver)
	}
	rawResult := cluster.db.ExecMulti(conn, watching, txCmdLines[1:])
	_, ok := rawResult.(*protocol.EmptyMultiBulkReply)
	if ok {
		return rawResult
	}
	resultMBR, ok := rawResult.(*protocol.MultiRawReply)
	if !ok {
		return protocol.MakeErrReply("exec failed")
	}
	return encodeMultiRawReply(resultMBR)
}

func execWatch(cluster *Cluster, conn redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("watch")
	}
	args = args[1:]
	watching := conn.GetWatching()
	for _, bkey := range args {
		key := string(bkey)
		err := cluster.ensureKey(key)
		if err != nil {
			return err
		}
		result := cluster.relayByKey(key, conn, utils.ToCmdLine("GetVer", key))
		if protocol.IsErrorReply(result) {
			return result
		}
		intResult, ok := result.(*protocol.IntReply)
		if !ok {
			return protocol.MakeErrReply("get version failed")
		}
		watching[key] = uint32(intResult.Code)
	}
	return protocol.MakeOkReply()
}
