package pubsub

// 实现了一个基本的发布/订阅系统，支持客户端订阅和取消订阅频道，以及在频道上发布消息。
// 通过使用锁机制，确保了并发操作的安全性

import (
	"strconv"

	"github.com/CodingCaius/godis/datastruct/list"
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/redis/protocol"
)

var (
	_subscribe         = "subscribe"
	_unsubscribe       = "unsubscribe"
	messageBytes       = []byte("message")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n") // 取消订阅时没有订阅的响应消息，使用 Redis 协议格式
)

// 使用 Redis 协议格式构建一个消息
func makeMsg(t string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + protocol.CRLF + t + protocol.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 10) + protocol.CRLF + channel + protocol.CRLF +
		":" + strconv.FormatInt(code, 10) + protocol.CRLF)
}

/*
 * invoker should lock channel
 * return: is new subscribed
 */
 // 将客户端订阅到指定的频道，并更新 hub 中的订阅者列表
 // bool：表示是否是新的订阅。如果客户端已经订阅了该频道，则返回 false；否则返回 true
func subscribe0(hub *Hub, channel string, client redis.Connection) bool {
	client.Subscribe(channel)

	// add into hub.subs
	raw, ok := hub.subs.Get(channel)
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.subs.Put(channel, subscribers)
	}
	// 检查订阅者列表中是否已经包含该客户端
	if subscribers.Contains(func(a interface{}) bool {
		return a == client
	}) {
		return false
	}
	// 将客户端添加到订阅者列表中
	subscribers.Add(client)
	return true
}

/*
 * invoker should lock channel
 * return: is actually un-subscribe
 */
 // 将客户端取消订阅指定的频道，并更新 hub 中的订阅者列表。
func unsubscribe0(hub *Hub, channel string, client redis.Connection) bool {
	client.UnSubscribe(channel)

	// remove from hub.subs
	raw, ok := hub.subs.Get(channel)
	if ok {
		subscribers, _ := raw.(*list.LinkedList)
		subscribers.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, client)
		})

		if subscribers.Len() == 0 {
			// clean
			hub.subs.Remove(channel)
		}
		return true
	}
	return false
}

// Subscribe 将给定的客户端连接订阅到指定的频道
func Subscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	// 每个字符串表示一个频道名称
	for i, b := range args {
		channels[i] = string(b)
	}

	// 锁定所有要订阅的频道
	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		if subscribe0(hub, channel, c) {
			// 如果订阅成功（即是新的订阅），则向客户端发送订阅确认消息
			_, _ = c.Write(makeMsg(_subscribe, channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

// UnsubscribeAll 将给定的客户端连接从所有订阅的频道中取消订阅
func UnsubscribeAll(hub *Hub, c redis.Connection) {
	// 获取客户端当前订阅的所有频道
	channels := c.GetChannels()

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		unsubscribe0(hub, channel, c)
	}

}

// UnSubscribe 将给定的客户端连接从指定的频道中取消订阅。如果没有指定频道，则取消订阅客户端当前订阅的所有频道
func UnSubscribe(db *Hub, c redis.Connection, args [][]byte) redis.Reply {
	var channels []string
	if len(args) > 0 {
		channels = make([]string, len(args))
		for i, b := range args {
			channels[i] = string(b)
		}
	} else {
		channels = c.GetChannels()
	}

	db.subsLocker.Locks(channels...)
	defer db.subsLocker.UnLocks(channels...)

	if len(channels) == 0 {
		_, _ = c.Write(unSubscribeNothing)
		return &protocol.NoReply{}
	}

	for _, channel := range channels {
		if unsubscribe0(db, channel, c) {
			_, _ = c.Write(makeMsg(_unsubscribe, channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

// Publish 将消息发送到指定的频道，所有订阅该频道的客户端都会收到这条消息
func Publish(hub *Hub, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return &protocol.ArgNumErrReply{Cmd: "publish"}
	}
	// 将 args 中的第一个元素转换为频道名称，第二个元素作为消息内容
	channel := string(args[0])
	message := args[1]
	//  锁定要发布消息的频道，以确保并发安全
	hub.subsLocker.Lock(channel)
	defer hub.subsLocker.UnLock(channel)

	// 获取指定频道的订阅者列表。如果频道不存在，则返回一个整数回复对象，表示成功发送消息的订阅者数量为 0。
	raw, ok := hub.subs.Get(channel)
	if !ok {
		return protocol.MakeIntReply(0)
	}
	subscribers, _ := raw.(*list.LinkedList)
	// 遍历订阅者列表，将消息发送给每个订阅该频道的客户端
	subscribers.ForEach(func(i int, c interface{}) bool {
		client, _ := c.(redis.Connection)
		replyArgs := make([][]byte, 3)
		replyArgs[0] = messageBytes
		replyArgs[1] = []byte(channel)
		replyArgs[2] = message
		_, _ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
		return true
	})
	return protocol.MakeIntReply(int64(subscribers.Len()))
}

