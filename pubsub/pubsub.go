package pubsub

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
	if subscribers.Contains(func(a interface{}) bool {
		return a == client
	}) {
		return false
	}
	subscribers.Add(client)
	return true
}

