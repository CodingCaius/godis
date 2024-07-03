package pubsub

import (
	"github.com/CodingCaius/godis/datastruct/dict"
	"github.com/CodingCaius/godis/datastruct/lock"
)

// 管理所有频道和订阅者的核心结构
// Hub stores all subscribe relations
type Hub struct {
	// channel -> list(*Client)
	subs dict.Dict
	// lock channel
	subsLocker *lock.Locks
}

// MakeHub creates new hub
func MakeHub() *Hub {
	return &Hub{
		subs:       dict.MakeConcurrent(4),
		subsLocker: lock.Make(16),
	}
}
