package database

import (
	"container/list"
	"time"

	"github.com/CodingCaius/godis/datastruct/dict"
	"github.com/CodingCaius/godis/datastruct/set"
	"github.com/CodingCaius/godis/datastruct/sortedset"
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/utils"
	"github.com/CodingCaius/godis/redis/protocol"
)

// execDel removes a key from db
func execDel(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("del", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

// 撤销删除操作
func undoDel(db *DB, args [][]byte) []CmdLine {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return rollbackGivenKeys(db, keys...)
}

// execExists 检查给定的键是否存在于数据库中
func execExists(db *DB, args [][]byte) redis.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return protocol.MakeIntReply(result)
}

// execFlushDB 清空当前数据库中的所有数据
// deprecated, use Server.flushDB
func execFlushDB(db *DB, args [][]byte) redis.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine3("flushdb", args...))
	return &protocol.OkReply{}
}

// execType returns the type of entity, including: string, list, hash, set and zset
func execType(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return protocol.MakeStatusReply("string")
	case list.List:
		return protocol.MakeStatusReply("list")
	case dict.Dict:
		return protocol.MakeStatusReply("hash")
	case *set.Set:
		return protocol.MakeStatusReply("set")
	case *sortedset.SortedSet:
		return protocol.MakeStatusReply("zset")
	}
	return &protocol.UnknownErrReply{}
}

func prepareRename(args [][]byte) ([]string, []string) {
	src := string(args[0])
	dest := string(args[1])
	return []string{dest}, []string{src}
}

// execRename 重命名一个键
func execRename(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply("no such key")
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.PutEntity(dest, entity)
	db.Remove(src)
	if hasTTL {
		db.Persist(src) // clean src and dest with their ttl
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	db.addAof(utils.ToCmdLine3("rename", args...))
	return &protocol.OkReply{}
}

// 撤销重命名操作
func undoRename(db *DB, args [][]byte) []CmdLine {
	src := string(args[0])
	dest := string(args[1])
	return rollbackGivenKeys(db, src, dest)
}

// execRenameNx 重命名一个键，但仅当新键不存在时才执行
func execRenameNx(db *DB, args [][]byte) redis.Reply {
	src := string(args[0])
	dest := string(args[1])

	_, ok := db.GetEntity(dest)
	if ok {
		return protocol.MakeIntReply(0)
	}

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply("no such key")
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.Removes(src, dest) // clean src and dest with their ttl
	db.PutEntity(dest, entity)
	if hasTTL {
		db.Persist(src) // clean src and dest with their ttl
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	db.addAof(utils.ToCmdLine3("renamenx", args...))
	return protocol.MakeIntReply(1)
}

// execExpire 设置一个键的过期时间（以秒为单位）
func execExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(ttlArg) * time.Second

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execExpireAt sets a key's expiration in unix timestamp
func execExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(raw, 0)

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execExpireTime returns the absolute Unix expiration timestamp in seconds at which the given key will expire.
func execExpireTime(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	rawExpireTime, _ := raw.(time.Time)
	expireTime := rawExpireTime.Unix()
	return protocol.MakeIntReply(expireTime)
}