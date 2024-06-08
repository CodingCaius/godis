/*
实现了 Redis 协议中的几种回复类型，并提供了将这些回复序列化为字节数组的功能。主要定义了五种类型的回复：批量字符串回复（BulkReply）、多批量字符串回复（MultiBulkReply）、多原始回复（MultiRawReply）、状态回复（StatusReply）和整数回复（IntReply）。此外，还定义了标准错误回复（StandardErrReply）
*/

package protocol

import (
	"bytes"
	"strconv"

	"github.com/CodingCaius/godis/interface/redis"
)

var (
	CRLF = "\r\n"
)

// 表示一个二进制安全字符串回复。
type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{Arg: arg}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

// 表示一个字符串列表回复。
type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args: args}
}

func (r *MultiBulkReply) ToBytes() []byte {
	var buf bytes.Buffer
	argLen := len(r.Args)
	bufLen := 1 + len(strconv.Itoa(argLen)) + 2
	for _, arg := range r.Args {
		if arg == nil {
			bufLen += 3 + 2
		} else {
			bufLen += 1 + len(strconv.Itoa(len(arg))) + 2 + len(arg) + 2
		}
	}
	buf.Grow(bufLen)
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(argLen))
	buf.WriteString(CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1")
			buf.WriteString(CRLF)
		} else {
			buf.WriteString("$")
			buf.WriteString(strconv.Itoa(len(arg)))
			buf.WriteString(CRLF)
			buf.Write(arg)
			buf.WriteString(CRLF)
		}
	}
	return buf.Bytes()
}

// 表示一个复杂列表结构
type MultiRawReply struct {
	Replies []redis.Reply
}

func MakeMultiRawReply(replies []redis.Reply) *MultiRawReply {
	return &MultiRawReply{Replies: replies}
}

// ToBytes marshal redis.Reply
func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}

/* ---- Status Reply ---- */

// StatusReply stores a simple status string
type StatusReply struct {
	Status string
}

// MakeStatusReply creates StatusReply
func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// ToBytes marshal redis.Reply
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

// IsOKReply returns true if the given protocol is +OK
func IsOKReply(reply redis.Reply) bool {
	return string(reply.ToBytes()) == "+OK\r\n"
}

/* ---- Int Reply ---- */

// IntReply stores an int64 number
type IntReply struct {
	Code int64
}

// MakeIntReply creates int protocol
func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ToBytes marshal redis.Reply
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ---- Error Reply ---- */

// ErrorReply is an error and redis.Reply
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrReply represents server error
type StandardErrReply struct {
	Status string
}

// MakeErrReply creates StandardErrReply
func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply returns true if the given protocol is error
func IsErrorReply(reply redis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

// ToBytes marshal redis.Reply
func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}
