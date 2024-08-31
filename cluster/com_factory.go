package cluster

// 管理 Redis 集群中节点之间的连接池

import (
	"errors"
	"fmt"

	"net"

	"github.com/CodingCaius/godis/config"
	"github.com/CodingCaius/godis/datastruct/dict"
	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/logger"
	"github.com/CodingCaius/godis/lib/utils"
	"github.com/CodingCaius/godis/lib/pool"
	"github.com/CodingCaius/godis/redis/parser"
	"github.com/CodingCaius/godis/redis/protocol"
)

type defaultClientFactory struct {
	// 存储每个节点的连接池
	nodeConnections dict.Dict // map[string]*pool.Pool
}

// 最大空闲连接数为 1，最大活跃连接数为 16
var connectionPoolConfig = pool.Config{
	MaxIdle:   1,
	MaxActive: 16,
}

// GetPeerClient 从连接池中获取一个与指定节点的连接
func (factory *defaultClientFactory) GetPeerClient(peerAddr string) (peerClient, error) {
	var connectionPool *pool.Pool
	raw, ok := factory.nodeConnections.Get(peerAddr)
	if !ok {
		// 创建新的连接池
		creator := func() (interface{}, error) {
			c, err := client.MakeClient(peerAddr)
			if err != nil {
				return nil, err
			}
			c.Start()
			// all peers of cluster should use the same password
			if config.Properties.RequirePass != "" {
				authResp := c.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
				if !protocol.IsOKReply(authResp) {
					return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
				}
			}
			return c, nil
		}
		finalizer := func(x interface{}) {
			logger.Debug("destroy client")
			cli, ok := x.(client.Client)
			if !ok {
				return
			}
			cli.Close()
		}
		connectionPool = pool.New(creator, finalizer, connectionPoolConfig)
		factory.nodeConnections.Put(peerAddr, connectionPool)
	} else {
		connectionPool = raw.(*pool.Pool)
	}
	raw, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection pool make wrong type")
	}
	return conn, nil
}

// ReturnPeerClient 将连接返回到连接池中
func (factory *defaultClientFactory) ReturnPeerClient(peer string, peerClient peerClient) error {
	raw, ok := factory.nodeConnections.Get(peer)
	if !ok {
		return errors.New("connection pool not found")
	}
	raw.(*pool.Pool).Put(peerClient)
	return nil
}

// 表示一个 TCP 流，包含一个网络连接和一个解析器通道
type tcpStream struct {
	conn net.Conn
	ch   <-chan *parser.Payload
}

func (s *tcpStream) Stream() <-chan *parser.Payload {
	return s.ch
}

func (s *tcpStream) Close() error {
	return s.conn.Close()
}

// 创建一个新的 TCP 流，用于与指定节点进行通信
func (factory *defaultClientFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	// todo: reuse connection
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect with %s failed: %v", peerAddr, err)
	}
	ch := parser.ParseStream(conn)
	send2node := func(cmdLine CmdLine) redis.Reply {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		resp := <-ch
		if resp.Err != nil {
			return protocol.MakeErrReply(resp.Err.Error())
		}
		return resp.Data
	}
	if config.Properties.RequirePass != "" {
		authResp := send2node(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
		if !protocol.IsOKReply(authResp) {
			return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
		}
	}
	req := protocol.MakeMultiBulkReply(cmdLine)
	_, err = conn.Write(req.ToBytes())
	if err != nil {
		return nil, protocol.MakeErrReply("send cmdLine failed: " + err.Error())
	}
	return &tcpStream{
		conn: conn,
		ch:   ch,
	}, nil
}

func newDefaultClientFactory() *defaultClientFactory {
	return &defaultClientFactory{
		nodeConnections: dict.MakeConcurrent(1),
	}
}

func (factory *defaultClientFactory) Close() error {
	factory.nodeConnections.ForEach(func(key string, val interface{}) bool {
		val.(*pool.Pool).Close()
		return true
	})
	return nil
}
