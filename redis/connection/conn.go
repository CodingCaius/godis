package connection

import (
	"net"
	"sync"
	"time"

	"github.com/CodingCaius/godis/lib/logger"
	"github.com/CodingCaius/godis/lib/sync/wait"
)

const (
	// flagSlave 这是一个与从节点（slave）连接的标志
	flagSlave = uint64(1 << iota) // 1
	// 表示这是一个与主节点（master）连接的标志  2
	flagMaster
	// 表示这个连接处于一个事务（transaction）中 4
	flagMulti
)

// 表示 Redis 客户端连接
type Connection struct {
	// 表示底层的网络连接，使用 Go 的 net.Conn 接口
	conn net.Conn

	// 用于等待数据发送完成，通常用于优雅关闭连接时确保所有数据都已发送
	sendingData wait.Wait

	// 服务器发送响应时锁定
	mu sync.Mutex
	// 用于存储连接的状态标志，例如是否是主节点连接、从节点连接或事务状态等
	flags uint64

	// 存储订阅的频道，键是频道名称，值是布尔值，表示是否订阅。
	subs map[string]bool

	// 密码可能会在运行时通过 CONFIG 命令更改，因此请存储密码
	password string

	//存储在事务（multi）模式下排队的命令，每个命令是一个字节数组的数组
	queue [][][]byte
	// 存储被 WATCH 命令监视的键及其版本号
	watching map[string]uint32
	// 存储事务中的错误
	txErrors []error

	// 存储当前选择的数据库索引
	selectedDB int
}

// 用于存储和重用 Connection 对象
var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// RemoteAddr 返回远程网络地址
func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

// Close 断开与客户端的连接
func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	connPool.Put(c)
	return nil
}

// NewConn 用于创建一个新的 Connection 实例
func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	return c
}

// Write 通过 TCP 连接向客户端发送响应
func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()

	return c.conn.Write(b)
}

// 返回连接的远程地址（通常是客户端的 IP 地址和端口）
func (c *Connection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

// 将当前连接添加到指定频道的订阅者列表中
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// 将当前连接从指定频道的订阅者列表中移除
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// SubsCount 返回当前连接订阅的频道数量
func (c *Connection) SubsCount() int {
	return len(c.subs)
}

// GetChannels 返回当前连接订阅的所有频道
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

// SetPassword 存储连接的密码
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// GetPassword 返回存储的密码
func (c *Connection) GetPassword() string {
	return c.password
}

// InMultiState 检查当前连接是否处于未提交的事务状态
func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0
}

// SetMultiState 设置事务状态标志
func (c *Connection) SetMultiState(state bool) {
	if !state { // 如果 state 为 false，表示取消事务
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti // clean multi flag
		return
	}
	c.flags |= flagMulti
}

// GetQueuedCmdLine 返回当前事务中排队的命令
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmd  将命令添加到当前事务的队列中
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// AddTxError 在事务中存储语法错误
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

// GetTxErrors 返回事务中的所有语法错误
func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

// ClearQueuedCmds 清空当前事务中的排队命令
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

// GetWatching 返回当前连接正在监视的键及其版本号
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

// GetDBIndex 返回当前选择的数据库索引
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB 选择一个数据库
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

// 将当前连接标记为从节点
func (c *Connection) SetSlave() {
	c.flags |= flagSlave
}

// 检查当前连接是否为从节点
func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0
}

// 将当前连接标记为主节点
func (c *Connection) SetMaster() {
	c.flags |= flagMaster
}

// 检查当前连接是否为主节点
func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0
}