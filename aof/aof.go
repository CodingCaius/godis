package aof

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodingCaius/godis/interface/database"
	"github.com/CodingCaius/godis/lib/logger"
	"github.com/CodingCaius/godis/redis/parser"
	"github.com/CodingCaius/godis/redis/protocol"
)

type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 20
)

const (
	// FsyncAlways do fsync for every command
	FsyncAlways = "always"
	// FsyncEverySec do fsync every second
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

// 监听器收到 aof 负载后将被回调
// 通过侦听器，我们可以将更新转发到从属节点等。
type Listener interface {
	// 收到 aof Payload 后会回调
	Callback([]CmdLine)
}

// Persister 从通道接收消息并写入 AOF 文件
type Persister struct {
	ctx        context.Context
	cancel     context.CancelFunc
	db         database.DBEngine
	tmpDBMaker func() database.DBEngine
	// aofChan 是接收 aof 负载的通道（listenCmd 会将负载发送到该通道）
	aofChan chan *payload
	// aofFile是aof文件的文件处理程序
	aofFile *os.File
	// aofFilename是aof文件的路径
	aofFilename string
	// aofFsync是fsync的策略
	aofFsync string
	// 当aof任务完成并准备关闭时，aof goroutine将通过该通道向主goroutine发送消息
	aofFinished chan struct{}
	// 暂停 aof 以开始/完成 aof 重写进度
	pausingAof sync.Mutex
	// 当前活动数据库
	currentDB int
	// 存储当前所有活跃的监听器
	listeners map[Listener]struct{}
	// 重用cmdLine缓冲区
	buffer []CmdLine
}

// NewPersister 创建一个新的 aof.Persister
func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{}
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	// load aof file if needed
	if load {
		persister.LoadAof(0)
	}
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	// 启动aof goroutine在后台写入aof文件，并在需要时定期进行fsync（请参阅fsyncEverySecond）
	// 启动一个新的 goroutine 以后台监听并处理 AOF 命令。
	go func() {
		persister.listenCmd()
	}()
	// 创建一个带有取消功能的上下文，并将其保存到 persister
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	// 如果 aofFsync 策略是 FsyncEverySec，则启动定期（每秒）同步文件的功能。
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

// RemoveListener 从 aof.Persister 实例中移除一个监听器
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// 将命令发送到 AOF处理 goroutine，通过通道传递命令
// SaveCmdLine send command to aof goroutine through channel
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// aofChan 在加载 aof 时将被暂时设置为 nil
	if persister.aofChan == nil {
		return
	}

	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}
	// 如果 aofFsync 策略不是 FsyncAlways，则将命令行和数据库索引封装到一个 payload 实例中。
	// 通过 aofChan 通道将 payload 发送到后台处理 goroutine
	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}

}

// listenCmd 监听 AOF通道并将收到的命令写入文件
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	// 当 aofChan 通道被关闭并且所有数据都被处理完后，循环结束。
	// 向 persister.aofFinished 通道发送一个空的结构体，以通知其他 goroutine AOF 写入操作已完成
	persister.aofFinished <- struct{}{}
}

// aof 的后台处理goroutine
func (persister *Persister) writeAof(p *payload) {
	// 重用其底层数组，减少内存分配
	persister.buffer = persister.buffer[:0]
	// 加锁以防止其他 goroutine 暂停 AOF 操作
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	// ensure aof is in the right database
	if p.dbIndex != persister.currentDB {
		// select db
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}
	// save command
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	// 将数据写入到操作系统的文件缓冲区，并没有真正写入磁盘
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	if persister.aofFsync == FsyncAlways {
		// 将操作系统文件缓冲区中的数据真正写入磁盘
		_ = persister.aofFile.Sync()
	}
}

// LoadAof 用于读取 AOF（Append Only File）文件，并在启动 Persister.listenCmd 之前加载其中的命令
func (persister *Persister) LoadAof(maxBytes int) {
	// persister.db.Exec 可能会调用 persister.AddAof
	//删除 aofChan 以防止加载的命令返回到 aofChan
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	// load rdb preamble if needed
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// no rdb preamble
		file.Seek(0, io.SeekStart)
	} else {
		// has rdb preamble
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		maxBytes = maxBytes - decoder.GetReadCount()
	}
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	fakeConn := connection.NewFakeConn() // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		ret := persister.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// Fsync 将 aof 文件刷新到磁盘
func (persister *Persister) Fsync() {
	persister.pausingAof.Lock()
	if err := persister.aofFile.Sync(); err != nil {
		logger.Errorf("fsync failed: %v", err)
	}
	persister.pausingAof.Unlock()
}

// Close 优雅地停止 aof 持久化过程
func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished // wait for aof finished
		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	persister.cancel()
}

// fsyncEverySecond fsync aof file every second
func (persister *Persister) fsyncEverySecond() {
	// 创建了一个新的 Ticker，每秒钟触发一次
	// 它会按照指定的时间间隔向其 C 通道发送时间戳
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.Fsync()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}
