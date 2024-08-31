package database

import (
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/CodingCaius/godis/interface/redis"
	"github.com/CodingCaius/godis/lib/logger"
	"github.com/CodingCaius/godis/lib/sync/atomic"
)

/*
这个文件实现了 Redis 主从复制的核心逻辑，包括从节点的管理、全量和部分同步、后台保存、命令处理以及定时任务等功能。通过这些功能，主节点可以将数据同步到多个从节点，从而实现数据的高可用性和负载均衡




*/

// 定义了从节点在主节点视角下的不同状态
const (
	//从节点正在与主节点进行握手
	slaveStateHandShake = uint8(iota)
	// 从节点正在等待主节点完成 RDB 文件的生成
	slaveStateWaitSaveEnd
	// 主节点正在向从节点发送 RDB 文件
	slaveStateSendingRDB
	// 从节点已经完成同步并处于在线状态
	slaveStateOnline
)

// 定义了主节点后台保存（BGSave）操作的不同状态
const (
	// 后台保存处于空闲状态，没有进行任何保存操作
	bgSaveIdle = uint8(iota)
	// 后台保存正在进行中。
	bgSaveRunning
	// 后台保存已经完成
	bgSaveFinish
)

// 定义了从节点支持的不同能力
const (
	// 从节点不支持任何特殊能力
	slaveCapacityNone = 0
	// 从节点支持 EOF（End of File）标记
	slaveCapacityEOF = 1 << iota
	// 从节点支持 PSYNC2 协议
	slaveCapacityPsync2
)

// slaveClient 存储了主节点视角下的从节点状态信息
type slaveClient struct {
	conn         redis.Connection // 从节点的连接对象
	state        uint8
	offset       int64     // 从节点的复制偏移量
	lastAckTime  time.Time // 从节点最后一次发送 ACK 的时间
	announceIp   string    // 从节点宣布的 IP 地址
	announcePort int       // 从节点宣布的端口号
	capacity     uint8     // 从节点的能力，使用上述定义的能力常量
}

// aofListener is currently only responsible for updating the backlog
// 存储主节点的复制积压日志
type replBacklog struct {
	buf           []byte // 积压日志的缓冲区，存储最近的写操作
	beginOffset   int64  // 积压日志的起始偏移量
	currentOffset int64  // 积压日志的当前偏移量
}

// 将新的字节数据追加到积压日志的缓冲区中，并更新当前的偏移量
func (backlog *replBacklog) appendBytes(bin []byte) {
	backlog.buf = append(backlog.buf, bin...)
	backlog.currentOffset += int64(len(bin))
}

// 获取积压日志的当前快照，包括缓冲区内容和当前偏移量
func (backlog *replBacklog) getSnapshot() ([]byte, int64) {
	return backlog.buf[:], backlog.currentOffset
}

// 获取从指定偏移量开始的积压日志快照
func (backlog *replBacklog) getSnapshotAfter(beginOffset int64) ([]byte, int64) {
	beg := beginOffset - backlog.beginOffset
	return backlog.buf[beg:], backlog.currentOffset
}

// 检查给定的偏移量是否在积压日志的有效范围内
func (backlog *replBacklog) isValidOffset(offset int64) bool {
	return offset >= backlog.beginOffset && offset < backlog.currentOffset
}

type masterStatus struct {
	mu           sync.RWMutex
	replId       string
	backlog      *replBacklog
	slaveMap     map[redis.Connection]*slaveClient
	waitSlaves   map[*slaveClient]struct{}
	onlineSlaves map[*slaveClient]struct{}
	bgSaveState  uint8
	rdbFilename  string
	aofListener  *replAofListener
	rewriting    atomic.Boolean
}

// bgSaveForReplication does bg-save and send rdb to waiting slaves
func (server *Server) bgSaveForReplication() {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("panic: %v", e)
			}
		}()
		if err := server.saveForReplication(); err != nil {
			logger.Errorf("save for replication error: %v", err)
		}
	}()

}

// saveForReplication does bg-save and send rdb to waiting slaves
func (server *Server) saveForReplication() error {
	rdbFile, err := ioutil.TempFile("", "*.rdb")
	if err != nil {
		return fmt.Errorf("create temp rdb failed: %v", err)
	}
	rdbFilename := rdbFile.Name()
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveRunning
	server.masterStatus.rdbFilename = rdbFilename // todo: can reuse config.Properties.RDBFilename?
	aofListener := &replAofListener{
		mdb:     server,
		backlog: server.masterStatus.backlog,
	}
	server.masterStatus.aofListener = aofListener
	server.masterStatus.mu.Unlock()

	err = server.persister.GenerateRDBForReplication(rdbFilename, aofListener, nil)
	if err != nil {
		return err
	}
	aofListener.readyToSend = true

	// change bgSaveState and get waitSlaves for sending
	waitSlaves := make(map[*slaveClient]struct{})
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveFinish
	for slave := range server.masterStatus.waitSlaves {
		waitSlaves[slave] = struct{}{}
	}
	server.masterStatus.waitSlaves = nil
	server.masterStatus.mu.Unlock()

	// send rdb to waiting slaves
	for slave := range waitSlaves {
		err = server.masterFullReSyncWithSlave(slave)
		if err != nil {
			server.removeSlave(slave)
			logger.Errorf("masterFullReSyncWithSlave error: %v", err)
			continue
		}
	}
	return nil
}
