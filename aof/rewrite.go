package aof

import (
	"io"
	"os"
	"strconv"

	"github.com/CodingCaius/godis/config"
	"github.com/CodingCaius/godis/lib/logger"
	"github.com/CodingCaius/godis/lib/utils"
	"github.com/CodingCaius/godis/redis/protocol"
)

func (persister *Persister) newRewritehandler() *Persister {
	h := &Persister{}
	h.aofFilename = persister.aofFilename
	h.db = persister.tmpDBMaker()
	return h
}

// RewriteCtx 保存 AOF 重写过程的上下文
type RewriteCtx struct {
	tmpFile  *os.File // tmpFile是aof tmpFile的文件处理程序
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// Rewrite 进行AOF重写
func (persister *Persister) Rewrite() error {
	ctx, err := persister.StartRewrite()
	if err != nil {
		return err
	}
	err = persister.DoRewrite(ctx)
	if err != nil {
		return err
	}

	persister.FinishRewrite(ctx)
	return nil
}

// DoRewrite实际上是重写aof文件
// 根据配置选择生成AOF前缀或RDB前缀，并执行相应的重写操作
func (persister *Persister) DoRewrite(ctx *RewriteCtx) (err error) {
	// 通过检查配置中的AofUseRdbPreamble值，代码能够灵活地选择不同的重写策略
	// start rewrite
	if !config.Properties.AofUseRdbPreamble {
		logger.Info("generate aof preamble")
		err = persister.generateAof(ctx)
	} else {
		logger.Info("generate rdb preamble")
		err = persister.generateRDB(ctx)
	}
	return err
}

// StartRewrite 暂停AOF写入，同步当前AOF文件内容到磁盘，获取文件大小，并创建一个临时文件
func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// pausing aof
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// 将AOF文件的内容同步到磁盘
	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	fileInfo, _ := os.Stat(persister.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		dbIdx:    persister.currentDB,
	}, nil
}

// FinishRewrite 将重写期间执行的命令复制到临时文件，关闭当前AOF文件，使用临时文件替换当前AOF文件，重新打开AOF文件以便继续写入，并写入一个 SELECT 命令以确保数据库索引一致
func (persister *Persister) FinishRewrite(ctx *RewriteCtx) {
	persister.pausingAof.Lock() // pausing aof
	defer persister.pausingAof.Unlock()
	tmpFile := ctx.tmpFile

	// 将重写期间执行的命令复制到 tmpFile
	errOccurs := func() bool {
		/* 重写期间执行的读写命令 */
		src, err := os.Open(persister.aofFilename)
		if err != nil {
			logger.Error("open aofFilename failed: " + err.Error())
			return true
		}
		defer func() {
			_ = src.Close()
			_ = tmpFile.Close()
		}()

		// 将文件指针移动到重写开始时记录的文件大小位置
		_, err = src.Seek(ctx.fileSize, 0)
		if err != nil {
			logger.Error("seek failed: " + err.Error())
			return true
		}
		// sync tmpFile's db index with online aofFile
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
		_, err = tmpFile.Write(data)
		if err != nil {
			logger.Error("tmp file rewrite failed: " + err.Error())
			return true
		}
		// copy data
		_, err = io.Copy(tmpFile, src)
		if err != nil {
			logger.Error("copy aof filed failed: " + err.Error())
			return true
		}
		return false
	}()
	if errOccurs {
		return
	}

	// replace current aof file by tmp file
	_ = persister.aofFile.Close()
	if err := os.Rename(tmpFile.Name(), persister.aofFilename); err != nil {
		logger.Warn(err)
	}
	// reopen aof file for further write
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	persister.aofFile = aofFile

	// write select command again to resume aof file selected db
	// it should have the same db index with  persister.currentDB
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
