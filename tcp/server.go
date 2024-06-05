package tcp

import (
	"os"
	"time"
)

// tcp 服务器

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect`
	Timeout    time.Duration `yaml:"timeout`
}

// 客户端连接计数
var ClientCounter int32

// ListenAndServerWithSignal 监听端口并处理请求，阻塞直到收到停止信号
func ListenAndServerWithSignal(cfg *Config, handler tcp.Handler) error {
	// 用于通知服务器关闭的通道
	closeChan := make(chan struct{})
	// 用于接收操作系统信号的通道
	sigCh := make(chan os.Signal)


}
