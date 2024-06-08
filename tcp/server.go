package tcp

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/CodingCaius/godis/interface/tcp"
	"github.com/CodingCaius/godis/lib/logger"
)

// tcp 服务器

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// 客户端连接计数
var ClientCounter int32

// ListenAndServerWithSignal 监听端口并处理请求，阻塞直到收到停止信号
func ListenAndServerWithSignal(cfg *Config, handler tcp.Handler) error {
	// 用于通知服务器关闭的通道
	closeChan := make(chan struct{})
	// 用于接收操作系统信号的通道
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServer(listener, handler, closeChan)
	return nil
}

// ListenAndServer 监听端口并处理请求，阻塞直到关闭
func ListenAndServer(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	/*
		实现了一个服务器的主循环，
		它在指定的监听器上接受连接，并使用处理器处理连接，
		同时支持优雅地关闭服务器。
	*/
	errCh := make(chan error, 1)
	defer close(errCh)
	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal")
		case er := <-errCh:
			logger.Info(fmt.Sprintf("accept error: %s", er.Error()))
		}
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately
		_ = handler.Close()  // close connections
	}()

	ctx := context.Background()
	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Infof("accept occurs temporary error: %v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			errCh <- err
			break
		}
		// 处理连接
		logger.Info("accept link")
		ClientCounter++
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				atomic.AddInt32(&ClientCounter, -1)
			}()
			handler.Handle(ctx, conn)
		}()
	}
	wg.Wait()
}
