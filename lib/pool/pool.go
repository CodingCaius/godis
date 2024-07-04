package pool

/*
实现了一个通用的对象池（Pool），可以用于管理和重用资源，
比如数据库连接、网络连接等。
通过对象池，可以减少频繁创建和销毁资源的开销，提高系统性能和资源利用率
*/

import (
	"errors"
	"sync"
)

var (
	// 表示池已经关闭
	ErrClosed = errors.New("pool closed")
	// 表示已经达到最大连接限制
	ErrMax    = errors.New("reach max connection limit")
)

type request chan interface{}

type Config struct {
	MaxIdle   uint
	MaxActive uint
}

// Pool stores object for reusing, such as redis connection
type Pool struct {
	Config
	// 创建新对象的工厂方法
	factory     func() (interface{}, error)
	// 销毁对象的方法
	finalizer   func(x interface{})
	// 存储空闲对象的通道
	idles       chan interface{}
	// 等待获取对象的请求队列
	waitingReqs []request
	// 当前活跃的对象数量
	activeCount uint // increases during creating connection, decrease during destroying connection
	mu          sync.Mutex
	// 标记池是否已经关
	closed      bool
}

// z创建并返回一个新的对象池实例
func New(factory func() (interface{}, error), finalizer func(x interface{}), cfg Config) *Pool {
	return &Pool{
		factory:     factory,
		finalizer:   finalizer,
		idles:       make(chan interface{}, cfg.MaxIdle),
		waitingReqs: make([]request, 0),
		Config:      cfg,
	}
}

// getOnNoIdle try to create a new connection or waiting for connection being returned
// invoker should have pool.mu
// 当没有空闲对象时，尝试创建一个新对象或等待其他请求返回对象。
// 如果活跃对象数量已经达到最大限制，则将请求放入等待队列。
// 如果可以创建新对象，则增加活跃对象计数并调用工厂方法创建对象
func (pool *Pool) getOnNoIdle() (interface{}, error) {
	if pool.activeCount >= pool.MaxActive {
		// waiting for connection being returned
		req := make(chan interface{}, 1)
		pool.waitingReqs = append(pool.waitingReqs, req)
		pool.mu.Unlock()
		x, ok := <-req
		if !ok {
			return nil, ErrMax
		}
		return x, nil
	}

	// create a new connection
	pool.activeCount++ // hold a place for new connection
	pool.mu.Unlock()
	x, err := pool.factory()
	if err != nil {
		// create failed return token
		pool.mu.Lock()
		pool.activeCount-- // release the holding place
		pool.mu.Unlock()
		return nil, err
	}
	return x, nil
}

// 获取一个对象
func (pool *Pool) Get() (interface{}, error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrClosed
	}

	select {
	case item := <-pool.idles: // 如果有空闲对象，直接返回
		pool.mu.Unlock()
		return item, nil
	default:
		// no pooled item, create one
		return pool.getOnNoIdle()
	}
}

// 将对象放回池中
func (pool *Pool) Put(x interface{}) {
	pool.mu.Lock()

	// 如果池已经关闭，直接销毁对象
	if pool.closed {
		pool.mu.Unlock()
		pool.finalizer(x)
		return
	}
	// 如果有等待请求，将对象分配给第一个等待请求
	if len(pool.waitingReqs) > 0 {
		req := pool.waitingReqs[0]
		copy(pool.waitingReqs, pool.waitingReqs[1:])
		pool.waitingReqs = pool.waitingReqs[:len(pool.waitingReqs)-1]
		req <- x
		pool.mu.Unlock()
		return
	}

	select {
	case pool.idles <- x: // 如果没有等待请求且空闲对象未达到最大限制，将对象放入空闲通道
		pool.mu.Unlock()
		return
	default: // 如果空闲对象已达到最大限制，销毁对象并减少活跃对象计数
		// reach max idle, destroy redundant item
		pool.mu.Unlock()
		pool.activeCount--
		pool.finalizer(x)
	}
}

// 关闭对象池
// 标记池为关闭状态，关闭空闲通道，并销毁所有空闲对象
func (pool *Pool) Close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	close(pool.idles)
	pool.mu.Unlock()

	for x := range pool.idles {
		pool.finalizer(x)
	}
}