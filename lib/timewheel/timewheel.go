package timewheel

/*
实现了一个时间轮，用于在给定的时间间隔后执行任务。

时间轮是一种高效的定时任务调度算法，适用于需要在未来某个时间点执行任务的场景

时间轮通过槽位和圈数来管理任务的调度，具有高效的时间复杂度。时间轮的主要功能包括添加任务、移除任务和定时执行任务
*/

import (
	"container/list"
	"github.com/CodingCaius/godis/lib/logger"
	"time"
)

// 记录任务在时间轮中的位置
type location struct {
	slot  int // 任务所在的槽位
	etask *list.Element // 任务在槽位链表中的元素
}

// TimeWheel 等待给定时间后可以执行作业
type TimeWheel struct {
	interval time.Duration // 时间轮的时间间隔
	ticker   *time.Ticker // 用于定时触发的 time.Ticker
	slots    []*list.List // 时间轮的槽位，每个槽位是一个链表，存储任务

	timer             map[string]*location //记录任务的键和位置的映射
	currentPos        int // 当前时间轮的位置
	slotNum           int // 时间轮的槽位数量
	addTaskChannel    chan task  // 用于添加任务的通道
	removeTaskChannel chan string // 移除任务的通道
	stopChannel       chan bool // 停止时间轮的通道
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

// New 创建一个新的时间轮
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots()

	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start starts ticker for time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob add new job into pending queue
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

// RemoveJob add remove job from pending queue
// if job is done or not found, then nothing happened
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(&task)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos]
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	go tw.scanAndRunTask(l)
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; {
		task := e.Value.(*task)
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job := task.job
			job()
		}()
		next := e.Next()
		l.Remove(e)
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		e = next
	}
}

func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	e := tw.slots[pos].PushBack(task)
	loc := &location{
		slot:  pos,
		etask: e,
	}
	if task.key != "" {
		_, ok := tw.timer[task.key]
		if ok {
			tw.removeTask(task.key)
		}
	}
	tw.timer[task.key] = loc
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum

	return
}

func (tw *TimeWheel) removeTask(key string) {
	pos, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[pos.slot]
	l.Remove(pos.etask)
	delete(tw.timer, key)
}