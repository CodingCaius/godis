package timewheel

import "time"

var tw = New(time.Second, 3600)

func init() {
	tw.Start()
}

// Delay 在指定的延迟时间后执行任务
func Delay(duration time.Duration, key string, job func()) {
	tw.AddJob(duration, key, job)
}

// At 在指定的时间点执行任务
func At(at time.Time, key string, job func()) {
	tw.AddJob(at.Sub(time.Now()), key, job)
}

// Cancel 取消一个已经添加到时间轮中的任务
func Cancel(key string) {
	tw.RemoveJob(key)
}